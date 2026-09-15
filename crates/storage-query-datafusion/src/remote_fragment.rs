// Copyright (c) 2023 - 2026 Restate Software Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Serialization and execution of physical-plan fragments at partition owners.
//!
//! The physical optimizer rules in `scan_fragment` and `partial_aggregation`
//! create a [`RemoteFragment`] on the query coordinator. A fragment is an
//! immutable, single-input DataFusion plan template whose input is represented
//! by [`FragmentLeafExec`]. Construction validates that shape and pre-encodes
//! the plan. `RemoteNodeExec` attaches the fragment to a remote scan; execution
//! sends its versioned wire form in `RemoteQueryScannerOpen`. The worker decodes
//! it in `ScannerTask`, replaces the placeholder with the raw partition stream,
//! and returns the fragment's output. If decoding or synchronous setup fails,
//! the worker declines it and the coordinator binds the same template over the
//! returned raw stream instead.
//!
//! *Remote-safe* means that evaluating an expression on another node preserves
//! its semantics and that the physical-plan codec preserves its configuration.
//! This module rejects volatile and coordinator-stateful dynamic expressions,
//! unresolved columns, and casts whose non-default options the codec loses.
//! It also exposes helpers for the currently supported row-wise operators
//! (`FilterExec` and `ProjectionExec`). Each optimizer remains responsible for
//! stronger operator-specific rules, such as partial aggregation's mergeability
//! requirements.
//!
//! The fragment value and wire bytes are stateless. The small `stream_input`
//! submodule isolates the mutable, one-shot adapter needed only while binding a
//! concrete record-batch stream to DataFusion's execution-plan interface.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::format::DEFAULT_CAST_OPTIONS;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::{CastExpr, UnKnownColumn};
use datafusion::physical_expr_common::physical_expr::{is_dynamic_physical_expr, is_volatile};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use restate_types::net::remote_query_scanner::RemoteQueryScannerFragment;

use crate::{decode_schema, encode_schema};

/// Version of the serialized physical-fragment contract.
///
/// Increment this for every DataFusion upgrade or fragment-codec change.
pub(crate) const REMOTE_FRAGMENT_FORMAT_VERSION: u32 = 1;
pub(crate) const REMOTE_FRAGMENT_ACCEPTED_METRIC: &str = "remote_fragment_accepted";
pub(crate) const REMOTE_FRAGMENT_DECLINED_METRIC: &str = "remote_fragment_declined";

/// Returns whether the physical-expression codec preserves an expression's
/// semantics when it is evaluated on another node.
pub(crate) fn is_remote_safe_expression(expression: &Arc<dyn PhysicalExpr>) -> bool {
    if is_volatile(expression) || is_dynamic_physical_expr(expression) {
        return false;
    }

    !expression
        .exists(|node| {
            Ok(node.is::<UnKnownColumn>()
                || node
                    .downcast_ref::<CastExpr>()
                    .is_some_and(|cast| cast.cast_options() != &DEFAULT_CAST_OPTIONS))
        })
        .expect("remote expression safety visitor is infallible")
}

/// Identifies the unary operators whose row-wise semantics commute with
/// partition-local execution.
pub(crate) fn is_row_wise_operator(operator: &Arc<dyn ExecutionPlan>) -> bool {
    operator.is::<FilterExec>() || operator.is::<ProjectionExec>()
}

/// Returns whether a supported row-wise operator can execute on another node.
pub(crate) fn is_remote_safe_operator(operator: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(filter) = operator.downcast_ref::<FilterExec>() {
        filter.fetch().is_none() && is_remote_safe_expression(filter.predicate())
    } else if let Some(projection) = operator.downcast_ref::<ProjectionExec>() {
        projection
            .expr()
            .iter()
            .all(|expression| is_remote_safe_expression(&expression.expr))
    } else {
        false
    }
}

/// Rebuilds a validated unary chain over `input` without executing it.
pub(crate) fn bind_unary_operators(
    operators: &[Arc<dyn ExecutionPlan>],
    mut input: Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    for operator in operators.iter().rev() {
        input = Arc::clone(operator).with_new_children(vec![input]).ok()?;
    }
    Some(input)
}

/// An immutable, validated single-input plan template and its cached wire form.
///
/// Structural validation guarantees one placeholder input, a unary path to the
/// root, and one output partition. Each optimizer that constructs a fragment
/// remains responsible for its operator-specific semantic allowlist; physical
/// expressions can use [`is_remote_safe_expression`] for common cross-node
/// checks.
#[derive(Clone)]
pub(crate) struct RemoteFragment {
    template: Arc<dyn ExecutionPlan>,
    input_schema: SchemaRef,
    wire: RemoteQueryScannerFragment,
}

impl Debug for RemoteFragment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteFragment")
            .field("root", &self.template.name())
            .field("input_schema", &self.input_schema)
            .field("output_schema", &self.output_schema())
            .finish()
    }
}

impl RemoteFragment {
    /// Builds and pre-encodes a fragment. The template must contain exactly one
    /// [`FragmentLeafExec`], no other leaves, and one output partition. This
    /// checks structure and codec support, not feature-specific semantics.
    pub(crate) fn try_new(template: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let input_schema = validate_template(&template)?;
        let wire = encode_fragment(&template)?;
        Ok(Self {
            template,
            input_schema,
            wire,
        })
    }

    /// Decodes a fragment received from a peer. A version mismatch is a clean
    /// decline so that the caller can execute the same fragment over raw rows.
    pub(crate) fn from_wire(
        wire: &RemoteQueryScannerFragment,
        context: &TaskContext,
        expected_input_schema: &SchemaRef,
    ) -> Result<Option<Self>> {
        if wire.format_version != REMOTE_FRAGMENT_FORMAT_VERSION {
            return Ok(None);
        }

        let template = datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(
            &wire.serialized_plan,
            context,
            &FragmentCodec,
        )?;
        let input_schema = validate_template(&template)?;

        if input_schema != *expected_input_schema {
            return Ok(None);
        }
        let declared_output_schema = Arc::new(
            decode_schema(&wire.output_schema_bytes)
                .map_err(|error| DataFusionError::External(error.into()))?,
        );
        if template.schema() != declared_output_schema {
            return Ok(None);
        }

        Ok(Some(Self {
            template,
            input_schema,
            wire: wire.clone(),
        }))
    }

    pub(crate) fn output_schema(&self) -> SchemaRef {
        self.template.schema()
    }

    /// Writes the fragment's operators as one inline pipeline, omitting the
    /// placeholder input that is replaced by the remote partition scan.
    ///
    /// `RemoteNodeExec` uses this only for plan display. Keeping the fragment
    /// out of `ExecutionPlan::children` preserves the remote optimizer boundary.
    pub(crate) fn fmt_pipeline(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut plan = Arc::clone(&self.template);
        let mut separator = "";
        while !plan.is::<FragmentLeafExec>() {
            write!(f, "{separator}")?;
            plan.fmt_as(DisplayFormatType::Default, f)?;
            separator = " -> ";

            let children = plan.children();
            let [child] = children.as_slice() else {
                return Err(std::fmt::Error);
            };
            plan = Arc::clone(child);
        }
        Ok(())
    }

    pub(crate) fn to_wire(&self) -> RemoteQueryScannerFragment {
        self.wire.clone()
    }

    /// Executes this immutable template over one input stream.
    /// Mutable one-shot stream ownership is isolated in [`stream_input`].
    pub(crate) fn execute(
        &self,
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.execute_recoverable(input, context)
            .map_err(|failure| failure.error)
    }

    /// Constructs the fragment stream without consuming the raw input. A
    /// setup failure returns that input so a remote worker can decline the
    /// fragment and let the coordinator execute its fallback.
    pub(crate) fn execute_recoverable(
        &self,
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, FragmentSetupFailure> {
        stream_input::execute_recoverable(self, input, context)
    }

    /// Binds the fragment template to a physical input plan. Optimizers use
    /// this to materialize the same fragment locally and to derive the
    /// properties advertised by a remote boundary.
    pub(crate) fn bind_input(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if input.schema() != self.input_schema {
            return internal_err!(
                "remote fragment input schema mismatch: expected {:?}, got {:?}",
                self.input_schema,
                input.schema()
            );
        }
        let bound = bind_fragment_input(&self.template, input)?;
        if bound.schema() != self.output_schema() {
            return internal_err!(
                "binding changed remote fragment output schema: expected {:?}, got {:?}",
                self.output_schema(),
                bound.schema()
            );
        }
        Ok(bound)
    }
}

/// A synchronous fragment setup error paired with the untouched raw stream.
/// Once the returned fragment stream is polled, failures are ordinary query
/// errors because its input may already have been consumed.
pub(crate) struct FragmentSetupFailure {
    pub(crate) error: DataFusionError,
    pub(crate) input: SendableRecordBatchStream,
}

/// An immutable fragment paired with the execution state of one concrete scan.
///
/// The distributed scanner API does not otherwise carry DataFusion's
/// `TaskContext`, so this value keeps the context and fragment together until
/// the remote peer accepts it or the coordinator executes its fallback.
#[derive(Clone)]
pub(crate) struct RemoteFragmentExecution {
    fragment: Arc<RemoteFragment>,
    context: Arc<TaskContext>,
    metrics: Option<RemoteFragmentMetrics>,
}

impl RemoteFragmentExecution {
    /// Captures the query context now so the fragment can cross the scanner
    /// abstraction and still execute locally if the remote peer declines it.
    pub(crate) fn new(fragment: Arc<RemoteFragment>, context: Arc<TaskContext>) -> Self {
        Self {
            fragment,
            context,
            metrics: None,
        }
    }

    /// Adds execution-plan counters that make remote acceptance and coordinator
    /// fallback visible in `EXPLAIN ANALYZE`.
    pub(crate) fn with_metrics(
        mut self,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        self.metrics = Some(RemoteFragmentMetrics {
            accepted: MetricBuilder::new(metrics)
                .counter(REMOTE_FRAGMENT_ACCEPTED_METRIC, partition),
            declined: MetricBuilder::new(metrics)
                .counter(REMOTE_FRAGMENT_DECLINED_METRIC, partition),
        });
        self
    }

    pub(crate) fn record_remote_acceptance(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.accepted.add(1);
        }
    }

    pub(crate) fn record_remote_decline(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.declined.add(1);
        }
    }

    pub(crate) fn output_schema(&self) -> SchemaRef {
        self.fragment.output_schema()
    }

    pub(crate) fn to_wire(&self) -> RemoteQueryScannerFragment {
        self.fragment.to_wire()
    }

    pub(crate) fn execute(
        self,
        input: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        self.fragment.execute(input, self.context)
    }
}

/// Per-query execution-plan counters updated after fragment negotiation.
#[derive(Clone)]
struct RemoteFragmentMetrics {
    accepted: Count,
    declined: Count,
}

/// Placeholder for the partition scan at the bottom of a remote fragment.
#[derive(Debug)]
pub(crate) struct FragmentLeafExec {
    properties: Arc<PlanProperties>,
}

impl FragmentLeafExec {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        let properties = Arc::new(leaf_properties(schema));
        Self { properties }
    }
}

impl DisplayAs for FragmentLeafExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FragmentLeafExec")
    }
}

impl ExecutionPlan for FragmentLeafExec {
    fn name(&self) -> &str {
        "FragmentLeafExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("FragmentLeafExec does not support children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        internal_err!("FragmentLeafExec must be bound before execution")
    }
}

fn leaf_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// Validates the fragment boundary independently of any particular producer:
/// exactly one designated input, no hidden leaf plans, and one output lane.
fn validate_template(template: &Arc<dyn ExecutionPlan>) -> Result<SchemaRef> {
    let mut input_schema = None;
    template.apply(|plan| {
        if let Some(leaf) = plan.downcast_ref::<FragmentLeafExec>() {
            if input_schema.replace(leaf.schema()).is_some() {
                return internal_err!("remote fragment contains multiple input leaves");
            }
        } else {
            match plan.children().len() {
                0 => {
                    return internal_err!(
                        "remote fragment contains unsupported leaf {}",
                        plan.name()
                    );
                }
                1 => {}
                count => {
                    return internal_err!(
                        "remote fragment operator {} has {count} inputs; expected one",
                        plan.name()
                    );
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if template
        .properties()
        .output_partitioning()
        .partition_count()
        != 1
    {
        return internal_err!("remote fragment must have exactly one output partition");
    }
    input_schema
        .ok_or_else(|| DataFusionError::Internal("remote fragment has no input leaf".to_owned()))
}

/// Replaces the template's unique placeholder through DataFusion's normal tree
/// rewrite API so every parent recomputes its properties for the concrete input.
fn bind_fragment_input(
    template: &Arc<dyn ExecutionPlan>,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut input = Some(input);
    let bound = Arc::clone(template)
        .transform_up(|plan| {
            if plan.downcast_ref::<FragmentLeafExec>().is_some() {
                let input = input.take().ok_or_else(|| {
                    DataFusionError::Internal(
                        "remote fragment contains multiple input leaves".to_owned(),
                    )
                })?;
                Ok(Transformed::yes(input))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()?;
    if input.is_some() {
        return internal_err!("remote fragment has no input leaf");
    }
    Ok(bound)
}

/// Serializes the standard physical nodes and the custom fragment leaf, then
/// caches the declared output schema beside the plan bytes for negotiation.
fn encode_fragment(template: &Arc<dyn ExecutionPlan>) -> Result<RemoteQueryScannerFragment> {
    let serialized_plan = datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec(
        Arc::clone(template),
        &FragmentCodec,
    )?;
    Ok(RemoteQueryScannerFragment {
        format_version: REMOTE_FRAGMENT_FORMAT_VERSION,
        serialized_plan: serialized_plan.to_vec(),
        output_schema_bytes: encode_schema(&template.schema()),
    })
}

/// Codec for the sole Restate-specific node allowed in a remote fragment.
#[derive(Debug)]
struct FragmentCodec;

impl PhysicalExtensionCodec for FragmentCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _context: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!("FragmentLeafExec does not support children");
        }
        let schema = decode_schema(buf).map_err(|error| DataFusionError::External(error.into()))?;
        Ok(Arc::new(FragmentLeafExec::new(Arc::new(schema))))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(leaf) = node.downcast_ref::<FragmentLeafExec>() else {
            return internal_err!(
                "remote fragment contains unsupported extension {}",
                node.name()
            );
        };
        buf.extend_from_slice(&encode_schema(&leaf.schema()));
        Ok(())
    }
}

/// Stateful adapter used only while executing an otherwise immutable fragment.
/// Keeping it private to this module makes the wire/template layer above free of
/// input-stream lifecycle concerns.
mod stream_input {
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use futures::{TryStreamExt, stream};
    use parking_lot::Mutex;

    use super::*;

    /// Binds a fragment to a stream and executes its sole output partition. The
    /// input stream is consumed exactly once by DataFusion.
    pub(super) fn execute_recoverable(
        fragment: &RemoteFragment,
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, FragmentSetupFailure> {
        let schema = input.schema();
        let partition = Arc::new(OneShotInput::new(Arc::clone(&schema), input));
        let result = StreamingTableExec::try_new(
            schema,
            vec![Arc::clone(&partition) as Arc<dyn PartitionStream>],
            None,
            [],
            false,
            None,
        )
        .map(|input| Arc::new(input) as Arc<dyn ExecutionPlan>)
        .and_then(|input| fragment.bind_input(input))
        .and_then(|plan| plan.execute(0, context));

        result.map_err(|error| FragmentSetupFailure {
            error,
            input: partition
                .take()
                .expect("fragment setup does not poll its deferred input"),
        })
    }

    /// DataFusion partition adapter that transfers ownership of one existing
    /// stream without buffering or prefetching any record batches.
    struct OneShotInput {
        schema: SchemaRef,
        stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    }

    impl OneShotInput {
        fn new(schema: SchemaRef, stream: SendableRecordBatchStream) -> Self {
            Self {
                schema,
                stream: Arc::new(Mutex::new(Some(stream))),
            }
        }

        fn take(&self) -> Option<SendableRecordBatchStream> {
            self.stream.lock().take()
        }
    }

    impl Debug for OneShotInput {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("OneShotInput")
                .field("schema", &self.schema)
                .finish_non_exhaustive()
        }
    }

    impl PartitionStream for OneShotInput {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
            let schema = Arc::clone(&self.schema);
            let input = Arc::clone(&self.stream);
            let stream = stream::once(async move {
                input.lock().take().ok_or_else(|| {
                    DataFusionError::Internal(
                        "remote fragment attempted to execute its input more than once".to_owned(),
                    )
                })
            })
            .try_flatten();
            Box::pin(RecordBatchStreamAdapter::new(schema, stream))
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::format::DEFAULT_CAST_OPTIONS;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, CastExpr, Column, DynamicFilterPhysicalExpr, Literal, UnKnownColumn,
    };
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExecBuilder;
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::scalar::ScalarValue;
    use futures::StreamExt;

    use super::*;

    #[test]
    fn expression_safety_rejects_stateful_and_lossy_encodings() {
        let column = Arc::new(Column::new("value", 0)) as Arc<dyn PhysicalExpr>;
        let default_cast = Arc::new(CastExpr::new(Arc::clone(&column), DataType::Float64, None))
            as Arc<dyn PhysicalExpr>;
        assert!(is_remote_safe_expression(&default_cast));

        let mut non_default_options = DEFAULT_CAST_OPTIONS;
        non_default_options.safe = true;
        let non_default_cast = Arc::new(CastExpr::new(
            Arc::clone(&column),
            DataType::Float64,
            Some(non_default_options),
        )) as Arc<dyn PhysicalExpr>;
        assert!(!is_remote_safe_expression(&non_default_cast));

        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&column)],
            Arc::clone(&column),
        )) as Arc<dyn PhysicalExpr>;
        assert!(!is_remote_safe_expression(&dynamic));
        assert!(!is_remote_safe_expression(
            &(Arc::new(UnKnownColumn::new("missing")) as Arc<dyn PhysicalExpr>)
        ));
    }

    #[tokio::test]
    async fn arbitrary_unary_fragment_round_trips_and_executes() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let column =
            Arc::new(Column::new("value", 0)) as Arc<dyn datafusion::physical_plan::PhysicalExpr>;
        let filter = Arc::new(
            FilterExecBuilder::new(
                Arc::new(BinaryExpr::new(
                    Arc::clone(&column),
                    Operator::Gt,
                    Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
                )),
                Arc::new(FragmentLeafExec::new(Arc::clone(&schema))),
            )
            .build()
            .expect("filter fragment"),
        );
        let projection = ProjectionExec::try_new(
            [ProjectionExpr {
                expr: Arc::new(BinaryExpr::new(
                    column,
                    Operator::Plus,
                    Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
                )),
                alias: "shifted".to_owned(),
            }],
            filter,
        )
        .expect("projection fragment");
        let fragment = RemoteFragment::try_new(Arc::new(projection)).expect("valid fragment");
        let wire = fragment.to_wire();
        let context = Arc::new(TaskContext::default());
        let decoded = RemoteFragment::from_wire(&wire, &context, &schema)
            .expect("decode fragment")
            .expect("compatible fragment");

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("input batch");
        let input = Box::pin(
            MemoryStream::try_new(vec![batch], Arc::clone(&schema), None).expect("input stream"),
        );
        let output = decoded
            .execute(input, context)
            .expect("execute decoded fragment")
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .expect("fragment output");
        let values = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("projected integers");
        assert_eq!(values.values(), &[3, 4]);

        assert!(
            RemoteFragment::from_wire(&wire, &TaskContext::default(), &Arc::new(Schema::empty()))
                .expect("input schema mismatch is a clean decline")
                .is_none()
        );

        let mut incompatible_output = wire.clone();
        incompatible_output.output_schema_bytes = encode_schema(&schema);
        assert!(
            RemoteFragment::from_wire(&incompatible_output, &TaskContext::default(), &schema)
                .expect("output schema mismatch is a clean decline")
                .is_none()
        );

        let mut incompatible = wire;
        incompatible.format_version += 1;
        assert!(
            RemoteFragment::from_wire(&incompatible, &TaskContext::default(), &schema)
                .expect("version mismatch is a clean decline")
                .is_none()
        );

        let unsupported_leaf = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        assert!(RemoteFragment::try_new(unsupported_leaf).is_err());

        let multiple_inputs = UnionExec::try_new(vec![
            Arc::new(FragmentLeafExec::new(Arc::clone(&schema))),
            Arc::new(FragmentLeafExec::new(schema)),
        ])
        .expect("union template");
        assert!(RemoteFragment::try_new(multiple_inputs).is_err());
    }

    #[tokio::test]
    async fn fragment_setup_failure_returns_the_unconsumed_input() {
        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "expected",
            DataType::Int64,
            false,
        )]));
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "actual",
            DataType::Int64,
            false,
        )]));
        let fragment = RemoteFragment::try_new(Arc::new(FragmentLeafExec::new(expected_schema)))
            .expect("identity fragment");
        let batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("input batch");
        let input =
            Box::pin(MemoryStream::try_new(vec![batch], input_schema, None).expect("input stream"));

        let failure = match fragment.execute_recoverable(input, Arc::new(TaskContext::default())) {
            Ok(_) => panic!("schema mismatch should fail fragment setup"),
            Err(failure) => failure,
        };
        assert!(failure.error.to_string().contains("input schema mismatch"));
        let batches = failure
            .input
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .expect("recovered input remains readable");
        assert_eq!(batches[0].num_rows(), 3);
    }
}
