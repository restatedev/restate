use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::PhysicalSortExpr;

pub(crate) fn compute_ordering(schema: SchemaRef) -> Option<Vec<PhysicalSortExpr>> {
    let ordering = vec![PhysicalSortExpr {
        expr: col("partition_key", &schema).ok()?,
        options: Default::default(),
    }];

    Some(ordering)
}
