use http::header::InvalidHeaderValue;
use http::Uri;
use restate_schema_api::invocation_target::BadInputContentType;
use restate_types::errors::GenericError;
use restate_types::identifiers::DeploymentId;

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum Error {
    // Those are generic and used by all schema resources
    #[error("not found in the schema registry")]
    #[code(unknown)]
    NotFound,
    #[error("already exists in the schema registry")]
    #[code(unknown)]
    Override,

    // Specific resources errors
    #[error(transparent)]
    Component(
        #[from]
        #[code]
        ComponentError,
    ),
    #[error(transparent)]
    Deployment(
        #[from]
        #[code]
        DeploymentError,
    ),
    #[error(transparent)]
    Subscription(
        #[from]
        #[code]
        SubscriptionError,
    ),
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum ComponentError {
    #[error("cannot insert/modify component '{0}' as it contains a reserved name")]
    #[code(restate_errors::META0005)]
    ReservedName(String),
    #[error("detected a new component '{0}' revision with a component type different from the previous revision. Component type cannot be changed across revisions")]
    #[code(restate_errors::META0006)]
    DifferentType(String),
    #[error("the component '{0}' already exists but the new revision removed the handlers {1:?}")]
    #[code(restate_errors::META0006)]
    RemovedHandlers(String, Vec<String>),
    #[error("the handler '{0}' input content-type is not valid: {1}")]
    #[code(unknown)]
    BadInputContentType(String, BadInputContentType),
    #[error("the handler '{0}' output content-type is not valid: {1}")]
    #[code(unknown)]
    BadOutputContentType(String, InvalidHeaderValue),
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(restate_errors::META0009)]
pub enum SubscriptionError {
    #[error(
        "invalid source URI '{0}': must have a scheme segment, with supported schemes: [kafka]."
    )]
    InvalidSourceScheme(Uri),
    #[error("invalid source URI '{0}': source URI of Kafka type must have a authority segment containing the cluster name.")]
    InvalidKafkaSourceAuthority(Uri),

    #[error(
        "invalid sink URI '{0}': must have a scheme segment, with supported schemes: [component]."
    )]
    InvalidSinkScheme(Uri),
    #[error("invalid sink URI '{0}': sink URI of component type must have a authority segment containing the component name.")]
    InvalidComponentSinkAuthority(Uri),
    #[error("invalid sink URI '{0}': cannot find component/handler specified in the sink URI.")]
    SinkComponentNotFound(Uri),

    #[error(transparent)]
    #[code(unknown)]
    Validation(GenericError),
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum DeploymentError {
    #[error("existing deployment id is different from requested (requested = {requested}, existing = {existing})")]
    #[code(restate_errors::META0004)]
    IncorrectId {
        requested: DeploymentId,
        existing: DeploymentId,
    },
}
