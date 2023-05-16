use crate::types;
use crate::utils::GenericError;
use base64::engine::general_purpose::STANDARD;
use base64::prelude::*;
use std::error::Error;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Default, thiserror::Error)]
#[error("cannot parse the opaque id, bad format")]
pub struct ParseError {
    #[source]
    cause: Option<GenericError>,
}

impl ParseError {
    pub fn from_cause(cause: impl Error + Send + Sync + 'static) -> Self {
        Self {
            cause: Some(Box::new(cause)),
        }
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
pub struct ServiceInvocationId(String);

impl fmt::Display for ServiceInvocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<types::ServiceInvocationId> for ServiceInvocationId {
    fn from(value: types::ServiceInvocationId) -> Self {
        ServiceInvocationId(format!(
            "{}-{}-{}",
            value.invocation_id.as_simple(),
            value.service_id.service_name,
            STANDARD.encode(value.service_id.key)
        ))
    }
}

impl TryFrom<ServiceInvocationId> for types::ServiceInvocationId {
    type Error = ParseError;

    fn try_from(value: ServiceInvocationId) -> Result<Self, Self::Error> {
        let ServiceInvocationId(str) = value;

        // This encoding is based on the fact that neither invocation_id
        // nor service_name can contain the '-' character
        // Invocation id is serialized as simple
        // Service name follows the fullIdent ABNF here:
        // https://protobuf.dev/reference/protobuf/proto3-spec/#identifiers
        let mut splits: Vec<&str> = str.splitn(3, '-').collect();
        if splits.len() != 3 {
            return Err(ParseError::default());
        }
        let key = STANDARD
            .decode(splits.pop().unwrap())
            .map_err(ParseError::from_cause)?;
        let service_name = splits.pop().unwrap().to_string();
        let invocation_id: Uuid = splits
            .pop()
            .unwrap()
            .parse()
            .map_err(ParseError::from_cause)?;

        Ok(types::ServiceInvocationId::new(
            service_name,
            key,
            invocation_id,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_service_invocation_id() {
        let expected_sid = types::ServiceInvocationId::new(
            "my.example.Service",
            "-------stuff------".as_bytes().to_vec(),
            Uuid::now_v7(),
        );
        let opaque_sid: ServiceInvocationId = expected_sid.clone().into();
        let actual_sid: types::ServiceInvocationId = opaque_sid.try_into().unwrap();
        assert_eq!(expected_sid, actual_sid);
    }
}
