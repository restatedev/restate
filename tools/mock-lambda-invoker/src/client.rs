use anyhow::{Context, anyhow};
use bytes::Bytes;
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use restate_service_client::{
    AssumeRoleCacheMode, Endpoint, Method, Parts, Request, ResponseBody, ServiceClient,
};
use restate_types::config::ServiceClientOptions;
use restate_types::identifiers::LambdaARN;

#[derive(Clone)]
pub struct ConfiguredClient {
    parts: Parts,
    client: ServiceClient,
}

pub struct ClientResponse {
    pub status: StatusCode,
    pub body: Option<Bytes>,
}

impl ConfiguredClient {
    pub fn new(
        opts: &ServiceClientOptions,
        lambda_arn: impl Into<String>,
        service_name: impl Into<String>,
        handler_name: impl Into<String>,
    ) -> Self {
        let arn: LambdaARN = lambda_arn
            .into()
            .parse()
            .expect("failed to parse lambda arn");

        let path_and_query: PathAndQuery = PathAndQuery::try_from(format!(
            "/invoke/{}/{}",
            service_name.into(),
            handler_name.into()
        ))
        .expect("failed to parse path query");

        let mut headers = HeaderMap::new();
        headers.append(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/vnd.restate.invocation.v5"),
        );

        let parts = Parts::new(
            Method::POST,
            Endpoint::Lambda(arn, None),
            path_and_query,
            headers,
        );

        let client = ServiceClient::from_options(opts, AssumeRoleCacheMode::Unbounded)
            .expect("Failed to create lambda client");

        Self { client, parts }
    }

    pub async fn invoke(&self, buf: Bytes) -> anyhow::Result<ClientResponse> {
        let body = BoxBody::new(Full::new(buf));
        let req = Request::new(self.parts.clone(), body);
        let res = self.client.call(req).await?;
        let code = res.status();
        if !code.is_success() {
            anyhow::bail!("status: {}", code);
        }
        let response_body = res.into_body();
        match response_body {
            ResponseBody::Left(_) => {
                panic!("only relevant for http endpoints")
            }
            ResponseBody::Right(mut full) => {
                let maybe_frame = full.frame().await.ok_or_else(|| {
                    anyhow!("there must be a frame for a successful response to lambda")
                })?;
                let frame = maybe_frame.context("a completed frame must contain body")?;
                let body = frame.into_data().expect("an ok frame without a body");
                Ok(ClientResponse {
                    status: code,
                    body: Some(body),
                })
            }
        }
    }
}
