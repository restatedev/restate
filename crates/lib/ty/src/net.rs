// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub use crate::protobuf::{ProtocolVersion, ServiceTag};

// #[derive(thiserror::Error, Debug)]
// pub enum EncodeError {
//     #[error(
//         "message of type {type_tag} requires peer to have minimum version {min_required:?} but actual negotiated version is {actual:?}."
//     )]
//     IncompatibleVersion {
//         type_tag: &'static str,
//         min_required: ProtocolVersion,
//         actual: ProtocolVersion,
//     },
// }
//
// pub trait Service: Send + Unpin + 'static {
//     const TAG: ServiceTag;
// }
//
// pub trait RpcRequest: WireEncode + WireDecode + Send + 'static {
//     const TYPE: &str;
//     type Service: Service;
//     type Response: RpcResponse;
// }
//
// pub trait RpcResponse: WireEncode + WireDecode + Unpin + Send {
//     type Service: Service;
// }
//
// pub trait WatchRequest: WireEncode + WireDecode + Send + 'static {
//     const TYPE: &str;
//     type Service: Service;
//     type Response: WatchResponse;
// }
//
// pub trait WatchResponse: WireEncode + WireDecode + Unpin + Send {
//     type Service: Service;
// }
//
// pub trait UnaryMessage: WireEncode + WireDecode + Send + 'static {
//     const TYPE: &str;
//     type Service: Service;
// }
//
// pub trait WireEncode {
//     fn encode_to_bytes(&self, protocol_version: ProtocolVersion) -> Result<Bytes, EncodeError>;
// }
//
// pub trait WireDecode {
//     type Error: std::fmt::Debug + Into<anyhow::Error>;
//     /// Panics if decode failed
//     fn decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Self
//     where
//         Self: Sized,
//     {
//         Self::try_decode(buf, protocol_version).expect("decode WireDecode message failed")
//     }
//
//     fn try_decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Result<Self, Self::Error>
//     where
//         Self: Sized;
// }
//
// impl<T> WireEncode for Box<T>
// where
//     T: WireEncode,
// {
//     fn encode_to_bytes(&self, protocol_version: ProtocolVersion) -> Result<Bytes, EncodeError> {
//         self.as_ref().encode_to_bytes(protocol_version)
//     }
// }
//
// impl<T> WireDecode for Box<T>
// where
//     T: WireDecode,
// {
//     type Error = T::Error;
//     fn try_decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Result<Self, Self::Error>
//     where
//         Self: Sized,
//     {
//         Ok(Box::new(T::try_decode(buf, protocol_version)?))
//     }
// }
//
// impl<T> WireDecode for Arc<T>
// where
//     T: WireDecode,
// {
//     type Error = T::Error;
//
//     fn try_decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Result<Self, Self::Error>
//     where
//         Self: Sized,
//     {
//         Ok(Arc::new(T::try_decode(buf, protocol_version)?))
//     }
// }
//
// /// to define a service we need
// /// - Service type
// /// - Service Tag
// ///
// /// Example:
// /// ```ignore
// ///   define_service! {
// ///       @service = IngressService,
// ///       @tag = ServiceTag::Ingress,
// ///   }
// /// ```
// #[macro_export]
// macro_rules! define_service {
//     (
//         @service = $service:ty,
//         @tag = $tag:expr,
//     ) => {
//         impl restate_ty::net::Service for $service {
//             const TAG: restate_ty::net::ServiceTag = $tag;
//         }
//     };
// }
//
// /// to define a unary message, we need
// /// - Message type
// /// - service type
// ///
// /// Example:
// /// ```ignore
// ///   define_unary_message! {
// ///       @message = IngressMessage,
// ///       @service = IngressService,
// ///   }
// /// ```
// #[macro_export]
// macro_rules! define_unary_message {
//     (
//         @message = $message:ty,
//         @service = $service:ty,
//     ) => {
//         impl restate_ty::net::UnaryMessage for $message {
//             const TYPE: &str = stringify!($message);
//             type Service = $service;
//         }
//     };
// }
//
// /// to define an RPC, we need
// /// - Request type
// /// - request service tag
// /// - Service type
// ///
// /// Example:
// /// ```ignore
// ///   define_rpc! {
// ///       @request = AttachRequest,
// ///       @response = AttachResponse,
// ///       @service = ClusterControllerService,
// ///   }
// /// ```
// #[macro_export]
// macro_rules! define_rpc {
//     (
//         @request = $request:ty,
//         @response = $response:ty,
//         @service = $service:ty,
//     ) => {
//         impl restate_ty::net::RpcRequest for $request {
//             const TYPE: &str = stringify!($request);
//             type Response = $response;
//             type Service = $service;
//         }
//
//         impl restate_ty::net::RpcResponse for $response {
//             type Service = $service;
//         }
//     };
// }
//
// pub use {define_rpc, define_service, define_unary_message};
