//! Service traits and request handler contracts.

use std::{future::Future, pin::Pin};

use http::{Request, Response};

use crate::{
    body::{RequestBody, ResponseBody},
    error::SebokError,
};

/// Type alias for heap-allocated pinned asynchronous futures.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Asynchronous service contract mapping incoming HTTP requests to outgoing responses.
pub trait HttpService: Send + Sync + 'static {
    /// Processes an incoming request and returns an asynchronous response future.
    fn call(
        &self,
        request: Request<RequestBody>,
    ) -> BoxFuture<'static, Result<Response<ResponseBody>, SebokError>>;
}

impl<F, Fut> HttpService for F
where
    F: Fn(Request<RequestBody>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Response<ResponseBody>, SebokError>> + Send + 'static,
{
    fn call(
        &self,
        request: Request<RequestBody>,
    ) -> BoxFuture<'static, Result<Response<ResponseBody>, SebokError>> {
        Box::pin((self)(request))
    }
}
