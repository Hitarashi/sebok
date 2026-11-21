mod common;

use common::{create_duplex, stream_output};
use futures_util::{AsyncReadExt, AsyncWriteExt};
use http::{Response, StatusCode};
use sebok::{RequestBody, ResponseBody, ServerConfig, serve_connection};

#[tokio::test]
async fn test_expect_100_continue_handshake() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        let config = ServerConfig::builder().expect_continue(true).build();
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let body = req.into_body().into_bytes().await.unwrap();
                assert_eq!(body, "Large uploaded file payload");

                let response = Response::builder()
                    .status(StatusCode::OK)
                    .body(ResponseBody::from("Upload processed successfully"))
                    .unwrap();
                Ok(response)
            },
            config,
        )
        .await
    });

    let payload = "Large uploaded file payload";
    let request_head = format!(
        "POST /upload HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );

    // 1. Client sends request head only
    client.write_all(request_head.as_bytes()).await.unwrap();

    // 2. Client awaits 100 Continue response
    let mut continue_buf = [0u8; 25];
    client.read_exact(&mut continue_buf).await.unwrap();
    assert_eq!(&continue_buf, b"HTTP/1.1 100 Continue\r\n\r\n");

    // 3. Client sends payload
    client.write_all(payload.as_bytes()).await.unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);

    assert!(response_str.contains("HTTP/1.1 200 OK\r\n"));
    assert!(response_str.ends_with("Upload processed successfully"));
}
