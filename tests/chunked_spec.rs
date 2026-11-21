mod common;

use common::{create_duplex, stream_output};
use futures_util::AsyncWriteExt;
use http::{Response, StatusCode};
use sebok::{RequestBody, ResponseBody, ServerConfig, serve_connection};

#[tokio::test]
async fn test_chunked_request_body_decoding() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let body_bytes = req.into_body().into_bytes().await.unwrap();
                assert_eq!(body_bytes, "Wikipedia\n in\n\nchunks.");

                let response = Response::builder()
                    .status(StatusCode::OK)
                    .body(ResponseBody::from("Chunked body received"))
                    .unwrap();
                Ok(response)
            },
            ServerConfig::default(),
        )
        .await
    });

    let request_head = "POST /chunked HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
    let chunk1 = "4\r\nWiki\r\n";
    let chunk2 = "6\r\npedia\n\r\n";
    let chunk3 = "C\r\n in\n\nchunks.\r\n";
    let terminal = "0\r\n\r\n";

    client.write_all(request_head.as_bytes()).await.unwrap();
    client.write_all(chunk1.as_bytes()).await.unwrap();
    client.write_all(chunk2.as_bytes()).await.unwrap();
    client.write_all(chunk3.as_bytes()).await.unwrap();
    client.write_all(terminal.as_bytes()).await.unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    assert!(response_str.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response_str.ends_with("Chunked body received"));
}

#[tokio::test]
async fn test_chunked_response_streaming() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let _ = req;
                let payload_stream = futures_util::io::Cursor::new(
                    b"Streaming dynamically generated payload via Sebok server engine".to_vec(),
                );

                let response = Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/plain")
                    .body(ResponseBody::from_stream(payload_stream))
                    .unwrap();
                Ok(response)
            },
            ServerConfig::default(),
        )
        .await
    });

    client
        .write_all(b"GET /stream HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);

    assert!(response_str.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response_str.contains("transfer-encoding: chunked\r\n"));
    assert!(response_str.ends_with("0\r\n\r\n"));
    assert!(
        response_str.contains("Streaming dynamically generated payload via Sebok server engine")
    );
}
