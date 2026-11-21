mod common;

use common::{create_duplex, stream_output};
use futures_util::AsyncWriteExt;
use http::{Response, StatusCode};
use sebok::{RequestBody, ResponseBody, ServerConfig, serve_connection};

#[tokio::test]
async fn test_fixed_length_body_into_bytes() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let body_bytes = req.into_body().into_bytes().await.unwrap();
                assert_eq!(body_bytes, "Hello Druto Framework!");

                let response = Response::builder()
                    .status(StatusCode::OK)
                    .body(ResponseBody::from("Received payload!"))
                    .unwrap();
                Ok(response)
            },
            ServerConfig::default(),
        )
        .await
    });

    let payload = "Hello Druto Framework!";
    let request_head = format!(
        "POST /submit HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );

    client.write_all(request_head.as_bytes()).await.unwrap();
    client.write_all(payload.as_bytes()).await.unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    assert!(response_str.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response_str.ends_with("Received payload!"));
}

#[tokio::test]
async fn test_empty_request_body() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let body_bytes = req.into_body().into_bytes().await.unwrap();
                assert!(body_bytes.is_empty());

                let response = Response::builder()
                    .status(StatusCode::OK)
                    .body(ResponseBody::Empty)
                    .unwrap();
                Ok(response)
            },
            ServerConfig::default(),
        )
        .await
    });

    client
        .write_all(b"POST /empty HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_large_fixed_body_streaming() {
    let (mut client, server) = create_duplex();

    let large_size = 512 * 1024; // 512 KB
    let payload = vec![b'x'; large_size];

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let body_bytes = req.into_body().into_bytes().await.unwrap();
                assert_eq!(body_bytes.len(), 512 * 1024);

                let response = Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/octet-stream")
                    .body(ResponseBody::Bytes(body_bytes))
                    .unwrap();
                Ok(response)
            },
            ServerConfig::default(),
        )
        .await
    });

    let request_head = format!(
        "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        large_size
    );

    client.write_all(request_head.as_bytes()).await.unwrap();
    client.write_all(&payload).await.unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    let response_bytes = stream_output(&client);
    assert!(response_bytes.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(response_bytes.ends_with(&payload));
}
