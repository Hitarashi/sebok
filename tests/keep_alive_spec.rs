mod common;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use common::{create_duplex, stream_output};
use futures_util::AsyncWriteExt;
use http::{Response, StatusCode};
use sebok::{RequestBody, ResponseBody, ServerConfig, serve_connection};

#[tokio::test]
async fn test_sequential_keep_alive_requests() {
    let (mut client, server) = create_duplex();
    let request_count = Arc::new(AtomicUsize::new(0));
    let count_clone = request_count.clone();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            move |req: http::Request<RequestBody>| {
                let count = count_clone.clone();
                async move {
                    let current = count.fetch_add(1, Ordering::SeqCst);
                    match current {
                        0 => {
                            assert_eq!(req.uri().path(), "/first");
                            let response = Response::builder()
                                .status(StatusCode::OK)
                                .body(ResponseBody::from("First response"))
                                .unwrap();
                            Ok(response)
                        }
                        1 => {
                            assert_eq!(req.uri().path(), "/second");
                            let body = req.into_body().into_bytes().await.unwrap();
                            assert_eq!(body, "payload for second");
                            let response = Response::builder()
                                .status(StatusCode::OK)
                                .body(ResponseBody::from("Second response"))
                                .unwrap();
                            Ok(response)
                        }
                        2 => {
                            assert_eq!(req.uri().path(), "/third");
                            let response = Response::builder()
                                .status(StatusCode::OK)
                                .body(ResponseBody::from("Third response"))
                                .unwrap();
                            Ok(response)
                        }
                        _ => panic!("Unexpected request index {current}"),
                    }
                }
            },
            ServerConfig::default(),
        )
        .await
    });

    // Write request 1
    client
        .write_all(b"GET /first HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    // Write request 2
    let payload = "payload for second";
    let req2_head = format!(
        "POST /second HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
        payload.len()
    );
    client.write_all(req2_head.as_bytes()).await.unwrap();
    client.write_all(payload.as_bytes()).await.unwrap();

    // Write request 3 (with Connection: close)
    client
        .write_all(b"GET /third HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    assert_eq!(request_count.load(Ordering::SeqCst), 3);

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);

    assert!(response_str.contains("First response"));
    assert!(response_str.contains("Second response"));
    assert!(response_str.contains("Third response"));
}

#[tokio::test]
async fn test_pipelined_requests() {
    let (mut client, server) = create_duplex();
    let request_count = Arc::new(AtomicUsize::new(0));
    let count_clone = request_count.clone();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            move |req: http::Request<RequestBody>| {
                let count = count_clone.clone();
                async move {
                    let idx = count.fetch_add(1, Ordering::SeqCst);
                    let response = Response::builder()
                        .status(StatusCode::OK)
                        .body(ResponseBody::from(format!(
                            "Pipeline {idx}: {}",
                            req.uri().path()
                        )))
                        .unwrap();
                    Ok(response)
                }
            },
            ServerConfig::default(),
        )
        .await
    });

    // Both requests sent together in one pipelined buffer
    let pipelined_data = b"GET /one HTTP/1.1\r\nHost: localhost\r\n\r\nGET /two HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    client.write_all(pipelined_data).await.unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    assert_eq!(request_count.load(Ordering::SeqCst), 2);

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);

    assert!(response_str.contains("Pipeline 0: /one"));
    assert!(response_str.contains("Pipeline 1: /two"));
}

#[tokio::test]
async fn test_unread_fixed_body_draining() {
    let (mut client, server) = create_duplex();
    let request_count = Arc::new(AtomicUsize::new(0));
    let count_clone = request_count.clone();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            move |req: http::Request<RequestBody>| {
                let count = count_clone.clone();
                async move {
                    let idx = count.fetch_add(1, Ordering::SeqCst);
                    if idx == 0 {
                        // Crucial: we do NOT read the request body here, intentionally dropping it!
                        let _ = req;
                        let response = Response::builder()
                            .status(StatusCode::OK)
                            .body(ResponseBody::from("First response ignoring body"))
                            .unwrap();
                        Ok(response)
                    } else {
                        assert_eq!(req.uri().path(), "/second_after_drain");
                        let response = Response::builder()
                            .status(StatusCode::OK)
                            .body(ResponseBody::from("Second response aligned successfully"))
                            .unwrap();
                        Ok(response)
                    }
                }
            },
            ServerConfig::default(),
        )
        .await
    });

    let ignored_payload = "This payload is 50 bytes long and will be ignored!";
    let req1 = format!(
        "POST /first_with_body HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{}",
        ignored_payload.len(),
        ignored_payload
    );

    let req2 = "GET /second_after_drain HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

    client.write_all(req1.as_bytes()).await.unwrap();
    client.write_all(req2.as_bytes()).await.unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    assert_eq!(request_count.load(Ordering::SeqCst), 2);

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);

    assert!(response_str.contains("First response ignoring body"));
    assert!(response_str.contains("Second response aligned successfully"));
}

#[tokio::test]
async fn test_unread_chunked_body_draining() {
    let (mut client, server) = create_duplex();
    let request_count = Arc::new(AtomicUsize::new(0));
    let count_clone = request_count.clone();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            move |req: http::Request<RequestBody>| {
                let count = count_clone.clone();
                async move {
                    let idx = count.fetch_add(1, Ordering::SeqCst);
                    if idx == 0 {
                        // Do NOT read chunked body
                        let _ = req;
                        let response = Response::builder()
                            .status(StatusCode::OK)
                            .body(ResponseBody::from("Ignored chunked body"))
                            .unwrap();
                        Ok(response)
                    } else {
                        assert_eq!(req.uri().path(), "/after_chunked_drain");
                        let response = Response::builder()
                            .status(StatusCode::OK)
                            .body(ResponseBody::from("Clean alignment after chunked drain"))
                            .unwrap();
                        Ok(response)
                    }
                }
            },
            ServerConfig::default(),
        )
        .await
    });

    let req1_head =
        "POST /first_chunked HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n";
    let chunk1 = "5\r\nhello\r\n";
    let chunk2 = "6\r\nworld!\r\n";
    let terminal = "0\r\n\r\n";
    let req2 = "GET /after_chunked_drain HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

    client.write_all(req1_head.as_bytes()).await.unwrap();
    client.write_all(chunk1.as_bytes()).await.unwrap();
    client.write_all(chunk2.as_bytes()).await.unwrap();
    client.write_all(terminal.as_bytes()).await.unwrap();
    client.write_all(req2.as_bytes()).await.unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    assert_eq!(request_count.load(Ordering::SeqCst), 2);

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);

    assert!(response_str.contains("Ignored chunked body"));
    assert!(response_str.contains("Clean alignment after chunked drain"));
}
