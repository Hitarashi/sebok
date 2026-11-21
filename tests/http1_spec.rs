mod common;

use common::{create_duplex, stream_output};
use futures_util::AsyncWriteExt;
use http::{Response, StatusCode};
use sebok::{RequestBody, ResponseBody, ServerConfig, serve_connection};

#[tokio::test]
async fn test_basic_get_request() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                assert_eq!(req.method(), http::Method::GET);
                assert_eq!(req.uri().path(), "/hello");

                let response = Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/plain")
                    .body(ResponseBody::from("Hello from Sebok!"))
                    .unwrap();
                Ok(response)
            },
            ServerConfig::default(),
        )
        .await
    });

    client
        .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    println!("SERVER RESULT: {:?}", result);

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    println!("RESPONSE STR: {:?}", response_str);

    assert!(response_str.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response_str.contains("content-type: text/plain\r\n"));
    assert!(response_str.contains("content-length: 17\r\n"));
    assert!(response_str.contains("date: "));
    assert!(response_str.contains("connection: close\r\n"));
    assert!(response_str.ends_with("Hello from Sebok!"));
}

#[tokio::test]
async fn test_http_1_0_defaults_to_connection_close() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let _ = req;
                let response = Response::builder()
                    .status(StatusCode::OK)
                    .body(ResponseBody::from("Legacy HTTP/1.0 response"))
                    .unwrap();
                Ok(response)
            },
            ServerConfig::default(),
        )
        .await
    });

    client
        .write_all(b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    println!("SERVER RESULT: {:?}", result);

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    println!("RESPONSE STR: {:?}", response_str);

    assert!(response_str.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response_str.contains("connection: close\r\n"));
    assert!(response_str.ends_with("Legacy HTTP/1.0 response"));
}

#[tokio::test]
async fn test_uri_with_query_parameters() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                assert_eq!(req.uri().path(), "/search");
                assert_eq!(req.uri().query(), Some("q=druto&limit=10"));

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
        .write_all(b"GET /search?q=druto&limit=10 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    assert!(response_str.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response_str.contains("content-length: 0\r\n"));
}

#[tokio::test]
async fn test_malformed_request_returns_400() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let _ = req;
                Ok(Response::new(ResponseBody::Empty))
            },
            ServerConfig::default(),
        )
        .await
    });

    client
        .write_all(b"NOT A VALID HTTP REQUEST\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_err());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    assert!(response_str.starts_with("HTTP/1.1 400 Bad Request\r\n"));
    assert!(response_str.contains("Connection: close\r\n"));
}

#[tokio::test]
async fn test_header_too_large_returns_431() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        let config = ServerConfig::builder().max_header_size(256).build();
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let _ = req;
                Ok(Response::new(ResponseBody::Empty))
            },
            config,
        )
        .await
    });

    let massive_header = "x-large-header: ".to_string() + &"a".repeat(500) + "\r\n";
    let request_payload = format!("GET / HTTP/1.1\r\nHost: localhost\r\n{massive_header}\r\n");

    client.write_all(request_payload.as_bytes()).await.unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_err());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    assert!(response_str.starts_with("HTTP/1.1 431 Request Header Fields Too Large\r\n"));
    assert!(response_str.contains("Connection: close\r\n"));
}

#[tokio::test]
async fn test_request_smuggling_defense_returns_400() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let _ = req;
                Ok(Response::new(ResponseBody::Empty))
            },
            ServerConfig::default(),
        )
        .await
    });

    // Contains both Content-Length and chunked Transfer-Encoding
    client
        .write_all(b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n0\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_err());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    assert!(response_str.starts_with("HTTP/1.1 400 Bad Request\r\n"));
    assert!(response_str.contains("Connection: close\r\n"));
}

#[tokio::test]
async fn test_service_error_handling() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let _ = req;
                Err(sebok::SebokError::PayloadTooLarge)
            },
            ServerConfig::default(),
        )
        .await
    });

    client
        .write_all(b"GET /error HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_err());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    assert!(response_str.starts_with("HTTP/1.1 413 Payload Too Large\r\n"));
    assert!(response_str.contains("Connection: close\r\n"));
}

#[tokio::test]
async fn test_response_explicit_connection_close() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let _ = req;
                let response = Response::builder()
                    .status(StatusCode::OK)
                    .header("connection", "close")
                    .body(ResponseBody::from("Closing immediately"))
                    .unwrap();
                Ok(response)
            },
            ServerConfig::default(),
        )
        .await
    });

    client
        .write_all(b"GET /close HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    assert!(response_str.contains("connection: close\r\n"));
}

#[tokio::test]
async fn test_custom_date_header_preserved() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let _ = req;
                let response = Response::builder()
                    .status(StatusCode::OK)
                    .header("date", "Sun, 01 Jan 2024 00:00:00 GMT")
                    .body(ResponseBody::Empty)
                    .unwrap();
                Ok(response)
            },
            ServerConfig::default(),
        )
        .await
    });

    client
        .write_all(b"GET /date HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    assert!(response_str.contains("date: Sun, 01 Jan 2024 00:00:00 GMT\r\n"));
}

#[tokio::test]
async fn test_http_1_0_explicit_keep_alive() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let _ = req;
                let response = Response::builder()
                    .status(StatusCode::OK)
                    .body(ResponseBody::from("HTTP/1.0 Keep-Alive"))
                    .unwrap();
                Ok(response)
            },
            ServerConfig::default(),
        )
        .await
    });

    client
        .write_all(b"GET /keep HTTP/1.0\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    assert!(response_str.contains("connection: keep-alive\r\n"));
}

#[tokio::test]
async fn test_response_explicit_content_length_and_keep_alive() {
    let (mut client, server) = create_duplex();

    let server_task = tokio::spawn(async move {
        serve_connection(
            server,
            |req: http::Request<RequestBody>| async move {
                let _ = req;
                let response = Response::builder()
                    .status(StatusCode::OK)
                    .header("content-length", "12")
                    .header("connection", "keep-alive")
                    .body(ResponseBody::from("customlength"))
                    .unwrap();
                Ok(response)
            },
            ServerConfig::default(),
        )
        .await
    });

    client
        .write_all(b"GET /custom HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    client.close().await.unwrap();

    let result = server_task.await.unwrap();
    assert!(result.is_ok());

    let response_bytes = stream_output(&client);
    let response_str = String::from_utf8_lossy(&response_bytes);
    assert!(response_str.contains("content-length: 12\r\n"));
    assert!(response_str.contains("connection: keep-alive\r\n"));
    assert!(response_str.ends_with("customlength"));
}
