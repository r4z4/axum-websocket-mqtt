use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use axum::{Router, extract::WebSocketUpgrade, response::IntoResponse, routing::get};
use futures_util::{
    sink::SinkExt,
    stream::{SplitSink, SplitStream, StreamExt},
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::broadcast::{self, Receiver, Sender};

#[tokio::main]
async fn main() {
    // Create a shared broadcast channel for messages
    let (tx, _rx) = broadcast::channel::<String>(100);
    let state = Arc::new(Mutex::new(tx));
    // Build the Axum application
    let app = Router::new().route(
        "/ws",
        get({
            let state = state.clone();
            move |ws: WebSocketUpgrade| handle_websocket(ws, state.clone())
        }),
    );
    // Start the server
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Listening on http://127.0.0.1:3000");
    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}
async fn handle_websocket(
    ws: WebSocketUpgrade,
    state: Arc<Mutex<broadcast::Sender<String>>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}
async fn handle_socket(
    socket: axum::extract::ws::WebSocket,
    state: Arc<Mutex<broadcast::Sender<String>>>,
) {
    let tx = state.lock().await.clone();
    let rx = tx.subscribe();

    let (sender, receiver) = socket.split();
    tokio::spawn(write(rx, sender));
    tokio::spawn(read(tx, receiver));

    // Spawn a task to receive messages from other clients
    // tokio::spawn(async move {
    async fn write(mut rx: Receiver<String>, mut sender: SplitSink<WebSocket, Message>) {
        while let Ok(msg) = rx.recv().await {
            if let Ok(_) = sender
                .send(axum::extract::ws::Message::Text(
                    Utf8Bytes::try_from(msg).unwrap(),
                ))
                .await
            {
                continue;
            }
            break;
        }
    }
    // tokio::spawn(async move {
    async fn read(tx: Sender<String>, mut receiver: SplitStream<WebSocket>) {
        // Read messages from the client and broadcast them
        while let Some(Ok(msg)) = receiver.next().await {
            dbg!("Received Message {:?}", msg.clone());
            if let axum::extract::ws::Message::Text(text) = msg {
                let _ = tx.send(text.to_string());
            }
        }
    }
}
