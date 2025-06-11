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
use std::time::Duration;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};

const PORT: u16 = 3000;
const MQTT_HOST: &'static str = "192.168.1.139";
const MQTT_PORT: u16 = 1883;
const MQTT_ID: &'static str = "admin";
const MQTT_PASS: &'static str = "adminpwd";

#[tokio::main]
async fn main() {
    // Create a shared broadcast channel for messages
    let (tx, _rx) = broadcast::channel::<String>(100);
    let state = Arc::new(Mutex::new(tx));
    // Build the Axum application
    let app = Router::new().route(
        "/ws/dh",
        get({
            let state = state.clone();
            move |ws: WebSocketUpgrade| handle_websocket(ws, state.clone(), "dh")
        }),
    )
    .route(
        "/ws/hc",
        get({
            let state = state.clone();
            move |ws: WebSocketUpgrade| handle_websocket(ws, state.clone(), "hc")
        }),
    ) 
    .route(
        "/ws/re",
        get({
            let state = state.clone();
            move |ws: WebSocketUpgrade| handle_websocket(ws, state.clone(), "re")
        }),
    )
    .route(
        "/ws/pr",
        get({
            let state = state.clone();
            move |ws: WebSocketUpgrade| handle_websocket(ws, state.clone(), "pr")
        })
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
    topic: &'static str
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, topic))
}
async fn handle_socket(
    socket: axum::extract::ws::WebSocket,
    state: Arc<Mutex<broadcast::Sender<String>>>,
    topic: &'static str
) {
    // Subscribe to & Handle MQTT
    let ws_topic = match topic {
        "pr" => "esp32/photoresistor",
        "re" => "esp32/rotary_encoder",
        "dh" => "esp32/sensor_data",
        "hc" => "esp32/sensor_data_hc_sr04",
        _ => ""
    };
    // let topic_dh = "esp32/sensor_data";
    // let topic_hc = "esp32/sensor_data_hc_sr04";
    // let topic_pr = "esp32/photoresistor";
    // let topic_re = "esp32/rotary_encoder";

    let tx = state.lock().await.clone();
    let rx = tx.subscribe();
   
    let arc_tx = Arc::new(Mutex::new(tx));
    // let tx1 = arc_tx.clone();
    // let tx2 = arc_tx.clone();
    // let tx3 = arc_tx.clone();
    // let tx4 = arc_tx.clone();

    let tx_ws = arc_tx.clone();

    // // DH Sensor
    // tokio::spawn(subscribe_and_handle(tx1, topic_dh));
    // // Ultrasonic Sensor
    // tokio::spawn(subscribe_and_handle(tx2, topic_hc));
    // // Photoresistor
    // tokio::spawn(subscribe_and_handle(tx3, topic_pr));
    // // Rotary Encoder
    // tokio::spawn(subscribe_and_handle(tx4, topic_re));

    tokio::spawn(subscribe_and_handle(arc_tx.clone(), ws_topic));
    println!("Subscribing to {}", topic); 
    // WS Splits
    let (sender, receiver) = socket.split();
    tokio::spawn(write(rx, sender));
    tokio::spawn(read(tx_ws, receiver));

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
    async fn read(tx: Arc<Mutex<Sender<String>>>, mut receiver: SplitStream<WebSocket>) {
        // Read messages from the client and broadcast them
        while let Some(Ok(msg)) = receiver.next().await {
            dbg!("Received Message {:?}", msg.clone());
            if let axum::extract::ws::Message::Text(text) = msg {
                let sender = tx.lock().await;
                let _ = sender.send(text.to_string());
            }
        }
    }
}

fn set_up_client(topic: &'static str) -> (AsyncClient, EventLoop) {
    println!("Setting Up Client for {}", topic);
    let mut mqttoptions = MqttOptions::new(format!("rumqtt-async-{}", topic), MQTT_HOST, MQTT_PORT);
    mqttoptions.set_credentials(MQTT_ID, MQTT_PASS);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);
    return (client, eventloop);
}

async fn subscribe_and_handle(
    tx: Arc<Mutex<Sender<String>>>,
    topic: &'static str,
) {
    println!("Setting up subscription");

    let (client, mut eventloop) = set_up_client(topic);
    client.subscribe(topic, QoS::AtMostOnce).await.unwrap();

    println!("Subscribing to {}", topic);

    while let Ok(notification) = eventloop.poll().await {
        // dbg!(notification.clone());
        match notification {
            Event::Incoming(packet) => {
                match packet {
                    Packet::Publish(msg) => {
                        let topic = msg.topic;
                        let payload = msg.payload;
                        let payload_vec = payload.to_vec();
                        // callbacks.get(topic).map(|cb| cb(msg.payload));
                        // println!("{}", payload);
                        let publish = format!("Topic => {} :: Payload => {:?}",
                            topic,
                            String::from_utf8(payload_vec.clone()).unwrap()
                        );
                        println!(
                            "Topic => {} :: Payload => {:?}",
                            topic,
                            String::from_utf8(payload_vec)
                        );
                        let sender = tx.lock().await;
                        let _ = sender.send(publish);
                    }
                    _ => (),
                }
            }
            _ => (),
        }
    }
}
