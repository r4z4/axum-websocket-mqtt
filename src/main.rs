use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use axum::{Router, extract::WebSocketUpgrade, response::IntoResponse, routing::get};
use futures_util::{
    sink::SinkExt,
    stream::{SplitSink, SplitStream, StreamExt},
};

use services::mqtt_svcs::{publish_default, publish_dh, publish_hc, publish_pr, publish_rgb};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::broadcast::{self, Receiver, Sender};
use std::time::Duration;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};

// Modules
mod services;

// const PORT: u16 = 3000;
const MQTT_PIXEL_HOST: &'static str = "192.168.1.139";
// const MQTT_ESP_HOST: &'static str = "192.168.1.13";
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
        "/ws/sensors",
        get({
            let state = state.clone();
            move |ws: WebSocketUpgrade| handle_websocket(ws, state.clone(), "sensors")
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
    topic: &'static str
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, topic))
}
async fn handle_socket(
    socket: axum::extract::ws::WebSocket,
    _state: Arc<Mutex<broadcast::Sender<String>>>,
    _topic: &'static str
) {
    // Subscribe to & Handle MQTT
    // let ws_topic = match topic {
    //     "pr" => "esp32/photoresistor",
    //     "re" => "esp32/rotary_encoder",
    //     "dh" => "esp32/sensor_data",
    //     "hc" => "esp32/sensor_data_hc_sr04",
    //     _ => ""
    // };
    let ws_topics = vec![
        "esp32/photoresistor",
        "esp32/rotary_encoder",
        "esp32/sensor_data",
        "esp32/rgb",
        "esp32/sensor_data_hc_sr04"
    ];
    let (tx, _rx) = broadcast::channel::<String>(100);

    // let tx = state.lock().await.clone();
    let rx = tx.subscribe();
   
    let arc_tx = Arc::new(Mutex::new(tx));

    let tx_ws = arc_tx.clone();

    for topic in ws_topics {
        tokio::spawn(subscribe_and_handle(arc_tx.clone(), topic));
        println!("Subscribing to {}", topic); 
    }
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
    let mut mqttoptions = MqttOptions::new(format!("rumqtt-async-{}", topic), MQTT_PIXEL_HOST, MQTT_PORT);
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

    let mut msg_count = -1;

    let (client, mut eventloop) = set_up_client(topic);
    
    // client.subscribe(topic, QoS::AtMostOnce).await.unwrap();

    // Unsure if this is beneficial at all
    match client.subscribe(topic, QoS::AtMostOnce).await {
        Ok(_ok) => (),
        Err(err) => println!("Mqtt error on subscribing: {:?}", err)
    }

    println!("Subscribing to {}", topic);

    while let Ok(notification) = eventloop.poll().await {
        // dbg!(notification.clone());
        match notification {
            Event::Incoming(packet) => {
                match packet {
                    Packet::Publish(msg) => {
                        if msg_count >= 19 {
                            msg_count = -1;
                        }
                        dbg!(msg.clone());
                        dbg!(msg.topic);

                        let js_topic = match topic {
                            "esp32/photoresistor" => "pr",
                            "esp32/rotary_encoder" => "re",
                            "esp32/ir" => "ir",
                            "esp32/rgb" => "rgb",
                            "esp32/sensor_data" => "dh",
                            "esp32/sensor_data_hc_sr04" => "hc",
                            _ => ""
                        };

                        // let msg_topic = msg.topic;
                        msg_count += 1;
                        let payload = msg.payload;
                        let payload_vec = payload.to_vec();
                        // callbacks.get(topic).map(|cb| cb(msg.payload));
                        // println!("{}", payload);
                        // let publish = format!("Topic => {} :: Payload => {:?}",
                        //     topic,
                        //     String::from_utf8(payload_vec.clone()).unwrap()
                        // );
                        let publish = publish_from_bytes(js_topic, payload_vec, msg_count).await;
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

async fn publish_from_bytes(topic: &'static str, bytes: Vec<u8>, msg_count: i32) -> String {
    match topic {
        "pr" => publish_pr(bytes),
        "dh" => publish_dh(bytes, msg_count),
        "re" => publish_re(bytes), // TODO: Impl
        "ir" => publish_ir(bytes), // TODO: Impl
        "rgb" => publish_rgb(bytes), // TODO: Impl
        "hc" => publish_hc(bytes, msg_count),
        _ => publish_default(bytes)
    }
}
