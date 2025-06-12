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
    let (tx, _rx) = broadcast::channel::<String>(100);
    // let topic_dh = "esp32/sensor_data";
    // let topic_hc = "esp32/sensor_data_hc_sr04";
    // let topic_pr = "esp32/photoresistor";
    // let topic_re = "esp32/rotary_encoder";

    // let tx = state.lock().await.clone();
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

    let mut msg_count = -1;

    let (client, mut eventloop) = set_up_client(topic);
    client.subscribe(topic, QoS::AtMostOnce).await.unwrap();

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
                        let publish = publish_from_bytes(topic, payload_vec, msg_count).await;

                        // println!(
                        //     "Topic => {} :: Payload => {:?}",
                        //     topic,
                        //     String::from_utf8(payload_vec)
                        // );
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
        "esp32/photoresistor" => {
            let msg = String::from_utf8(bytes.clone()).unwrap();
            let int = msg.parse::<i32>().unwrap();
            let brightness: i32 = int / 10;
            format!("hsl(90 100% {}%)", brightness)
        },
        "esp32/sensor_data" => {
            // <div class="dot" style="--x: 1; --y: 3"></div>
            let msg = String::from_utf8(bytes.clone()).unwrap();
            // let x_val = 2;
            if let Some((temp, hum)) = parse_message_to_float(msg.as_str()) {
                format!("<div class='dot' style='--x: {}; --y: {}'></div><div class='dot' style='--x: {}; --y: {}'></div>", msg_count, temp / 20.0, msg_count, hum / 20.0)
            } else {
                "<div class='dot' style='--x: 19; --y: 19'><div>".to_string()
            } 
        },
        "esp32/sensor_data_hc_sr04" => {
            let msg = String::from_utf8(bytes.clone()).unwrap();
            // let x_val = 2;
            if let Some((_ins, cms)) = parse_message_to_int(msg.as_str()) {
                // Only CMs
                format!("<div class='dot' style='--x: {}; --y: {}'></div>", msg_count, cms)
            } else {
                "<div class='dot' style='--x: 19; --y: 19'><div>".to_string()
            } 
        }
        _ => String::from_utf8(bytes.clone()).unwrap(),
    }
}

fn parse_message_to_float(message: &str) -> Option<(f32, f32)> {

    let parts: Vec<&str> = message.split(';').collect();
    
    if parts.len() != 2 {
        return None; // Invalid format
    }

    let first_part = parts[0];
    let second_part = parts[1];

    // let first_val = first_part.chars().last().unwrap();
    // let second_val = second_part.chars().last().unwrap();
    //
    let new_first_parts: Vec<&str> = first_part.split(':').collect();
    let first_number = new_first_parts[1].trim();

    let new_second_parts: Vec<&str> = second_part.split(':').collect();
    let second_number = new_second_parts[1].trim();

    let first_flt = first_number.parse::<f32>().unwrap();
    let second_flt = second_number.parse::<f32>().unwrap();

    Some((first_flt, second_flt))

}
fn parse_message_to_int(message: &str) -> Option<(u32, u32)> {
    let parts: Vec<&str> = message.split(';').collect();
    
    if parts.len() != 2 {
        return None; // Invalid format
    }

    let first_part = parts[0];
    let second_part = parts[1];

    // let first_val = first_part.chars().last().unwrap();
    // let second_val = second_part.chars().last().unwrap();
    //
    let new_first_parts: Vec<&str> = first_part.split(':').collect();
    let first_number = new_first_parts[1].trim();

    let new_second_parts: Vec<&str> = second_part.split(':').collect();
    let second_number = new_second_parts[1].trim();

    let first_int = first_number.parse::<u32>().unwrap();
    let second_int = second_number.parse::<u32>().unwrap();

    Some((first_int, second_int))

}
