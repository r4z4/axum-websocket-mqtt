use std::str::FromStr;

use serde::Serialize;

#[derive(Serialize)]
struct WsMessage {
    topic: &'static str,
    data: String
}

pub fn publish_pr(bytes: Vec<u8>) -> String {
    let msg = get_string_from_utf(bytes);
    let int = parse_val::<i32>(&msg);
    let brightness: i32 = int / 10;
    let data = format!("hsl(90 100% {}%)", brightness);
    let ws_msg = WsMessage {
        topic: "rgb",
        data
    };
    send_ws(&ws_msg)
}

pub fn publish_rgb(bytes: Vec<u8>) -> String {
    let msg = get_string_from_utf(bytes);
    let data =
    if let Some(vec) = parse_message_to_int(msg.as_str(), 3) {
        format!("r: {}, g: {}, b: {}", vec[0], vec[1], vec[2])
    } else {
        "r: 0, g: 0, b:0".to_string()
    };
    let ws_msg = WsMessage {
        topic: "rgb",
        data
    };
    send_ws(&ws_msg)
}

// Unsure what is being sent from MQTT
pub fn publish_ir(bytes: Vec<u8>) -> String {
    let msg = get_string_from_utf(bytes);
    // let x_val = 2;
    let data = msg;
    let ws_msg = WsMessage {
        topic: "dh",
        data
    };
    send_ws(&ws_msg)
}

pub fn publish_re(bytes: Vec<u8>) -> String {
    let msg = get_string_from_utf(bytes);
    let data =
    if let Some(vec) = parse_message_to_int(msg.as_str(), 3) {
        format!("ctr: {}, dir: {}, btn: {}", vec[0], vec[1], vec[2])
    } else {
        "ctr: 0, dir: 0, btn:0".to_string()
    };
    let ws_msg = WsMessage {
        topic: "re",
        data
    };
    send_ws(&ws_msg)
}

pub fn publish_dh(bytes: Vec<u8>, msg_count: i32) -> String {
    let msg = get_string_from_utf(bytes);
    // let x_val = 2;
    let data = 
        if let Some((temp, hum)) = parse_message_to_float(msg.as_str()) {
            format!("<div class='dot' style='--x: {}; --y: {}'></div><div class='dot' style='--x: {}; --y: {}'></div>", msg_count, temp / 20.0, msg_count, hum / 20.0)
        } else {
            "<div class='dot' style='--x: 19; --y: 19'><div>".to_string()
        };
    let ws_msg = WsMessage {
        topic: "dh",
        data
    };
    send_ws(&ws_msg)
}

pub fn publish_hc(bytes: Vec<u8>, msg_count: i32) -> String {
    let msg = get_string_from_utf(bytes);
    let data =
    if let Some(vec) = parse_message_to_int(msg.as_str(), 2) {
        let cms = vec[1]; // Only CMs
        format!("<div class='dot' style='--x: {}; --y: {}'></div>", msg_count, cms)
    } else {
        "<div class='dot' style='--x: 19; --y: 19'><div>".to_string()
    };
    let ws_msg = WsMessage {
        topic: "hc",
        data
    };
    send_ws(&ws_msg)
}

pub fn publish_default(bytes: Vec<u8>) -> String {
    let ws_msg = WsMessage {topic: "", data: get_string_from_utf(bytes) };
    send_ws(&ws_msg)
}

fn send_ws(ws_msg: &WsMessage) -> String {
    match serde_json::to_string(ws_msg) {
        Ok(msg) => msg,
        Err(_err) => "".to_string()
    }
}

fn parse_val<T>(arg: &str) -> T 
where T: Default + FromStr {
    match arg.parse::<T>() {
        Ok(ok) => ok,
        Err(_err) => {
            println!("Error parsing val: {:?}", arg);
            T::default()
        }
    }
}

fn get_string_from_utf(vec: Vec<u8>) -> String {
    match String::from_utf8(vec.clone()) {
        Ok(ok) => ok,
        Err(err) => {
            println!("Error parsing bytes: {:?}", err);
            "".to_string()
        }
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
    
    let new_first_parts: Vec<&str> = first_part.split(':').collect();
    let first_number = new_first_parts[1].trim();

    let new_second_parts: Vec<&str> = second_part.split(':').collect();
    let second_number = new_second_parts[1].trim();

    let first_flt = parse_val::<f32>(first_number);
    let second_flt = parse_val::<f32>(second_number);

    Some((first_flt, second_flt))

}

fn parse_message_to_int(message: &str, num_parts: i8) -> Option<Vec<u32>> {
    let parts: Vec<&str> = message.split(';').collect();
    
    match num_parts {
        2 => {
            if parts.len() != 2 {
                return None; // Invalid format
            }
        },
        3 => {
            if parts.len() != 3 {
                return None; // Invalid format
            }
        },
        _ => {
            println!("Invalid num parts");
            return None;
        }
    }

    let first_part = parts[0];
    let second_part = parts[1];

    // let first_val = first_part.chars().last().unwrap();
    // let second_val = second_part.chars().last().unwrap();
    
    let new_first_parts: Vec<&str> = first_part.split(':').collect();
    let first_number = new_first_parts[1].trim();

    let new_second_parts: Vec<&str> = second_part.split(':').collect();
    let second_number = new_second_parts[1].trim();

    let first_int = parse_val::<u32>(first_number);
    let second_int = parse_val::<u32>(second_number);

    let mut vec = vec![first_int, second_int];

    if num_parts == 3 {
        let third_part = parts[2];
        let new_third_parts: Vec<&str> = third_part.split(':').collect();
        let third_number = new_third_parts[1].trim();
        let third_int = parse_val::<u32>(third_number);
        vec.push(third_int);
    }
    
    // Some((first_int, second_int))
    Some(vec)
}
