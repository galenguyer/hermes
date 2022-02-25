use mercury::Message;
use paho_mqtt as mqtt;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
struct Source {
    author: String,
    location: String,
}

fn main() {
    let mut client = match mqtt::Client::new(
        mqtt::CreateOptionsBuilder::new()
            .server_uri("tcp://mercury.student.rit.edu")
            .client_id("hermes")
            .finalize(),
    ) {
        Ok(c) => c,
        Err(e) => panic!("Error creating MQTT client:\n\t{}", e),
    };

    let reciever = client.start_consuming();

    if let Err(e) = client.connect(
        mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(false)
            .user_name("hermes")
            .password(std::env::var("MQTT_PASSWORD").unwrap())
            .finalize(),
    ) {
        panic!("Unable to connect:\n\t{}", e);
    }

    if let Err(e) = client.subscribe("mercury", mqtt::QOS_0) {
        panic!("Unable to subscribe:\n\t{}", e);
    }

    let msgs: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));

    let t_msgs = Arc::clone(&msgs);
    let thread = thread::spawn(move || loop {
        println!("Messaged queued: {}", (*t_msgs.lock().unwrap()).len());
        (*t_msgs.lock().unwrap()).drain(..);
        thread::sleep(Duration::from_secs(15));
    });

    for msg in reciever.iter() {
        if let Some(message) = msg {
            let data: Message = serde_json::from_str(&message.payload_str()).unwrap();
            println!("{:?}", data.clone());
            (*msgs.lock().unwrap()).push(data);
        }
    }
    thread.join().unwrap();
}
