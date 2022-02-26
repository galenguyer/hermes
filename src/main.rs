use chrono::{TimeZone, Utc};
use mercury::Message;
use paho_mqtt as mqtt;
use rinfluxdb::line_protocol::blocking::Client;
use rinfluxdb::line_protocol::LineBuilder;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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

    let influx_client = Client::new(
        url::Url::parse("http://mercury.student.rit.edu:8086").unwrap(),
        {
            let credentials: Option<(String, String)> = None;
            credentials
        },
    ).unwrap();

    let msgs: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));

    let t_msgs = Arc::clone(&msgs);
    let thread = thread::spawn(move || loop {
        println!("Messaged queued: {}", (*t_msgs.lock().unwrap()).len());
        (*t_msgs.lock().unwrap()).drain(..);
        thread::sleep(Duration::from_secs(15));
    });

    for msg in reciever.iter().flatten() {
        let data: Message = serde_json::from_str(&msg.payload_str()).unwrap();
        println!("{:#?}", &data);
        let line = LineBuilder::new("temperature")
            .insert_field("temperature_f", data.temperature_f as f64)
            .insert_field("temperature_c", data.temperature_c as f64)
            .insert_tag("author", data.author.to_string())
            .set_timestamp(Utc.timestamp(data.timestamp.try_into().unwrap(), 0))
            .build();
        influx_client.send("mercury", &[line]).unwrap();

        (*msgs.lock().unwrap()).push(data);
    }
    thread.join().unwrap();
}
