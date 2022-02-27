use futures::executor::block_on;
use influxrs;
use mercury::Message;
use paho_mqtt as mqtt;
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

    let influx_client = influxrs::InfluxClient::builder(
        std::env::var("INFLUX_URL").unwrap(),
        std::env::var("INFLUX_KEY").unwrap(),
        std::env::var("INFLUX_ORG").unwrap(),
    ).build().unwrap();

    let msgs: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));

    let t_msgs = Arc::clone(&msgs);
    let thread = thread::spawn(move || loop {
        println!("Messaged queued: {}", (*t_msgs.lock().unwrap()).len());
        (*t_msgs.lock().unwrap()).drain(..);
        thread::sleep(Duration::from_secs(15));
    });

    for msg in reciever.iter().flatten() {
        match serde_json::from_str::<Message>(&msg.payload_str()) {
            Ok(data) => {
                println!("{:#?}", &data);
                let measurement = influxrs::Measurement::builder("temperature")
                    .field("temperature_f", data.temperature_f)
                    .field("temperature_c", data.temperature_c)
                    .tag("author", data.author.to_string())
                    .timestamp_ms((data.timestamp * 1000).into())
                    .build()
                    .unwrap();

                if let Err(e) = block_on(influx_client.write("mercury", &[measurement])) {
                    println!("Error inserting into InfluxDB: {}", e);
                }

                (*msgs.lock().unwrap()).push(data);
            }
            Err(e) => println!("Error deserializing message: {}", e),
        }
    }
    thread.join().unwrap();
}
