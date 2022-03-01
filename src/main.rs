use futures::executor::block_on;
use influxrs;
use mercury::Message;
use paho_mqtt as mqtt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{env, thread};

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
            .password(env::var("MQTT_PASSWORD").unwrap())
            .finalize(),
    ) {
        panic!("Unable to connect:\n\t{}", e);
    }

    if let Err(e) = client.subscribe("mercury", mqtt::QOS_0) {
        panic!("Unable to subscribe:\n\t{}", e);
    }

    let influx_client = influxrs::InfluxClient::builder(
        env::var("INFLUX_URL").unwrap(),
        env::var("INFLUX_KEY").unwrap(),
        env::var("INFLUX_ORG").unwrap(),
    )
    .build()
    .unwrap();

    let msgs: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));

    let t_msgs = Arc::clone(&msgs);
    let thread = thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(15));

        println!("Messaged queued: {}", (*t_msgs.lock().unwrap()).len());

        let lines: Vec<influxrs::Measurement> = (*t_msgs.lock().unwrap())
            .iter()
            .map(|m| {
                influxrs::Measurement::builder("temperature")
                    .field("temperature_f", m.temperature_f)
                    .field("temperature_c", m.temperature_c)
                    .tag("author", m.author.to_string())
                    .timestamp_ms((m.timestamp * 1000).into())
                    .build()
                    .unwrap()
            })
            .collect();

        if let Err(e) = block_on(influx_client.write("mercury", &lines)) {
            println!("Error inserting into InfluxDB: {}", e);
        }

        (*t_msgs.lock().unwrap()).drain(..);
    });

    for msg in reciever.iter().flatten() {
        match serde_json::from_str::<Message>(&msg.payload_str()) {
            Ok(data) => {
                println!("{:#?}", &data);
                (*msgs.lock().unwrap()).push(data);
            }
            Err(e) => println!("Error deserializing message: {}", e),
        }
    }
    thread.join().unwrap();
}
