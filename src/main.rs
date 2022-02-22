use mercury::Message;
use paho_mqtt as mqtt;
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
            .finalize(),
    ) {
        panic!("Unable to connect:\n\t{}", e);
    }

    if let Err(e) = client.subscribe("mercury", mqtt::QOS_2) {
        panic!("Unable to subscribe:\n\t{}", e);
    }

    for msg in reciever.iter() {
        if let Some(message) = msg {
            let data: Message = serde_json::from_str(&message.payload_str()).unwrap();
            println!("{:#?}", data);
        }
    }
}
