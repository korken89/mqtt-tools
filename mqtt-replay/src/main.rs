use log::*;
use regex::RegexSet;
use rumqttc::{Client, ClientConfig, MqttOptions, TlsConfiguration, Transport};
use serde::Deserialize;
use simple_logger::SimpleLogger;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};
use structopt::StructOpt;

// Reference:
// {"time": 1611137748.0325797, "qos": 0, "retain": true, "topic": "kvarntorp-test/gateway/165640a7e023861a/nodeversion", "msg_b64": "IjAuMi4xNSI="}

#[derive(Deserialize, Debug)]
struct MqttMessage {
    time: f64,
    qos: u8,
    retain: bool,
    topic: String,
    msg_b64: String,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "mqtt-replay", about = "A replay of an logged MQTT stream")]
struct Opt {
    /// The verbosity of output from this program, the higher the more output one can expect
    #[structopt(short, long, env = "VERBOSITY", default_value = "2")]
    verbosity: u32,

    /// Input log file
    #[structopt(env = "INPUT", parse(from_os_str))]
    input: PathBuf,

    /// Server address
    #[structopt(short, long, env = "SERVER", default_value = "localhost")]
    server: String,

    /// Server port
    #[structopt(short, long, env = "PORT")]
    port: Option<u16>,

    /// Replay speed multiplier
    #[structopt(long, env = "SPEED", default_value = "1.0")]
    speed: f64,

    /// Skip for certain amount of time
    #[structopt(long, env = "SKIP", default_value = "0.0")]
    skip: f64,

    /// Topic rejection regex, can be multiple or comma-separated: REGEX1,REGEX2,...
    #[structopt(long, use_delimiter = true, env = "TOPIC_REJECTION_REGEX")]
    filter_topic: Vec<String>,

    /// TLS enable
    #[structopt(long, env = "TLS")]
    tls: bool,

    /// Path to custom CA file
    #[structopt(long, env = "CUSTOM_CA")]
    custom_ca: Option<PathBuf>,

    /// The file is a ZSTD compressed log file
    #[structopt(long, env = "ZSTD")]
    zstd: Option<bool>,

    /// Password for the MQTT connection.
    #[structopt(long, env = "MQTT_PASSWORD")]
    password: Option<String>,

    /// Client ID for the MQTT connection.
    #[structopt(long, env = "MQTT_CLIENT_ID")]
    client_id: Option<String>,

    /// Username for the MQTT connection.
    #[structopt(long, env = "MQTT_USERNAME")]
    username: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let input = opt.input;
    let server = opt.server;
    let port = opt.port.unwrap_or(if opt.tls || opt.custom_ca.is_some() {
        8883
    } else {
        1883
    });
    let speed = opt.speed;
    let skip_to_time = opt.skip;
    let zstd = opt
        .zstd
        .unwrap_or(input.extension() == Some(OsStr::new("zst")));

    match opt.verbosity {
        0 => SimpleLogger::new().with_level(log::LevelFilter::Off),
        1 => SimpleLogger::new().with_level(log::LevelFilter::Error),
        2 => SimpleLogger::new().with_level(log::LevelFilter::Info),
        3 => SimpleLogger::new().with_level(log::LevelFilter::Debug),
        _ => SimpleLogger::new().with_level(log::LevelFilter::Trace),
    }
    .init()?;

    assert!(
        opt.speed > 0.0,
        "Playback speed multiplier needs to be larger than 0"
    );

    // println!("filter_topic: {:#?}", opt.filter_topic);

    // let re = Regex::new(r"tag/[[:xdigit:]]+/position").unwrap();
    // println!("is match: {}", re.is_match("/wispr/tag/f133d7df5ac4efa2/position"));
    let filter_topic = if !opt.filter_topic.is_empty() {
        Some(RegexSet::new(&opt.filter_topic).unwrap())
    } else {
        None
    };

    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::new(0, 1))
        .subsec_nanos();

    let client_id = if let Some(client_id) = opt.client_id {
        client_id
    } else {
        format!("mqtt-logger-sub{}", nanos)
    };

    let mut mqtt_options = MqttOptions::new(client_id, &server, port);

    // Check for custom CA file
    let custom_ca = if let Some(custom_ca_path) = &opt.custom_ca {
        use std::io::prelude::*;

        let ca_str = custom_ca_path.to_str();

        if !(ca_str == Some("null") || ca_str == Some("None") || ca_str == Some("none")) {
            let mut vec = Vec::new();

            File::open(custom_ca_path)
                .expect("Could not open CA file")
                .read_to_end(&mut vec)
                .expect("Could not read specified CA file");

            Some(vec)
        } else {
            None
        }
    } else {
        None
    };

    // Authentication?
    if let (Some(username), Some(password)) = (opt.username, opt.password) {
        mqtt_options.set_credentials(username, password);
    }

    // Encrypted MQTT?
    if opt.tls || custom_ca.is_some() {
        let transport = if let Some(custom_ca) = &custom_ca {
            Transport::Tls(TlsConfiguration::Simple {
                ca: custom_ca.clone(),
                alpn: None,
                client_auth: None,
            })
        } else {
            use rustls::RootCertStore;

            let root_certs = rustls_native_certs::load_native_certs()
                .expect("Failed to load platform certificates.");

            let root_certs: Vec<Vec<u8>> = root_certs
                .into_iter()
                .map(|certificate| certificate.0)
                .collect();

            let mut rootcert = RootCertStore::empty();
            rootcert.add_parsable_certificates(&root_certs);

            let client_config = ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(rootcert)
                .with_no_client_auth();

            // let mut client_config = ClientConfig::new();
            // // Use rustls-native-certs to load root certificates from the operating system.
            // client_config.root_store = rootcert;

            Transport::tls_with_config(client_config.into())
        };

        mqtt_options.set_transport(transport);
    }

    mqtt_options.set_keep_alive(Duration::from_secs(5));
    let (mut mqtt_client, mut connection) = Client::new(mqtt_options, 10);

    info!(
        "Replaying log from '{}' on address '{}:{}' with multiplier {}...",
        input.to_str().unwrap(),
        server,
        port,
        speed,
    );

    if !opt.filter_topic.is_empty() {
        let filters = opt.filter_topic.join(", ");
        info!("The following topic filters are active: {}", filters);
    }

    let start_time_local = SystemTime::now();
    let mut start_time_log: f64 = 0.0;
    let mut first_message = true;
    let mut seek_done = skip_to_time == 0.;

    let mut f = BufReader::new(File::open(&input)?);
    let mut zf = zstd::Decoder::new(BufReader::new(File::open(&input)?))?;
    let keep_running = Arc::new(AtomicBool::new(true));
    let thread_keep_running = keep_running.clone();

    thread::spawn(move || {
        let log_file: &mut dyn std::io::Read = if zstd { &mut zf } else { &mut f };
        let log_file = BufReader::new(log_file);

        for line in log_file.lines() {
            let line = if let Ok(line) = line {
                line
            } else {
                continue;
            };

            trace!("{:?}", &line);

            let msg: MqttMessage = match serde_json::from_str(&line) {
                Ok(msg) => msg,
                Err(e) => {
                    error!(
                        "Corrupted dataset: Serde error with line '{}', error: {}",
                        &line, e
                    );
                    continue;
                }
            };

            // Check for filtered message
            let filter_message = filter_topic
                .as_ref()
                .map(|re| re.is_match(&msg.topic))
                .unwrap_or(false);

            if seek_done && !filter_message {
                // Widefind messages
                let qos = match rumqttc::qos(msg.qos) {
                    Ok(q) => q,
                    Err(e) => {
                        error!("Corrupted dataset: QOS invalid '{}'", e);
                        continue;
                    }
                };

                let b64 = match base64::decode(msg.msg_b64) {
                    Ok(b) => b,
                    Err(e) => {
                        error!("Corrupted dataset: data is not base64 encoded '{}'", e);
                        continue;
                    }
                };

                mqtt_client
                    .publish(msg.topic, qos, msg.retain, b64)
                    .unwrap();
            }

            if first_message {
                start_time_log = msg.time;
                first_message = false;
            } else {
                let log_duration = msg.time - start_time_log;
                let speed_log_duration = log_duration / speed;

                let duration_left = if !seek_done && log_duration < skip_to_time {
                    0.
                } else {
                    if !seek_done {
                        seek_done = true;
                        start_time_log = msg.time;

                        info!("Seek until timestamp {} seconds completed!", skip_to_time);

                        0.
                    } else {
                        f64::max(
                            speed_log_duration
                                - SystemTime::now()
                                    .duration_since(start_time_local)
                                    .unwrap()
                                    .as_secs_f64(),
                            0.0,
                        )
                    }
                };

                if duration_left > 0.0 {
                    thread::sleep(Duration::from_secs_f64(duration_left));
                }
            }
        }

        info!("Dataset completed, shutting down...");

        thread_keep_running.store(false, Ordering::SeqCst);
    });

    for _notification in connection.iter() {
        if !keep_running.load(Ordering::SeqCst) {
            break;
        }
    }

    Ok(())
}
