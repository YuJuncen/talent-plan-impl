use log4rs::append::console::{ConsoleAppender, Target};
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log::LevelFilter;

pub fn config() -> Config {
    let stderr = ConsoleAppender::builder()
        .target(Target::Stderr)
        .encoder(Box::new(PatternEncoder::new(concat!(
        "{T}=>kvs[",
        env!("CARGO_PKG_VERSION"),
        "]@{d(%Y-%m-%d %H:%M:%S)}=>{t}: {m}{n}"
        ))))
        .build();
    let stdout = ConsoleAppender::builder()
        .target(Target::Stdout)
        .encoder(Box::new(PatternEncoder::new(concat!(
        "{T}=>kvs[",
        env!("CARGO_PKG_VERSION"),
        "]@{d(%Y-%m-%d %H:%M:%S)}=>{t}: {m}{n}"
        ))))
        .build();
    Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("stderr", Box::new(stderr)))
        .logger(Logger::builder()
            .appender("stderr")
            .build("app::error", LevelFilter::Error))
        .logger(Logger::builder()
            .appender("stdout")
            .build("app::request", LevelFilter::Info))
        .build(Root::builder()
            .appender("stdout")
            .build(LevelFilter::Info))
        .unwrap()
}