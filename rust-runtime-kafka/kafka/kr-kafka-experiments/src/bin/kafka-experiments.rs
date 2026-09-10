fn main() -> std::process::ExitCode {
    match kr_kafka_experiments::cli::run_cli(std::env::args().skip(1)) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}
