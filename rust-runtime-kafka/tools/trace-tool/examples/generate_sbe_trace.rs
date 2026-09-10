use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

#[path = "support/browser_sbe_sample.rs"]
mod browser_sbe_sample;

fn usage_error(program: &OsString) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("usage: {} <output.sbe>", PathBuf::from(program).display()),
    )
}

fn output_path() -> Result<PathBuf, io::Error> {
    let mut arguments = std::env::args_os();
    let program = arguments
        .next()
        .unwrap_or_else(|| OsString::from("generate_sbe_trace"));
    let Some(output) = arguments.next() else {
        return Err(usage_error(&program));
    };
    if arguments.next().is_some() {
        return Err(usage_error(&program));
    }
    Ok(output.into())
}

fn main() -> Result<(), Box<dyn Error>> {
    let output_path = output_path()?;
    let output = File::create(&output_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("could not create {}: {error}", output_path.display()),
        )
    })?;
    let mut output = BufWriter::new(output);
    output.write_all(&browser_sbe_sample::build_browser_sbe_sample()?)?;
    output.flush()?;

    Ok(())
}
