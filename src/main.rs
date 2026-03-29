use std::io::{self, Read};

use papera::dialect::SourceDialect;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let dialect = match args.get(1).map(|s| s.as_str()) {
        Some("trino") => SourceDialect::Trino,
        Some("redshift") => SourceDialect::Redshift,
        Some("hive") => SourceDialect::Hive,
        Some(other) => {
            eprintln!("Unknown dialect: {other}. Supported: trino, redshift, hive");
            std::process::exit(1);
        }
        None => {
            eprintln!("Usage: papera <trino|redshift|hive>");
            eprintln!("  Reads SQL from stdin and writes DuckDB-compatible SQL to stdout.");
            std::process::exit(1);
        }
    };

    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .expect("failed to read stdin");

    match papera::transpile(&input, dialect) {
        Ok(output) => println!("{output}"),
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}
