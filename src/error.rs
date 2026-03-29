/// Errors that can occur during SQL transpilation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL parse error: {0}")]
    Parse(#[from] sqlparser::parser::ParserError),

    #[error("Unsupported feature: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, Error>;
