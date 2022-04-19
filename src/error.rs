use std::fmt::Display;
use std::fmt::Formatter;
#[derive(Debug)]
pub enum ElasticError {
    Connection(String),
    Send(String),
    Response(String),
    NotFound(String),
}

impl ElasticError {
    #[allow(dead_code)]
    fn error(&self) -> Option<String> {
        let value = self.to_owned();
        match value {
            ElasticError::Response(e) => Some(e.to_string()),
            ElasticError::Connection(e) => Some(e.to_string()),
            ElasticError::Send(e) => Some(e.to_string()),
            ElasticError::NotFound(e) => Some(e.to_string()),
        }
    }
}

impl Display for ElasticError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let error = self.error();
        write!(f, "{}", error.unwrap_or_default())
    }
}
