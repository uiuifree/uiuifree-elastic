use std::fmt::Display;
use std::fmt::Formatter;
#[derive(Debug)]
pub enum ElasticError {
    Connection(String),
    Send(String),
    JsonParse(String),
    Status(u16, String),
    Response(String),
    NotFound(String),
}

impl ElasticError {
    #[allow(dead_code)]
    fn error(&self) -> Option<String> {
        let value = self.to_owned();
        match value {
            ElasticError::JsonParse(e) => Some(e.to_string()),
            ElasticError::Response(e) => Some(e.to_string()),
            ElasticError::Connection(e) => Some(e.to_string()),
            ElasticError::Status(_, e) => Some(e.to_string()),
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
