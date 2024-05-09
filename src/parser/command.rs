use std::fmt::Display;

#[derive(Debug, Clone, Copy)]
pub enum Command {
    Ping,
    Echo,
    Get,
    Set,
    Type,
    XAdd,
    Info,
    ReplConf,
    PSync,
}

impl Command {
    pub(super) fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "ping" => Some(Self::Ping),
            "echo" => Some(Self::Echo),
            "get" => Some(Self::Get),
            "set" => Some(Self::Set),
            "type" => Some(Self::Type),
            "xadd" => Some(Self::XAdd),
            "info" => Some(Self::Info),
            "replconf" => Some(Self::ReplConf),
            "psync" => Some(Self::PSync),
            _ => None,
        }
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ping => write!(f, "PING"),
            Self::Echo => write!(f, "ECHO"),
            Self::Get => write!(f, "GET"),
            Self::Set => write!(f, "SET"),
            Self::Type => write!(f, "TYPE"),
            Self::XAdd => write!(f, "XADD"),
            Self::Info => write!(f, "INFO"),
            Self::ReplConf => write!(f, "REPLCONF"),
            Self::PSync => write!(f, "PSYNC"),
        }
    }
}
