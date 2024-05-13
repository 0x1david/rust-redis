use std::fmt::Display;

/// Represents the various commands supported by a Redis-like service.
///
/// This enum encapsulates all supported commands, providing an easy reference
/// to all functionality that can be invoked through textual input.
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
    /// Parses a string reference into a corresponding `Command`.
    ///
    /// This method performs a case-insensitive comparison to match the input string
    /// to a known command. It is used primarily to translate text commands received
    /// from a client into their corresponding enum representations.
    ///
    /// # Parameters
    /// - `s`: A reference to a string slice that holds the command to be parsed.
    ///
    /// # Returns
    /// - `Some(Command)`: If the input string matches a known command.
    /// - `None`: If no matching command is found.
    ///
    /// # Examples
    /// ```
    /// use your_crate::Command;
    ///
    /// assert_eq!(Command::parse(&"ping"), Some(Command::Ping));
    /// assert_eq!(Command::parse(&"notacommand"), None);
    /// ```
    pub(super) fn parse<T: AsRef<str>>(s: &T) -> Option<Self> {
        match s.as_ref().to_lowercase().as_str() {
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
    /// Provides a string representation of a command suitable for display or logging.
    ///
    /// This method is used to convert `Command` enum variants into their respective
    /// textual representations, matching the commands understood by a Redis-like server.
    ///
    /// # Returns
    /// - A string slice representing the command.
    ///
    /// # Examples
    /// ```
    /// use your_crate::Command;
    /// use std::fmt::Display;
    ///
    /// assert_eq!(Command::Ping.to_string(), "PING");
    /// ```
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
