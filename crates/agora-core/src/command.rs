use anyhow::{bail, Result};

/// Split a command string into argv-style parts.
///
/// This intentionally handles only shell-like quoting and backslash escaping.
/// It does not execute through a shell, so shell expansions are not supported.
pub fn split_command_line(input: &str) -> Result<Vec<String>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;

    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        match ch {
            '\\' => escaped = true,
            '\'' | '"' if quote == Some(ch) => quote = None,
            '\'' | '"' if quote.is_none() => quote = Some(ch),
            c if c.is_whitespace() && quote.is_none() => {
                if !current.is_empty() {
                    parts.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }

    if escaped {
        current.push('\\');
    }
    if let Some(q) = quote {
        bail!("unterminated {q} quote in command");
    }
    if !current.is_empty() {
        parts.push(current);
    }
    if parts.is_empty() {
        bail!("command is empty");
    }

    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::split_command_line;

    #[test]
    fn splits_plain_args() {
        assert_eq!(
            split_command_line("nats-server -js -p 4222").unwrap(),
            vec!["nats-server", "-js", "-p", "4222"]
        );
    }

    #[test]
    fn preserves_quoted_args() {
        assert_eq!(
            split_command_line("tool --name \"backend developer\" 'hello world'").unwrap(),
            vec!["tool", "--name", "backend developer", "hello world"]
        );
    }

    #[test]
    fn rejects_unclosed_quotes() {
        assert!(split_command_line("tool \"unterminated").is_err());
    }
}
