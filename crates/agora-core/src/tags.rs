pub const MAX_TAGS_PER_SESSION: usize = 10;

/// Validate and normalize a tag: lowercase, trim, must match `^[a-z0-9][a-z0-9_-]{0,63}$`.
pub fn validate_tag(input: &str) -> anyhow::Result<String> {
    let normalized = input.trim().to_lowercase();
    if normalized.is_empty() {
        anyhow::bail!("tag cannot be empty");
    }
    if normalized.len() > 64 {
        anyhow::bail!("tag exceeds 64 characters");
    }
    let bytes = normalized.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() {
        anyhow::bail!("tag must start with a letter or digit");
    }
    if !normalized
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
    {
        anyhow::bail!(
            "tag may only contain lowercase alphanumeric characters, dashes, and underscores"
        );
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_tags() {
        assert_eq!(validate_tag("feature").unwrap(), "feature");
        assert_eq!(validate_tag("bug-123").unwrap(), "bug-123");
        assert_eq!(validate_tag("my_tag").unwrap(), "my_tag");
        assert_eq!(validate_tag("0day").unwrap(), "0day");
        assert_eq!(validate_tag("  Feature  ").unwrap(), "feature");
    }

    #[test]
    fn rejects_empty() {
        assert!(validate_tag("").is_err());
        assert!(validate_tag("   ").is_err());
    }

    #[test]
    fn rejects_too_long() {
        let long = "a".repeat(65);
        assert!(validate_tag(&long).is_err());
    }

    #[test]
    fn accepts_max_length() {
        let max = "a".repeat(64);
        assert!(validate_tag(&max).is_ok());
    }

    #[test]
    fn rejects_special_chars() {
        assert!(validate_tag("<script>").is_err());
        assert!(validate_tag("'; DROP").is_err());
        assert!(validate_tag("has space").is_err());
        assert!(validate_tag("emoji🎉").is_err());
        assert!(validate_tag("dot.dot").is_err());
    }

    #[test]
    fn rejects_leading_non_alnum() {
        assert!(validate_tag("-start").is_err());
        assert!(validate_tag("_start").is_err());
    }
}
