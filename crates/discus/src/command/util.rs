//! Helpers shared by command handlers.

use bytes::Bytes;

use super::error::CommandError;
use crate::node::Node;

/// Parse a canonical signed 64-bit integer. Rejects `+5`, spaces, and empty input.
pub fn parse_i64(b: &[u8]) -> Result<i64, CommandError> {
    if b.is_empty() || b[0] == b'+' {
        return Err(CommandError::NotInteger);
    }
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or(CommandError::NotInteger)
}

pub fn parse_f64(b: &[u8]) -> Result<f64, CommandError> {
    let f = std::str::from_utf8(b)
        .ok()
        .filter(|s| !s.is_empty() && !s.starts_with(char::is_whitespace))
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or(CommandError::NotFloat)?;
    if f.is_nan() {
        return Err(CommandError::NotFloat);
    }
    Ok(f)
}

pub fn upper(b: &Bytes) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

pub fn check_key(node: &Node, key: &[u8]) -> Result<(), CommandError> {
    if key.len() > node.config.max_key_bytes {
        return Err(CommandError::KeyTooLarge);
    }
    Ok(())
}

pub fn check_value(node: &Node, value: &[u8]) -> Result<(), CommandError> {
    if value.len() > node.config.max_value_bytes {
        return Err(CommandError::ValueTooLarge);
    }
    Ok(())
}

/// Make room for a write of about `bytes`. A value larger than the whole budget is refused.
pub fn admit(node: &Node, bytes: usize) -> Result<(), CommandError> {
    match node
        .store
        .reserve(bytes as u64 + discus_store::entry::ENTRY_OVERHEAD)
    {
        discus_store::Admit::Ok => Ok(()),
        discus_store::Admit::TooLarge => Err(CommandError::Custom(
            "OOM value larger than maxmemory".into(),
        )),
    }
}

/// Match a Redis glob pattern: `*`, `?`, `[abc]`, `[a-z]`, `[^a]`, and `\` escapes.
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0, 0);
    let (mut star, mut mark) = (None, 0);
    while t < text.len() {
        let step = match pattern.get(p) {
            Some(b'*') => {
                star = Some(p);
                mark = t;
                p += 1;
                continue;
            }
            Some(b'?') => Some(1),
            Some(b'[') => class_match(&pattern[p..], text[t]),
            Some(b'\\') if p + 1 < pattern.len() => (pattern[p + 1] == text[t]).then_some(2),
            Some(c) => (*c == text[t]).then_some(1),
            None => None,
        };
        match step {
            Some(n) => {
                p += n;
                t += 1;
            }
            None => match star {
                Some(s) => {
                    p = s + 1;
                    mark += 1;
                    t = mark;
                }
                None => return false,
            },
        }
    }
    while pattern.get(p) == Some(&b'*') {
        p += 1;
    }
    p == pattern.len()
}

/// Match one byte against a `[...]` class at the start of `pat`. Returns the class length.
fn class_match(pat: &[u8], c: u8) -> Option<usize> {
    let mut i = 1;
    let negate = pat.get(i) == Some(&b'^');
    if negate {
        i += 1;
    }
    let mut hit = false;
    while i < pat.len() && pat[i] != b']' {
        if pat[i] == b'\\' && i + 1 < pat.len() {
            hit |= pat[i + 1] == c;
            i += 2;
        } else if i + 2 < pat.len() && pat[i + 1] == b'-' && pat[i + 2] != b']' {
            let (lo, hi) = (pat[i].min(pat[i + 2]), pat[i].max(pat[i + 2]));
            hit |= (lo..=hi).contains(&c);
            i += 3;
        } else {
            hit |= pat[i] == c;
            i += 1;
        }
    }
    // `i` is at the closing bracket, or at the end of an unterminated class.
    (hit != negate).then_some(i + 1)
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    #[test]
    fn glob_matches_like_redis() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"user:*", b"user:42"));
        assert!(!glob_match(b"user:*", b"order:42"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[^e]llo", b"hello"));
        assert!(glob_match(b"h[a-b]llo", b"hbllo"));
        assert!(glob_match(b"a\\*b", b"a*b"));
        assert!(!glob_match(b"a\\*b", b"axb"));
        assert!(glob_match(b"*a*b*", b"xxaxxbxx"));
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"", b"x"));
    }
}
