//! Redis-style glob matching for `SCAN MATCH` and `CONFIG GET`.

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
