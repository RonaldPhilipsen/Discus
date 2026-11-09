//! Certificate identities and identity patterns.

use x509_parser::extensions::GeneralName;

/// The identity of a certificate: the first URI SAN, else the first DNS SAN, else the subject
/// CN.
pub fn cert_identity(der: &[u8]) -> Option<String> {
    let (_, cert) = x509_parser::parse_x509_certificate(der).ok()?;
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        let names = &san.value.general_names;
        let uri = names.iter().find_map(|n| match n {
            GeneralName::URI(u) => Some(u.to_string()),
            _ => None,
        });
        let dns = || {
            names.iter().find_map(|n| match n {
                GeneralName::DNSName(d) => Some(d.to_string()),
                _ => None,
            })
        };
        if let Some(name) = uri.or_else(dns) {
            return Some(name);
        }
    }
    let cn = cert.subject().iter_common_name().next()?;
    cn.as_str().ok().map(str::to_string)
}

/// Match `value` against `pattern`. A trailing `*` matches any suffix. Otherwise the match is
/// exact.
pub fn pattern_matches(pattern: &str, value: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => value.starts_with(prefix),
        None => pattern == value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns_match_exactly_or_by_prefix() {
        assert!(pattern_matches("spiffe://a/b", "spiffe://a/b"));
        assert!(!pattern_matches("spiffe://a/b", "spiffe://a/bc"));
        assert!(pattern_matches("spiffe://a/*", "spiffe://a/bc"));
        assert!(pattern_matches("*", "anything"));
    }

    #[test]
    fn the_uri_san_wins_over_dns_and_cn() {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut p = rcgen::CertificateParams::new(vec!["host.test".to_string()]).unwrap();
        p.distinguished_name
            .push(rcgen::DnType::CommonName, "the-cn");
        let dns_only = p.self_signed(&key).unwrap();
        assert_eq!(cert_identity(dns_only.der()).as_deref(), Some("host.test"));
        p.subject_alt_names
            .push(rcgen::SanType::URI("spiffe://x/y".try_into().unwrap()));
        let with_uri = p.self_signed(&key).unwrap();
        assert_eq!(
            cert_identity(with_uri.der()).as_deref(),
            Some("spiffe://x/y")
        );
        p.subject_alt_names.clear();
        let cn_only = p.self_signed(&key).unwrap();
        assert_eq!(cert_identity(cn_only.der()).as_deref(), Some("the-cn"));
    }
}
