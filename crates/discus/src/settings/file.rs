//! The versioned TOML settings file. It holds reloadable settings only.

use std::collections::BTreeMap;

pub const NEWEST_VERSION: u32 = 1;

pub struct Key {
    pub section: Option<&'static str>,
    pub key: &'static str,
    /// The clap argument ID of the flag that this key replaces.
    pub flag: &'static str,
    pub kind: Kind,
}

/// The TOML type that a key takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Integer,
    Boolean,
}

const fn integer(section: &'static str, key: &'static str, flag: &'static str) -> Key {
    Key {
        section: Some(section),
        key,
        flag,
        kind: Kind::Integer,
    }
}

const fn boolean(section: &'static str, key: &'static str, flag: &'static str) -> Key {
    Key {
        section: Some(section),
        key,
        flag,
        kind: Kind::Boolean,
    }
}

const fn top_level_integer(key: &'static str, flag: &'static str) -> Key {
    Key {
        section: None,
        key,
        flag,
        kind: Kind::Integer,
    }
}

pub const KEYS: &[Key] = &[
    integer("budget", "forward-read-ms", "forward_read_timeout_ms"),
    integer("budget", "forward-write-ms", "forward_write_timeout_ms"),
    integer("budget", "hold-ms", "hold_budget_ms"),
    integer("budget", "replication-ms", "replication_timeout_ms"),
    boolean("reads", "from-replicas", "read_from_replicas"),
    integer("handoff", "rate-bytes", "handoff_rate_bytes"),
    integer(
        "auth",
        "max-failures-per-connection",
        "auth_max_failures_per_connection",
    ),
    integer(
        "auth",
        "max-failures-per-source",
        "auth_max_failures_per_source",
    ),
    integer("auth", "failure-window-secs", "auth_failure_window_secs"),
    integer("tls", "reload-secs", "tls_reload_secs"),
    top_level_integer("databases", "databases"),
];

#[derive(Debug)]
pub struct FileSettings {
    pub version: u32,
    /// Values stay as TOML so that clap can parse them like command-line text.
    pub values: BTreeMap<&'static str, toml::Value>,
}

/// `restart_only` answers whether a kebab-case name is the long name of a flag that the file cannot set.
pub fn parse(text: &str, restart_only: &dyn Fn(&str) -> bool) -> Result<FileSettings, String> {
    let table: toml::Table = text.parse().map_err(|e| format!("invalid TOML: {e}"))?;

    let version = match table.get("version") {
        None => return Err("`version` is missing; it is required".to_string()),
        Some(toml::Value::Integer(v)) => *v,
        Some(_) => return Err("`version` must be an integer".to_string()),
    };
    if version < 1 {
        return Err(format!(
            "version {version} is not valid; the newest version is {NEWEST_VERSION}"
        ));
    }
    if version > i64::from(NEWEST_VERSION) {
        return Err(format!(
            "version {version} is not supported; the newest version is {NEWEST_VERSION}"
        ));
    }

    let mut values = BTreeMap::new();
    for (name, value) in &table {
        if name == "version" {
            continue;
        }
        if KEYS.iter().any(|k| k.section == Some(name.as_str())) {
            let toml::Value::Table(section) = value else {
                return Err(format!("`{name}` must be a table"));
            };
            for (inner, value) in section {
                let known = KEYS
                    .iter()
                    .find(|k| k.section == Some(name.as_str()) && k.key == inner);
                match known {
                    Some(k) => {
                        values.insert(k.flag, value.clone());
                    }
                    None => {
                        return Err(unknown_or_restart(
                            &format!("{name}.{inner}"),
                            inner,
                            restart_only,
                        ))
                    }
                }
            }
        } else if let Some(k) = KEYS.iter().find(|k| k.section.is_none() && k.key == name) {
            values.insert(k.flag, value.clone());
        } else {
            return Err(unknown_or_restart(name, name, restart_only));
        }
    }

    Ok(FileSettings {
        version: version as u32,
        values,
    })
}

fn unknown_or_restart(path: &str, name: &str, restart_only: &dyn Fn(&str) -> bool) -> String {
    if restart_only(name) {
        return format!("{name} needs a restart: pass it as --{name}");
    }
    // Clap derives a long name from the field name, so the flag ID with dashes is the long name.
    let same_flag = KEYS.iter().find(|k| k.flag.replace('_', "-") == name);
    match same_flag {
        Some(Key {
            section: Some(section),
            key,
            ..
        }) => format!("unknown setting `{path}`; use `[{section}] {key}` for --{name}"),
        Some(Key { key, .. }) => format!("unknown setting `{path}`; use `{key}` for --{name}"),
        None => format!("unknown setting `{path}`"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn restart_only(name: &str) -> bool {
        name == "replication-factor"
    }

    fn parse_text(text: &str) -> Result<FileSettings, String> {
        parse(text, &restart_only)
    }

    #[test]
    fn a_file_without_version_is_an_error() {
        let error = parse_text("[budget]\nhold-ms = 1").unwrap_err();
        assert!(error.contains("version"), "{error}");
    }

    #[test]
    fn a_newer_version_names_the_newest_known() {
        let error = parse_text("version = 2").unwrap_err();
        assert!(error.contains('2'), "{error}");
        assert!(error.contains("newest version is 1"), "{error}");
    }

    #[test]
    fn a_section_key_maps_to_its_flag() {
        let settings = parse_text("version = 1\n[budget]\nhold-ms = 1500").unwrap();
        assert_eq!(settings.version, 1);
        assert_eq!(settings.values.len(), 1);
        assert_eq!(
            settings.values["hold_budget_ms"],
            toml::Value::Integer(1500)
        );
    }

    #[test]
    fn a_restart_only_key_names_the_flag() {
        let error = parse_text("version = 1\nreplication-factor = 2").unwrap_err();
        assert_eq!(
            error,
            "replication-factor needs a restart: pass it as --replication-factor"
        );
    }

    #[test]
    fn an_unknown_key_is_an_error() {
        let error = parse_text("version = 1\n[budget]\nhold = 1").unwrap_err();
        assert!(error.contains("budget.hold"), "{error}");
        assert!(error.contains("unknown"), "{error}");
    }

    #[test]
    fn an_unknown_section_is_an_error() {
        let error = parse_text("version = 1\n[nope]\nx = 1").unwrap_err();
        assert!(error.contains("nope"), "{error}");
    }

    #[test]
    fn databases_is_a_top_level_key() {
        let settings = parse_text("version = 1\ndatabases = 3").unwrap();
        assert_eq!(settings.values["databases"], toml::Value::Integer(3));
    }

    #[test]
    fn the_key_table_matches_the_spec() {
        let expected = [
            (Some("budget"), "forward-read-ms"),
            (Some("budget"), "forward-write-ms"),
            (Some("budget"), "hold-ms"),
            (Some("budget"), "replication-ms"),
            (Some("reads"), "from-replicas"),
            (Some("handoff"), "rate-bytes"),
            (Some("auth"), "max-failures-per-connection"),
            (Some("auth"), "max-failures-per-source"),
            (Some("auth"), "failure-window-secs"),
            (Some("tls"), "reload-secs"),
            (None, "databases"),
        ];
        assert_eq!(KEYS.len(), 11);
        for (key, (section, name)) in KEYS.iter().zip(expected) {
            assert_eq!((key.section, key.key), (section, name));
        }
    }
}
