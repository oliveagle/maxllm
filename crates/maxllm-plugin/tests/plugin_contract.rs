// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Contract tests for the built-in plugins: every plugin must report the
//! name it was configured with, and every plugin category must be
//! constructible through the public factory.

use maxllm_plugin::create_plugin;
use toml::Value;

/// Minimal configuration for each plugin category.
fn config_for(category: &str) -> toml::Table {
    let mut config = toml::Table::new();
    config.insert("category".into(), Value::String(category.into()));
    match category {
        "key_auth" => {
            config.insert("keys".into(), Value::Array(vec![Value::String("k".into())]));
        }
        "rate_limit" => {
            config.insert("requests_per_minute".into(), Value::Integer(60));
        }
        "ip_restriction" => {
            config.insert(
                "ip_list".into(),
                Value::Array(vec![Value::String("10.0.0.0/8".into())]),
            );
        }
        "keyword_block" => {
            config.insert(
                "keywords".into(),
                Value::Array(vec![Value::String("forbidden".into())]),
            );
        }
        "secret_scan" => {
            // `patterns` references built-in pattern *names*.
            config.insert(
                "patterns".into(),
                Value::Array(vec![Value::String("aws_access_key".into())]),
            );
        }
        "regex_guard" => {
            let mut rule = toml::Table::new();
            rule.insert("name".into(), Value::String("r1".into()));
            rule.insert("regex".into(), Value::String(".*secret.*".into()));
            config.insert("rules".into(), Value::Array(vec![Value::Table(rule)]));
        }
        _ => {}
    }
    config
}

const CATEGORIES: &[&str] = &[
    "auto_compaction",
    "cache",
    "cors",
    "ip_restriction",
    "key_auth",
    "keyword_block",
    "max_size",
    "pii_filter",
    "prompt_guard",
    "rate_limit",
    "regex_guard",
    "request_id",
    "secret_scan",
    "webhook",
];

#[test]
fn test_every_plugin_reports_its_configured_name() {
    for category in CATEGORIES {
        let plugin = create_plugin("my-plugin", &config_for(category))
            .unwrap_or_else(|e| panic!("{category} must be constructible: {e}"));
        assert_eq!(plugin.name(), "my-plugin", "category {category}");
    }
}

#[test]
fn test_plugin_names_are_distinct_per_instance() {
    let first = create_plugin("first", &config_for("cache")).unwrap();
    let second = create_plugin("second", &config_for("cache")).unwrap();
    assert_eq!(first.name(), "first");
    assert_eq!(second.name(), "second");
}

#[test]
fn test_create_plugin_rejects_an_unknown_category() {
    let mut config = toml::Table::new();
    config.insert("category".into(), Value::String("nope".into()));
    assert!(create_plugin("x", &config).is_err());
}

#[test]
fn test_create_plugin_requires_a_category() {
    assert!(create_plugin("x", &toml::Table::new()).is_err());
}
