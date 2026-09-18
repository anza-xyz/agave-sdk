#[cfg(target_os = "linux")]
use {
    crate::common::{
        TEST_CONFIG, TEST_EVENT, TestContextBuilder, TestEvent, assert_is_empty, assert_received,
    },
    agave_event_system::{
        stream_name,
        subscriber::{self, AvailableStream},
    },
};
use {
    agave_event_system::stream_policy::{ParseStreamFilterError, StreamPolicy},
    rstest::rstest,
    std::assert_matches,
};

mod common;

#[cfg(target_os = "linux")]
#[test]
fn toggling_stream_policy_for_live_event_system() {
    let test_context = TestContextBuilder::new().build();

    let create_producer = |stream_name| {
        test_context
            .event_system
            .create_stream(stream_name, TEST_CONFIG)
            .unwrap()
            .try_create_producer()
            .unwrap()
    };

    let network_packets_producer = create_producer(stream_name!("network.packets"));
    let network_drops_producer = create_producer(stream_name!("network.drops"));
    let block_production_transaction_producer =
        create_producer(stream_name!("block-production.transaction"));

    let subscriber = subscriber::StreamExplorer::new(test_context.event_system_path());
    let mut available_streams: Vec<AvailableStream> =
        subscriber.available_streams().collect::<Vec<_>>();

    let mut connect = |name: &str| {
        available_streams
            .extract_if(.., |stream| stream.stream_name().as_str() == name)
            .next()
            .expect("stream should exist")
            .try_connect_typed::<TestEvent>()
            .unwrap()
    };

    let mut network_packets_subscriber = connect("network.packets");
    let mut network_drops_subscriber = connect("network.drops");
    let mut block_production_transaction_subscriber = connect("block-production.transaction");

    assert!(
        available_streams.is_empty(),
        "sanity check failed that no other streams were created"
    );

    let mut producers = [
        network_packets_producer,
        network_drops_producer,
        block_production_transaction_producer,
    ];

    // initially all are off due to default policy not being replaced
    let mut emit_event_on_all_producers = move || {
        for producer in producers.iter_mut() {
            producer.emit_event(&TEST_EVENT).unwrap();
        }
    };

    emit_event_on_all_producers();

    assert_is_empty([
        &mut network_packets_subscriber,
        &mut network_drops_subscriber,
        &mut block_production_transaction_subscriber,
    ]);

    // enable all streams
    test_context
        .event_system
        .set_stream_policy("on".parse().unwrap());

    // message sent earlier is not retained in the queues
    assert_is_empty([
        &mut network_packets_subscriber,
        &mut network_drops_subscriber,
        &mut block_production_transaction_subscriber,
    ]);

    emit_event_on_all_producers();

    assert_received([
        &mut network_packets_subscriber,
        &mut network_drops_subscriber,
        &mut block_production_transaction_subscriber,
    ]);

    test_context.event_system.set_stream_policy(
        "off, network.=on, network.packets=off, block-production=on"
            .parse()
            .unwrap(),
    );

    emit_event_on_all_producers();
    assert_is_empty([&mut network_packets_subscriber]);
    assert_received([
        &mut network_drops_subscriber,
        &mut block_production_transaction_subscriber,
    ]);
}

#[rstest]
#[case::white_space(" ")]
#[case::tabbed("\t")]
#[case::empty("")]
#[case::default(StreamPolicy::default())]
fn policies_matches_off(#[case] stream_policy: StreamPolicy) {
    let off_policy: StreamPolicy = "off".parse().unwrap();
    assert_eq!(stream_policy, off_policy);
}

#[test]
fn last_rule_wins() {
    let policy: StreamPolicy = "off, network.=on, on, network.=off".parse().unwrap();
    let expected: StreamPolicy = "on, network.=off".parse().unwrap();

    assert_eq!(policy, expected);
}

#[rstest]
#[case::overlapping_prefixes("network.=on,network.repair=off")]
#[case::case_sensitive_prefixes("Network.=on,network.=off")]
fn accepts_distinct_prefixes(#[case] value: &str) {
    assert_matches!(value.parse::<StreamPolicy>(), Ok(_));
}

#[rstest]
#[case::empty_policy("", "off")]
#[case::only_whitespace(" \t\r\n ", "off")]
#[case::bare_prefix("banking_stage", "banking_stage=on")]
#[case::trimmed_bare_prefix(" \t network. \n", "network.=on")]
#[case::multiple_bare_prefixes("network.,banking_stage", "network.=on,banking_stage=on")]
#[case::shorthand_after_default("off,network.", "off,network.=on")]
#[case::default_after_shorthand("network.,on", "on,network.=on")]
#[case::default_between_shorthands(
    "network.,off,banking_stage",
    "off,network.=on,banking_stage=on"
)]
#[case::shorthand_between_rules(
    "off,network.,network.repair=off",
    "off,network.=on,network.repair=off"
)]
#[case::keyword_prefix("off,off=", "off,off=on")]
#[case::uppercase_keyword_prefix("ON=", "ON=on")]
#[case::bare_prefix_overrides_rule("banking_stage=off,banking_stage", "banking_stage=on")]
#[case::empty_value("banking_stage=", "banking_stage=on")]
#[case::blank_value("banking_stage= \t ", "banking_stage=on")]
#[case::uppercase_default_on("ON", "on")]
#[case::mixed_case_default_on("oN", "on")]
#[case::uppercase_default_off("OFF", "off")]
#[case::mixed_case_default_off("oFf", "off")]
#[case::uppercase("ON,banking_stage=OFF", "on,banking_stage=off")]
#[case::mixed_case("oFf,banking_stage=oN", "off,banking_stage=on")]
#[case::whitespace(
    " \tON\n, banking_stage \t=\nOFF , network. = on ",
    "on,banking_stage=off,network.=on"
)]
#[case::default_after_prefix("network.=on,on", "on,network.=on")]
#[case::default_between_prefixes(
    "network.=on,off,network.repair=off",
    "off,network.=on,network.repair=off"
)]
#[case::reordered_equal_length_prefixes("net=on,rpc=off", "rpc=off,net=on")]
#[case::reordered_case_sensitive_prefixes("net=on,Net=off", "Net=off,net=on")]
#[case::reordered_mixed_length_prefixes(
    "network.repair=off,rpc=off,net=on,network.=on",
    "network.=on,net=on,rpc=off,network.repair=off"
)]
fn normalizes_policy(#[case] policy_format_1: &str, #[case] policy_format_2: &str) {
    assert_ne!(
        policy_format_1, policy_format_2,
        "sanity check failed. these strings can not be identical for the test"
    );

    let policy_1: StreamPolicy = policy_format_1.parse().unwrap();
    let policy_2: StreamPolicy = policy_format_2.parse().unwrap();

    assert_eq!(
        policy_1, policy_2,
        "policies must be the same as `StreamPolicy::parse` should normalize the input strings."
    );
}

#[rstest]
#[case::only_separator(",", "off")]
#[case::only_empty_entries(" , ,\t,\n, ", "off")]
#[case::trailing_separator("on,", "on")]
#[case::leading_separator(",off", "off")]
#[case::consecutive_separators("on,,banking_stage=off", "on,banking_stage=off")]
#[case::blank_directive("on, \t\n ,banking_stage=off", "on,banking_stage=off")]
#[case::trailing_blank_directive("network.=on, \t", "network.=on")]
fn ignores_empty_directives(#[case] value: &str, #[case] expected: &str) {
    assert_eq!(
        value.parse::<StreamPolicy>().unwrap(),
        expected.parse::<StreamPolicy>().unwrap()
    );
}

#[rstest]
#[case::empty_prefix_on("=on", "on")]
#[case::empty_prefix_off("=off", "off")]
#[case::empty_prefix_and_value("=", "on")]
#[case::blank_prefix(" \t =on", "on")]
#[case::blank_prefix_and_value(" \t = \n", "on")]
#[case::mixed_case_rule(" = oFf ", "off")]
fn empty_prefix_sets_default_rule(#[case] directive: &str, #[case] default_rule: &str) {
    for (policy, expected) in [
        (directive.to_string(), default_rule.to_string()),
        (
            format!("on,network.=on,{directive},network.repair=off"),
            format!("{default_rule},network.=on,network.repair=off"),
        ),
    ] {
        assert_eq!(
            policy.parse::<StreamPolicy>().unwrap(),
            expected.parse::<StreamPolicy>().unwrap()
        );
    }
}

#[rstest]
#[case::unknown_value("invalid")]
#[case::unsupported_value("allow")]
#[case::extra_assignment_in_value("on=off")]
fn rejects_invalid_rule_values(
    #[case] invalid_rule_value: &str,
    #[values("network.", "")] prefix: &str,
) {
    for policy_string in [
        format!("{prefix}={invalid_rule_value}"),
        format!("on,network.=on,{prefix}={invalid_rule_value},network.repair=off"),
        format!("{prefix}={invalid_rule_value},{prefix}=on"),
    ] {
        assert_eq!(
            policy_string.parse::<StreamPolicy>(),
            Err(ParseStreamFilterError::InvalidStreamRuleValue(
                invalid_rule_value.to_string()
            ))
        );
    }
}
