use {
    agave_event_system::{
        stream_name,
        stream_name::{StreamName, StreamNameValidationError},
    },
    rstest::rstest,
};

#[rstest]
#[case::embedded_nul("stream\0name")]
#[case::nested_path("nested/stream")]
#[case::parent_path("../stream")]
#[case::current_path("./stream")]
#[case::nested_parent_path("stream/../other")]
#[case::parent_backslash("..\\stream")]
#[case::absolute_path("/absolute")]
#[case::trailing_slash("trailing/")]
#[case::double_trailing_slash("trailing//")]
#[case::trailing_dot("trailing/.")]
#[case::space("stream name")]
#[case::tab("stream\tname")]
#[case::newline("stream\nname")]
#[case::policy_separator("stream,other")]
#[case::policy_assignment("stream=on")]
#[case::backslash("stream\\name")]
#[case::underscore_with_slash("stream_/name")]
#[case::emoji("🦀")]
#[case::unicode_character("界")]
fn try_new_rejects_invalid_names(#[case] invalid_name: &'static str) {
    assert_eq!(
        StreamName::try_new(invalid_name),
        Err(StreamNameValidationError::InvalidCharacter),
    );
}

#[rstest]
#[case::empty("", StreamNameValidationError::Empty)]
#[case::current_directory(".", StreamNameValidationError::ReservedDirectoryName)]
#[case::parent_directory("..", StreamNameValidationError::ReservedDirectoryName)]
fn try_new_rejects_invalid_directory_names(
    #[case] invalid_name: &'static str,
    #[case] expected_error: StreamNameValidationError,
) {
    assert_eq!(StreamName::try_new(invalid_name), Err(expected_error));
}

#[rstest]
#[case("stream_a", stream_name!("stream_a"))]
#[case("_", stream_name!("_"))]
#[case("_stream_a_", stream_name!("_stream_a_"))]
#[case("solana_core", stream_name!("solana_core"))]
#[case("solana_core::replay_stage", stream_name!("solana_core::replay_stage"))]
#[case("solana_core::replay_stage::slot_events", stream_name!("solana_core::replay_stage::slot_events"))]
#[case("slot.events:v1", stream_name!("slot.events:v1"))]
#[case(".hidden", stream_name!(".hidden"))]
#[case(".test", stream_name!(".test"))]
#[case("..tests", stream_name!("..tests"))]
#[case("test.stream-events", stream_name!("test.stream-events"))]
#[case("trailing.", stream_name!("trailing."))]
#[case("...", stream_name!("..."))]
fn sucessful_macro_evaluates_to_same_result_as_constructor(
    #[case] name: &'static str,
    #[case] stream_name: StreamName,
) {
    assert_eq!(stream_name.as_str(), name);
    assert_eq!(StreamName::try_new(name), Ok(stream_name));
}
