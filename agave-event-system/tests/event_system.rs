use {agave_event_system::EventSystem, std::assert_matches, tempfile::TempDir};

#[test]
fn create_fails_when_directory_is_retried() {
    let tmp_dir = TempDir::new().unwrap();
    let event_system_directory = tmp_dir.path().join("event-system");

    let _event_system = EventSystem::create(event_system_directory.clone()).unwrap();
    let create_result_with_reused_path = EventSystem::create(event_system_directory);

    assert_matches!(create_result_with_reused_path, Err(_));
}
