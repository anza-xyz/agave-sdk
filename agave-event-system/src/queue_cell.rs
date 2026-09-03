use wincode_dynamic::SerializedSize;

// The function is hidden from docs as it's only intended for macro expansion.
#[doc(hidden)]
pub const fn event_queue_cell_size(
    size: SerializedSize,
    max_serialized_size: Option<usize>,
) -> usize {
    match size {
        SerializedSize::Static(size) => size,
        SerializedSize::Dynamic(lower_bound) => match max_serialized_size {
            Some(max_serialized_size) if max_serialized_size > lower_bound => max_serialized_size,
            Some(_) => {
                panic!(
                    "`max_serialized_size` must be greater than wincode's dynamic serialized-size lower bound"
                )
            }
            None => {
                panic!(
                    "event has a dynamic serialized size; specify `max_serialized_size` in the `#[event(...)]` attribute"
                )
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use {super::event_queue_cell_size, wincode_dynamic::SerializedSize};

    #[test]
    fn static_size_uses_wincode_upper_bound() {
        assert_eq!(event_queue_cell_size(SerializedSize::Static(8), None), 8);
        assert_eq!(
            event_queue_cell_size(SerializedSize::Static(8), Some(16)),
            8
        );
    }

    #[test]
    fn dynamic_size_uses_explicit_upper_bound() {
        assert_eq!(
            event_queue_cell_size(SerializedSize::Dynamic(8), Some(16)),
            16
        );
    }

    #[test]
    #[should_panic(expected = "event has a dynamic serialized size")]
    fn dynamic_size_requires_explicit_upper_bound() {
        event_queue_cell_size(SerializedSize::Dynamic(8), None);
    }

    #[test]
    #[should_panic(expected = "must be greater than")]
    fn dynamic_size_rejects_bound_equal_to_lower_bound() {
        event_queue_cell_size(SerializedSize::Dynamic(8), Some(8));
    }

    #[test]
    #[should_panic(expected = "must be greater than")]
    fn dynamic_size_rejects_bound_below_lower_bound() {
        event_queue_cell_size(SerializedSize::Dynamic(8), Some(7));
    }
}
