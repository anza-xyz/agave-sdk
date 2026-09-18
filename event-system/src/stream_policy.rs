use {crate::stream_name::StreamName, std::str::FromStr};

const DIRECTIVE_DELIMITER: &str = ",";
const PREFIX_RULE_ASSIGNMENT: &str = "=";

const ON: &str = "on";
const OFF: &str = "off";

/// Configures which streams in an [`EventSystem`](crate::EventSystem) can emit events.
///
/// # Enabling events
///
/// Events are disabled on all streams by default. An empty or whitespace only
/// policy string is equivalent to `off` and [`StreamPolicy::default()`].
///
/// Use [`str::parse`] to create a policy with the following syntax:
///
/// ```text
/// on | off | prefix[=[rule]]
/// ```
///
/// A policy is a comma-separated list of directives in these forms. Brackets
/// mark optional parts. A bare `on` or `off` sets the default rule. Any other
/// bare directive enables events for a stream name prefix. A prefix can also
/// have an explicit rule (`<prefix>=<rule>`). For example:
///
/// ```text
/// off,network.=on,network.repair=off
/// ```
///
/// The **prefix** selects streams whose names start with the given text.
/// For example, `network.=on` enables events for `network.gossip`,
/// `network.repair`, and `network.repair.requests`. Prefixes are matched
/// case-sensitively. An empty prefix sets the default rule, so `=off` means `off`.
///
/// The **rule** controls whether matching streams can emit events:
///
/// - `on` enables events.
/// - `off` disables events.
///
/// Both `=` and the rule are optional: `network.` and `network.=` have the
/// same effect as `network.=on`. This shorthand can appear anywhere in the list.
/// To target a prefix named `on` or `off`, use `on=on` or `off=on`.
///
/// Rule names ignore ASCII case: `off`, `OFF`, and `oFf` have the same effect.
/// The examples below use lowercase names.
///
/// # Rule precedence
///
/// The longest matching prefix determines the rule for a stream. Streams with
/// no matching prefix use the default rule, which is `off` unless a directive
/// changes it. The default directive may appear anywhere in the list.
///
/// If directives with duplicate prefix or default rules are used, the last directive
/// will be used.
///
/// # Examples
///
/// Example policies:
///
/// - `on` enables events on all streams.
/// - `off` disables events on all streams.
/// - `network.=on` enables events on streams starting with `network.`.
/// - `network.` and `network.=` also enable events on streams starting with `network.`.
/// - `on,network.repair=off` enables events except on streams starting with
///   `network.repair`.
///
/// This policy enables `network.gossip`, disables `network.repair` and
/// `network.repair.requests`, and disables streams outside the `network.` prefix:
///
/// ```
/// use agave_event_system::stream_policy::StreamPolicy;
/// # use agave_event_system::stream_policy::ParseStreamFilterError;
///
/// let policy: StreamPolicy = "off,network.=on,network.repair=off".parse()?;
/// # Ok::<(), ParseStreamFilterError>(())
/// ```
///
/// # Disabled streams
///
/// Events emitted by a [`Producer`](crate::producer::Producer) on a disabled
/// stream are dropped. Its [`StreamSubscriber`](crate::subscriber::StreamSubscriber)s
/// do not receive those events, and enabling the stream later does not replay them.
#[derive(Debug, PartialEq, Eq, Default)]
pub struct StreamPolicy {
    /// Rule to apply when no prefix matches the stream name.
    pub(crate) default_rule: StreamRule,
    /// Prefix rules sorted by increasing prefix length, then lexicographically.
    pub(crate) prefix_rules_sorted_by_length: Box<[PrefixStreamRule]>,
}

impl StreamPolicy {
    #[cfg_attr(
        not(target_os = "linux"),
        expect(dead_code, reason = "only used by the linux backend")
    )]
    pub(crate) fn stream_rule(&self, stream_name: &StreamName) -> StreamRule {
        let best_match = self
            .prefix_rules_sorted_by_length
            .iter()
            .rev()
            .find(|PrefixStreamRule { prefix, .. }| stream_name.as_str().starts_with(prefix))
            .map(|PrefixStreamRule { stream_state, .. }| stream_state)
            .copied();

        best_match.unwrap_or(self.default_rule)
    }
}

impl FromStr for StreamPolicy {
    type Err = ParseStreamFilterError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.trim().is_empty() {
            return Ok(Self::default());
        }

        let mut prefix_rules: Vec<PrefixStreamRule> = vec![];
        let mut default_rule = StreamRule::default();

        for directive in input
            .split(DIRECTIVE_DELIMITER)
            // white space around directives are allowed
            .map(str::trim)
        {
            if directive.is_empty() {
                continue;
            }

            if let Ok(parsed_default_rule) = directive.parse::<StreamRule>() {
                default_rule = parsed_default_rule;
                continue;
            }

            let prefix_rule = directive.parse::<PrefixStreamRule>()?;

            // "=off," is the same as the default rule "off"
            let prefix_was_empty = prefix_rule.prefix.is_empty();
            if prefix_was_empty {
                default_rule = prefix_rule.stream_state;
                continue;
            }

            if let Some(previous_duplicate_rule) = prefix_rules
                .iter_mut()
                .find(|rule| rule.prefix == prefix_rule.prefix)
            {
                *previous_duplicate_rule = prefix_rule;
            } else {
                prefix_rules.push(prefix_rule);
            }
        }

        prefix_rules.sort_by(|left, right| {
            left.prefix
                .len()
                .cmp(&right.prefix.len())
                // sort lexicographic as tie breaker so we have same structure
                // for equality checks of `StreamPolicy::eq` implementation.
                .then_with(|| left.prefix.cmp(&right.prefix))
        });

        Ok(Self {
            default_rule,
            prefix_rules_sorted_by_length: prefix_rules.into_boxed_slice(),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PrefixStreamRule {
    prefix: String,
    stream_state: StreamRule,
}

impl FromStr for PrefixStreamRule {
    type Err = ParseStreamFilterError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (prefix, rule_value) = input
            .split_once(PREFIX_RULE_ASSIGNMENT)
            .unwrap_or((input, ""));
        let prefix = prefix.trim();

        let rule_value = rule_value.trim();
        let stream_state = if rule_value.is_empty() {
            StreamRule::On
        } else {
            rule_value.parse()?
        };

        Ok(Self {
            prefix: prefix.to_string(),
            stream_state,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum StreamRule {
    On,
    #[default]
    Off,
}

impl FromStr for StreamRule {
    type Err = ParseStreamFilterError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.eq_ignore_ascii_case(ON) {
            Ok(Self::On)
        } else if value.eq_ignore_ascii_case(OFF) {
            Ok(Self::Off)
        } else {
            Err(ParseStreamFilterError::InvalidStreamRuleValue(
                value.to_string(),
            ))
        }
    }
}

impl std::fmt::Display for StreamRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamRule::On => f.write_str(ON),
            StreamRule::Off => f.write_str(OFF),
        }
    }
}

/// An error encountered while parsing a stream policy.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum ParseStreamFilterError {
    #[error("invalid stream rule value. Must be either {ON} or {OFF}, got {0}")]
    InvalidStreamRuleValue(String),
}
