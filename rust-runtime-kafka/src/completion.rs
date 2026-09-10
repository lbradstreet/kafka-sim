use std::error::Error;
use std::fmt;

/// How certain a failed side-effecting operation is to have taken effect.
///
/// Certainty describes the operation's documented effect, not merely whether
/// the caller received a successful response.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum CompletionCertainty {
    /// The operation's effect definitely did not occur.
    NotApplied,
    /// The operation's effect occurred even though the operation returned an
    /// error.
    Applied,
    /// The operation may or may not have taken effect.
    ///
    /// Callers must reconcile the result or retry through an idempotent
    /// operation rather than assuming either outcome.
    MayHaveApplied,
}

impl fmt::Display for CompletionCertainty {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotApplied => formatter.write_str("not applied"),
            Self::Applied => formatter.write_str("applied"),
            Self::MayHaveApplied => formatter.write_str("may have applied"),
        }
    }
}

/// An operation error paired with the certainty of its side effect.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CompletionError<E> {
    certainty: CompletionCertainty,
    error: E,
}

impl<E> CompletionError<E> {
    /// Creates an error with an explicit completion certainty.
    #[must_use]
    pub const fn new(certainty: CompletionCertainty, error: E) -> Self {
        Self { certainty, error }
    }

    /// Creates an error for an operation whose effect definitely did not occur.
    #[must_use]
    pub const fn not_applied(error: E) -> Self {
        Self::new(CompletionCertainty::NotApplied, error)
    }

    /// Creates an error for an operation whose effect occurred.
    #[must_use]
    pub const fn applied(error: E) -> Self {
        Self::new(CompletionCertainty::Applied, error)
    }

    /// Creates an error for an operation whose effect may have occurred.
    #[must_use]
    pub const fn may_have_applied(error: E) -> Self {
        Self::new(CompletionCertainty::MayHaveApplied, error)
    }

    /// Returns the certainty of the operation's effect.
    #[must_use]
    pub const fn certainty(&self) -> CompletionCertainty {
        self.certainty
    }

    /// Returns the underlying operation error.
    #[must_use]
    pub const fn error(&self) -> &E {
        &self.error
    }

    /// Separates the completion certainty from the underlying operation error.
    #[must_use]
    pub fn into_parts(self) -> (CompletionCertainty, E) {
        (self.certainty, self.error)
    }

    /// Transforms the underlying error while preserving completion certainty.
    #[must_use]
    pub fn map<F, U>(self, map_error: F) -> CompletionError<U>
    where
        F: FnOnce(E) -> U,
    {
        CompletionError::new(self.certainty, map_error(self.error))
    }
}

impl<E> fmt::Display for CompletionError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} (completion certainty: {})",
            self.error, self.certainty
        )
    }
}

impl<E> Error for CompletionError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

/// The result of an operation whose failures report completion certainty.
pub type CompletionResult<T, E> = Result<T, CompletionError<E>>;

#[cfg(test)]
mod tests {
    use super::{CompletionCertainty, CompletionError, CompletionResult};
    use std::error::Error;
    use std::fmt;

    #[derive(Debug, Eq, PartialEq)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl Error for TestError {}

    #[test]
    fn constructors_set_certainty_and_error() {
        let cases = [
            (
                CompletionError::not_applied("before effect"),
                CompletionCertainty::NotApplied,
                "before effect",
            ),
            (
                CompletionError::applied("after effect"),
                CompletionCertainty::Applied,
                "after effect",
            ),
            (
                CompletionError::may_have_applied("lost completion"),
                CompletionCertainty::MayHaveApplied,
                "lost completion",
            ),
        ];

        for (error, certainty, expected_error) in cases {
            assert_eq!(error.certainty(), certainty);
            assert_eq!(error.error(), &expected_error);
        }
    }

    #[test]
    fn new_accepts_an_explicit_certainty() {
        let error = CompletionError::new(CompletionCertainty::Applied, 17);

        assert_eq!(error.certainty(), CompletionCertainty::Applied);
        assert_eq!(error.error(), &17);
    }

    #[test]
    fn into_parts_returns_both_values() {
        let error = CompletionError::may_have_applied(String::from("timeout"));

        assert_eq!(
            error.into_parts(),
            (CompletionCertainty::MayHaveApplied, String::from("timeout"))
        );
    }

    #[test]
    fn map_changes_only_the_underlying_error() {
        let error = CompletionError::not_applied(String::from("full"));
        let mapped = error.map(|message| message.len());

        assert_eq!(mapped.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(mapped.error(), &4);
    }

    #[test]
    fn certainty_display_is_stable_and_readable() {
        assert_eq!(CompletionCertainty::NotApplied.to_string(), "not applied");
        assert_eq!(CompletionCertainty::Applied.to_string(), "applied");
        assert_eq!(
            CompletionCertainty::MayHaveApplied.to_string(),
            "may have applied"
        );
    }

    #[test]
    fn error_display_includes_source_and_certainty() {
        let error = CompletionError::may_have_applied(TestError("connection lost"));

        assert_eq!(
            error.to_string(),
            "connection lost (completion certainty: may have applied)"
        );
    }

    #[test]
    fn error_exposes_the_underlying_source() {
        let error = CompletionError::applied(TestError("response lost"));
        let source = Error::source(&error).expect("completion error has a source");

        assert_eq!(source.to_string(), "response lost");
        assert_eq!(
            source.downcast_ref::<TestError>(),
            Some(&TestError("response lost"))
        );
    }

    #[test]
    fn result_alias_accepts_success_and_completion_error() {
        let success: CompletionResult<u8, TestError> = Ok(3);
        let failure: CompletionResult<u8, TestError> =
            Err(CompletionError::not_applied(TestError("unavailable")));

        assert_eq!(success, Ok(3));
        assert_eq!(
            failure,
            Err(CompletionError::new(
                CompletionCertainty::NotApplied,
                TestError("unavailable")
            ))
        );
    }
}
