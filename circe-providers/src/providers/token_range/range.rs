use itertools::Itertools;

/// A token range within the Murmur3 hash space.
#[derive(Debug, Clone)]
pub struct TokenRange {
    pub start: i64,
    pub end: i64,
    pub index: usize,
}

impl TokenRange {
    /// Splits the full Murmur3 token space into `num_ranges` sub-ranges.
    ///
    /// Returns a lazy iterator of [`TokenRange`] values covering
    /// `i64::MIN..=i64::MAX`.
    pub fn split(num_ranges: usize) -> impl Iterator<Item = TokenRange> {
        const MIN_TOKEN: i128 = i64::MIN as i128;
        const MAX_TOKEN: i128 = i64::MAX as i128;

        let total_range = (MAX_TOKEN - MIN_TOKEN) as usize;
        let step = total_range / num_ranges;

        (MIN_TOKEN..=MAX_TOKEN)
            .step_by(step)
            .tuple_windows()
            .enumerate()
            .map(|(i, (start, end))| TokenRange {
                start: start.max(MIN_TOKEN) as i64,
                end: end.min(MAX_TOKEN) as i64,
                index: i,
            })
    }
}
