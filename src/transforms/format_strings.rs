// Convert date/time format strings from source dialect conventions to
// DuckDB's strftime/strptime format.
//
// DuckDB uses C-style strftime specifiers (`%Y`, `%m`, `%d`, `%H`, `%M`, `%S`, …).
// Redshift uses PostgreSQL-style tokens (`YYYY`, `MM`, `DD`, `HH24`, `MI`, `SS`, …).
// Trino's `format_datetime` uses Java DateTimeFormatter tokens (`yyyy`, `MM`, `dd`, `HH`, `mm`, `ss`, …).

// ---------------------------------------------------------------------------
// Redshift (PostgreSQL-style) → DuckDB strftime
// ---------------------------------------------------------------------------

/// Convert a Redshift / PostgreSQL format string to DuckDB strftime format.
///
/// Handles common tokens; literal text inside double-quoted segments is preserved.
pub fn redshift_to_strftime(fmt: &str) -> String {
    let mut out = String::with_capacity(fmt.len());
    let chars: Vec<char> = fmt.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Double-quoted literal passthrough
        if chars[i] == '"' {
            i += 1;
            while i < len && chars[i] != '"' {
                out.push(chars[i]);
                i += 1;
            }
            if i < len {
                i += 1; // skip closing quote
            }
            continue;
        }

        if let Some((replacement, consumed)) = match_redshift_token(&chars, i, len) {
            out.push_str(replacement);
            i += consumed;
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

fn match_redshift_token(chars: &[char], i: usize, len: usize) -> Option<(&'static str, usize)> {
    let remaining = len - i;

    // Try longest tokens first to avoid ambiguity

    // 6-char tokens
    if remaining >= 6 {
        let t6: String = chars[i..i + 6].iter().collect();
        let t6_upper = t6.to_uppercase();
        if t6_upper.as_str() == "SSSSSS" {
            return Some(("%f", 6)); // microseconds
        }
    }

    // 5-char tokens
    if remaining >= 5 {
        let t5: String = chars[i..i + 5].iter().collect();
        let t5_upper = t5.to_uppercase();
        match t5_upper.as_str() {
            "MONTH" => return Some(("%B", 5)), // full month name
            "IYYY" | "IYYYY" => {}             // ISO year (handle 4-char below)
            _ => {}
        }
    }

    // 4-char tokens
    if remaining >= 4 {
        let t4: String = chars[i..i + 4].iter().collect();
        let t4_upper = t4.to_uppercase();
        match t4_upper.as_str() {
            "YYYY" => return Some(("%Y", 4)),
            "HH24" => return Some(("%H", 4)),
            "HH12" => return Some(("%I", 4)),
            "IYYY" => return Some(("%G", 4)), // ISO year
            "SSSS" => return Some(("%f", 4)), // fractional seconds (approx)
            _ => {}
        }
    }

    // 3-char tokens
    if remaining >= 3 {
        let t3: String = chars[i..i + 3].iter().collect();
        let t3_upper = t3.to_uppercase();
        match t3_upper.as_str() {
            "MON" => return Some(("%b", 3)), // abbreviated month
            "DAY" => return Some(("%A", 3)), // full weekday name
            "DDD" => return Some(("%j", 3)), // day of year
            _ if t3_upper == "DY " || (remaining == 3 && t3_upper.starts_with("DY")) => {}
            _ => {}
        }
    }

    // 2-char tokens
    if remaining >= 2 {
        let t2: String = chars[i..i + 2].iter().collect();
        let t2_upper = t2.to_uppercase();
        match t2_upper.as_str() {
            "YY" => return Some(("%y", 2)),
            "MM" => return Some(("%m", 2)),
            "DD" => return Some(("%d", 2)),
            "HH" => return Some(("%H", 2)),
            "MI" => return Some(("%M", 2)),
            "SS" => return Some(("%S", 2)),
            "MS" => return Some(("%g", 2)), // milliseconds
            "US" => return Some(("%f", 2)), // microseconds
            "AM" | "PM" => return Some(("%p", 2)),
            "TZ" => return Some(("%Z", 2)),
            "OF" => return Some(("%z", 2)), // UTC offset
            "DY" => return Some(("%a", 2)), // abbreviated weekday
            "WW" => return Some(("%W", 2)), // week of year
            "IW" => return Some(("%V", 2)), // ISO week
            "ID" => return Some(("%u", 2)), // ISO day of week
            "CC" => return Some(("%C", 2)), // century
            _ if t2_upper.starts_with('Q')
                && !t2_upper.chars().nth(1).unwrap_or(' ').is_alphabetic() => {}
            _ => {}
        }
    }

    // Single-char tokens
    let ch_upper = chars[i].to_uppercase().next().unwrap_or(chars[i]);
    match ch_upper {
        'D' if remaining >= 1 && (remaining < 2 || !chars[i + 1].is_alphabetic()) => {
            return Some(("%w", 1)); // day of week
        }
        'J' if remaining >= 1 && (remaining < 2 || !chars[i + 1].is_alphabetic()) => {
            return Some(("%j", 1)); // Julian day (approx)
        }
        _ => {}
    }

    None
}

// ---------------------------------------------------------------------------
// Trino (Java DateTimeFormatter-style) → DuckDB strftime
// ---------------------------------------------------------------------------

/// Convert a Trino / Java DateTimeFormatter format string to DuckDB strftime format.
///
/// Handles common patterns; single-quoted literal segments are preserved.
pub fn trino_to_strftime(fmt: &str) -> String {
    let mut out = String::with_capacity(fmt.len());
    let chars: Vec<char> = fmt.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Single-quoted literal passthrough (Java convention)
        if chars[i] == '\'' {
            i += 1;
            if i < len && chars[i] == '\'' {
                // Escaped single quote ''
                out.push('\'');
                i += 1;
                continue;
            }
            while i < len && chars[i] != '\'' {
                out.push(chars[i]);
                i += 1;
            }
            if i < len {
                i += 1; // skip closing quote
            }
            continue;
        }

        // Count the run length of the same letter
        let ch = chars[i];
        if ch.is_ascii_alphabetic() {
            let mut run = 1;
            while i + run < len && chars[i + run] == ch {
                run += 1;
            }
            let replacement = match_java_token(ch, run);
            out.push_str(replacement);
            i += run;
        } else {
            out.push(ch);
            i += 1;
        }
    }
    out
}

fn match_java_token(ch: char, count: usize) -> &'static str {
    match (ch, count) {
        // Era
        ('G', _) => "",

        // Year
        ('y', 1..=2) => "%y", // 2-digit year
        ('y', _) => "%Y",     // 4-digit year
        ('Y', 1..=2) => "%y", // week-based year (2-digit)
        ('Y', _) => "%G",     // week-based year (ISO)
        ('u', _) => "%Y",     // year (same as y for positive years)

        // Month
        ('M', 1..=2) => "%m", // numeric month
        ('M', 3) => "%b",     // abbreviated month name
        ('M', _) => "%B",     // full month name
        ('L', 1..=2) => "%m", // standalone month number
        ('L', 3) => "%b",
        ('L', _) => "%B",

        // Day
        ('d', _) => "%d", // day of month
        ('D', _) => "%j", // day of year

        // Day of week
        ('E', 1..=3) => "%a", // abbreviated day name
        ('E', _) => "%A",     // full day name
        ('e', _) => "%w",     // day of week (numeric)

        // AM/PM
        ('a', _) => "%p",

        // Hour
        ('H', _) => "%H", // 0-23
        ('k', _) => "%H", // 1-24 (approximate)
        ('h', _) => "%I", // 1-12
        ('K', _) => "%I", // 0-11 (approximate)

        // Minute
        ('m', _) => "%M",

        // Second
        ('s', _) => "%S",

        // Fractional seconds
        ('S', 1..=3) => "%g", // milliseconds
        ('S', _) => "%f",     // microseconds
        ('n', _) => "%f",     // nanoseconds (approx as microseconds)

        // Timezone
        ('z', _) => "%Z", // timezone name
        ('Z', _) => "%z", // UTC offset
        ('X', _) => "%z", // ISO offset
        ('x', _) => "%z", // localized offset
        ('O', _) => "%z", // localized offset

        // Week
        ('w', _) => "%V", // ISO week of year

        // Literal percent (shouldn't happen in Java format, but be safe)
        ('%', _) => "%%",

        // Unknown → empty (skip)
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Redshift → strftime tests
    #[test]
    fn redshift_basic_date() {
        assert_eq!(redshift_to_strftime("YYYY-MM-DD"), "%Y-%m-%d");
    }

    #[test]
    fn redshift_datetime() {
        assert_eq!(
            redshift_to_strftime("YYYY-MM-DD HH24:MI:SS"),
            "%Y-%m-%d %H:%M:%S"
        );
    }

    #[test]
    fn redshift_12hour() {
        assert_eq!(redshift_to_strftime("HH12:MI:SS AM"), "%I:%M:%S %p");
    }

    #[test]
    fn redshift_month_name() {
        assert_eq!(redshift_to_strftime("DD MON YYYY"), "%d %b %Y");
    }

    #[test]
    fn redshift_iso_week() {
        assert_eq!(redshift_to_strftime("IYYY-IW"), "%G-%V");
    }

    #[test]
    fn redshift_microseconds() {
        assert_eq!(
            redshift_to_strftime("YYYY-MM-DD HH24:MI:SS.US"),
            "%Y-%m-%d %H:%M:%S.%f"
        );
    }

    // Trino → strftime tests
    #[test]
    fn trino_basic_date() {
        assert_eq!(trino_to_strftime("yyyy-MM-dd"), "%Y-%m-%d");
    }

    #[test]
    fn trino_datetime() {
        assert_eq!(
            trino_to_strftime("yyyy-MM-dd HH:mm:ss"),
            "%Y-%m-%d %H:%M:%S"
        );
    }

    #[test]
    fn trino_12hour_ampm() {
        assert_eq!(trino_to_strftime("hh:mm:ss a"), "%I:%M:%S %p");
    }

    #[test]
    fn trino_month_name() {
        assert_eq!(trino_to_strftime("dd MMM yyyy"), "%d %b %Y");
    }

    #[test]
    fn trino_full_month() {
        assert_eq!(trino_to_strftime("dd MMMM yyyy"), "%d %B %Y");
    }

    #[test]
    fn trino_quoted_literal() {
        assert_eq!(trino_to_strftime("yyyy'T'HH:mm:ss"), "%YT%H:%M:%S");
    }

    #[test]
    fn trino_milliseconds() {
        assert_eq!(
            trino_to_strftime("yyyy-MM-dd HH:mm:ss.SSS"),
            "%Y-%m-%d %H:%M:%S.%g"
        );
    }

    #[test]
    fn trino_two_digit_year() {
        assert_eq!(trino_to_strftime("yy-MM-dd"), "%y-%m-%d");
    }

    #[test]
    fn trino_iso_week_year() {
        assert_eq!(trino_to_strftime("YYYY-ww"), "%G-%V");
    }
}
