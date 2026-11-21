//! Fast zero-dependency HTTP date formatting complying with RFC 9110 Section 5.6.7 (IMF-fixdate).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Formats a [`SystemTime`] into an RFC 9110 IMF-fixdate string (e.g. `Sun, 06 Nov 1994 08:49:37 GMT`).
#[must_use]
pub fn format_http_date(time: SystemTime) -> String {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let total_secs = duration.as_secs();

    let secs_of_day = (total_secs % 86400) as u32;
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    // Unix epoch 1970-01-01 was Thursday (index 0 in DAYS)
    let day_of_week = DAYS[((total_secs / 86400) % 7) as usize];

    // Compute calendar date from day count
    let mut days_remaining = total_secs / 86400;
    let mut year = 1970u64;

    loop {
        let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        let days_in_year = if is_leap { 366 } else { 365 };
        if days_remaining >= days_in_year {
            days_remaining -= days_in_year;
            year += 1;
        } else {
            break;
        }
    }

    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let mut month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if is_leap {
        month_days[1] = 29;
    }

    let mut month_index = 0usize;
    for (idx, &days_in_month) in month_days.iter().enumerate() {
        if days_remaining >= days_in_month {
            days_remaining -= days_in_month;
        } else {
            month_index = idx;
            break;
        }
    }

    let day = days_remaining + 1;
    let month = MONTHS[month_index];

    format!("{day_of_week}, {day:02} {month} {year:04} {hour:02}:{minute:02}:{second:02} GMT")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_http_date_epoch() {
        assert_eq!(
            format_http_date(UNIX_EPOCH),
            "Thu, 01 Jan 1970 00:00:00 GMT"
        );
    }

    #[test]
    fn test_format_http_date_known_timestamp() {
        // 784111777 = Sun, 06 Nov 1994 08:49:37 GMT (from RFC 9110 example)
        let time = UNIX_EPOCH + Duration::from_secs(784_111_777);
        assert_eq!(format_http_date(time), "Sun, 06 Nov 1994 08:49:37 GMT");
    }
}
