#include "timefunctions.h"
const TimePoint epochNull = std::chrono::system_clock::time_point(std::chrono::milliseconds(epochNull_ms));

TimePoint GetBeginOfDay(const TimePoint &tp) {
    const auto tz = std::chrono::current_zone();
    auto local_tp = tz->to_local(tp);
    auto local_midnight = std::chrono::floor<std::chrono::days>(local_tp);
    return tz->to_sys(local_midnight, std::chrono::choose::earliest);
}

TimePoint GetEndOfDay(const TimePoint &tp) {
    auto tz = std::chrono::current_zone();
    auto local_tp = tz->to_local(tp);
    auto local_midnight = std::chrono::floor<std::chrono::days>(local_tp);
    auto next_midnight_local = local_midnight + std::chrono::days(1);
    auto sys_next_midnight = tz->to_sys(next_midnight_local, std::chrono::choose::latest);
    return sys_next_midnight - 1ms;
}

TimePoint GetBeginOfYear(const TimePoint &tp) {
    auto tz = std::chrono::current_zone();
    auto local_tp = std::chrono::zoned_time(tz, tp).get_local_time();
    auto ld = std::chrono::floor<std::chrono::days>(local_tp);
    std::chrono::year_month_day ymd {ld};
    auto year_start = std::chrono::local_days {ymd.year() / 1 / 1};
    return tz->to_sys(year_start, std::chrono::choose::earliest);
}

TimePoint GetEndOfYear(const TimePoint &tp) {
    auto tz = std::chrono::current_zone();
    auto local_tp = std::chrono::zoned_time(tz, tp).get_local_time();
    auto ld = std::chrono::floor<std::chrono::days>(local_tp);
    std::chrono::year_month_day ymd {ld};
    auto next_year_start = std::chrono::local_days {(ymd.year() + std::chrono::years {1}) / 1 / 1};
    auto sys_next_year_start = tz->to_sys(next_year_start, std::chrono::choose::latest);
    return sys_next_year_start - 1ms;
}
