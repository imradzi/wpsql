#pragma once

#include <google/protobuf/timestamp.pb.h>
#include <chrono>
using namespace std::chrono_literals;

using TimePoint = std::chrono::system_clock::time_point;

/*
date::year_month_day _ymd = date::year{2023}/11/9; // November 9, 2023
std::chrono::system_clock::time_point tp = std::chrono::sys_days{_ymd};
*/

// eg: GetYMD(std::chrono::year{2023}/11/9);
inline TimePoint GetYMD(const std::chrono::year_month_day &ymd) { return std::chrono::sys_days(ymd); }

constexpr int64_t milli2nano_factor {1000000};

constexpr int64_t epochNull_ms {-62167219200000}; // 0000-01-01 00:00:00 UTC
extern const TimePoint epochNull;

inline int64_t ChronoToEPOCH(const TimePoint &t) {
    return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
}

inline auto EPOCHtoChrono(int64_t t) {
    std::chrono::system_clock::time_point tp {};  // zero
    tp += std::chrono::seconds(t);
    return tp;
}

constexpr auto timezoneHr = 8h;  // hard coded since cannot find timezonehour.

inline auto ChronoToEPOCH_ms(const TimePoint &t) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}

inline auto EPOCH_msToChrono(int64_t t) {
    std::chrono::system_clock::time_point tp {};  // zero
    tp += std::chrono::milliseconds(t);
    return tp;
}


inline int64_t GetTimestamp(const google::protobuf::Timestamp& v) { // returns sinceepoc in ms
    return  v.seconds() * 1000 + v.nanos() / milli2nano_factor;  // millisecs
}

inline TimePoint GetTimePoint(const google::protobuf::Timestamp &v) {
    return EPOCH_msToChrono(GetTimestamp(v));
}

inline void SetTimestamp(google::protobuf::Timestamp *v, int64_t epochTime) {
    v->set_seconds(epochTime / 1000);
    v->set_nanos((epochTime % 1000) * milli2nano_factor);
}

inline void SetTimestamp(google::protobuf::Timestamp *v, const TimePoint &timePoint) {
    SetTimestamp(v, ChronoToEPOCH_ms(timePoint));
}

TimePoint GetBeginOfDay(const TimePoint &t);
TimePoint GetBeginOfYear(const TimePoint &t);
TimePoint GetEndOfDay(const TimePoint &t);
TimePoint GetEndOfYear(const TimePoint &t);

inline void SetNullTimestamp(google::protobuf::Timestamp *v) { SetTimestamp(v, epochNull_ms); }
inline bool IsNullTimestamp(const google::protobuf::Timestamp &v) { return GetTimestamp(v) == epochNull_ms; }
