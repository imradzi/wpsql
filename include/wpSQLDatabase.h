#pragma once
#include "sqlite3.h"
#include <type_traits>
#include <fmt/format.h>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <charconv>
#include <stdexcept>
#include <string_view>
#include <codecvt>
#include <locale>
#include <boost/algorithm/string.hpp>
//#include <boost/json.hpp>
#include <boost/locale.hpp>
#include <boost/make_shared.hpp>
#include <boost/tokenizer.hpp>
#include <boost/uuid/nil_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "timefunctions.h"
#include "ulid.hpp"

constexpr auto wpDATEFORMAT = "%d-%m-%Y";
constexpr auto wpDATEFORMATLONG = "%a %d-%b-%Y";
constexpr auto wpDATETIMEFORMAT = "%d-%m-%Y %H:%M:%S";
constexpr auto wpEXPIRYDATEFORMAT = "%Y-%m";

#define attributeMark_delimiter "\t"
#define valueMark_delimiter "\n"

using namespace std::chrono_literals;

typedef std::chrono::system_clock::time_point TimePoint;

template<typename T> std::wstring to_wstring(const T &v) {
    if constexpr (std::is_same_v<T, std::string>)
        return boost::locale::conv::utf_to_utf<wchar_t>(v);
    else if constexpr (std::is_same_v<T, std::wstring>)
        return v;
    else if constexpr (std::is_same_v<T, char *>)
        return std::wstring(v, v + strlen(v));
    else if constexpr (std::is_same_v<T, boost::uuids::uuid>)
        return boost::uuids::to_wstring(v);
    else
        return std::to_wstring(v);
}

template<typename T> std::string to_string(const T &v) {
    if constexpr (std::is_same_v<T, std::string>)
        return v;
    else if constexpr (std::is_same_v<T, std::wstring>)
        return boost::locale::conv::utf_to_utf<char>(v);
    else if constexpr (std::is_same_v<T, const char *>)
        return std::string(v);
    else if constexpr (std::is_same_v<T, char *>)
        return std::string(v);
    else if constexpr (std::is_same_v<T, boost::uuids::uuid>)
        return boost::uuids::to_string(v);
    else
        return fmt::format("{}", v);
}

extern std::string BuildFTSSearch(const std::string &param);

template<class T, class... Args> T from_chars(const std::string &s, Args... args) {
    const char *end = s.begin() + s.size();
    T number;
    auto result = std::from_chars(s.begin(), end, number, args...);
    if (result.ec != std::errc {} || result.ptr != end) throw std::runtime_error("Cannot convert to number");
    return number;
}

inline auto to_number(const TimePoint &t) { return ChronoToEPOCH_ms(t); }

inline auto TimePointMax() { return std::chrono::time_point<std::chrono::system_clock>::max(); }
inline auto TimePointMin() { return std::chrono::time_point<std::chrono::system_clock>::min(); }

inline auto InvalidTimePoint() { return std::chrono::system_clock::time_point::min(); }

inline bool IsInValidTimePoint(const TimePoint &t) { return t == std::chrono::system_clock::time_point::min(); }
inline bool IsValidTimePoint(const TimePoint &t) { return t > std::chrono::system_clock::time_point::min(); }

inline void InvalidateTimePoint(TimePoint &t) { t = std::chrono::system_clock::time_point::min(); }

template<typename T> auto to_number(const std::string &s) {
    if constexpr (std::is_same_v<T, long long>)
        return std::atoll(s.c_str());
    else if constexpr (std::is_same_v<T, long>)
        return std::atol(s.c_str());
    else if constexpr (std::is_same_v<T, int64_t>)
        return std::atoll(s.c_str());
    else if constexpr (std::is_same_v<T, int>)
        return std::atoi(s.c_str());
    else if constexpr (std::is_same_v<T, short>)
        return std::atoi(s.c_str());
    else if constexpr (std::is_same_v<T, double>)
        return std::atof(s.c_str());
    else if constexpr (std::is_same_v<T, TimePoint>)
        return TimePoint(std::chrono::milliseconds(std::atoll(s.c_str())));
    else
        return std::atoi(s.c_str());
}

template<typename T> auto to_number(std::string_view s) {
    if constexpr (std::is_same_v<T, long long>) {
        long long x = 0;
        std::from_chars(s.data(), s.data() + s.length(), x);
        return x;
    } else if constexpr (std::is_same_v<T, long>) {
        long x = 0;
        std::from_chars(s.data(), s.data() + s.length(), x);
        return x;
    } else if constexpr (std::is_same_v<T, int>) {
        int x = 0;
        std::from_chars(s.data(), s.data() + s.length(), x);
        return x;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        int64_t x = 0;
        std::from_chars(s.data(), s.data() + s.length(), x);
        return x;
    } else if constexpr (std::is_same_v<T, short>) {
        short x = 0;
        std::from_chars(s.data(), s.data() + s.length(), x);
        return x;
    } else if constexpr (std::is_same_v<T, double>) {
        double x = 0;
        std::from_chars(s.data(), s.data() + s.length(), x);
        return x;
    } else if constexpr (std::is_same_v<T, TimePoint>) {
        return TimePoint(std::chrono::milliseconds(to_number<long long>(s)));
    } else {
        int x = 0;
        std::from_chars(s.data(), s.data() + s.length(), x);
        return x;
    }
}

template<typename T> auto to_chrono(const T &t) {
    if constexpr (std::is_same_v<T, int64_t>)
        return TimePoint(std::chrono::milliseconds(t));
    else if constexpr (std::is_same_v<T, std::string>)
        return TimePoint(std::chrono::milliseconds(to_number<int64_t>(t)));
    else
        return TimePoint(std::chrono::milliseconds(t));
}

std::string FormatDate(const std::chrono::system_clock::time_point &tp, const char *format = "%F %T %Z");

inline std::string FormatTime(const std::chrono::system_clock::time_point &tp, const char *format = "%H:%M:%S") { return FormatDate(tp, format); }

std::string FormatDateUTC(const std::chrono::system_clock::time_point &tp, const char *format);

extern std::tuple<bool, std::string> json_beautify(const std::string compact_json_string, int indent = 4);

template<typename T> static int to_integer(const T &str) { return str.empty() ? 0 : std::stoi(str); }

template<typename T> static double to_double(const T &str) { return str.empty() ? 0.0 : std::stod(str); }

constexpr int64_t googleTSfactor = 1000000;
inline int64_t get_int_value(const google::protobuf::Timestamp &v) { return v.seconds() * 1000 + v.nanos() / googleTSfactor; }

inline TimePoint get_timepoint_value(const google::protobuf::Timestamp &v) { return EPOCH_msToChrono(get_int_value(v)); }

inline google::protobuf::Timestamp get_ts_value(int64_t val) {
    google::protobuf::Timestamp v;
    v.set_seconds(val / 1000);
    v.set_nanos((val % 1000) * googleTSfactor);
    return v;
}

inline google::protobuf::Timestamp get_ts_value(const TimePoint &t) { return get_ts_value(ChronoToEPOCH_ms(t)); }

const void *GetSQLFunctionParamBlob(sqlite3_value *data, int &len);

struct wpSQLException {
    std::string message;
    int rc;

public:
    wpSQLException(const std::string m, int rc_, sqlite3 *db);
};

class wpSQLManager {
    sqlite3 *db;

public:
    wpSQLManager() : db(NULL) {}
    wpSQLManager(sqlite3 *d) : db(d) {}
    ~wpSQLManager();
    sqlite3 *GetSQLite3() { return db; }
};

class wpSQLStatementManager {
    std::shared_ptr<wpSQLManager> db;
    sqlite3_stmt *stmt;

public:
    wpSQLStatementManager() : stmt(NULL) {}
    wpSQLStatementManager(std::shared_ptr<wpSQLManager> d, sqlite3_stmt *s) : db(d), stmt(s) {}
    ~wpSQLStatementManager() {
        if (stmt) { sqlite3_finalize(stmt); }
        stmt = NULL;
    }
    sqlite3_stmt *GetStatement() { return stmt; }
    sqlite3 *GetSQLite3() { return db->GetSQLite3(); }
};

class wpSQLDatabase;

class wpSQLResultSet {
    std::shared_ptr<wpSQLStatementManager> stmt;
    bool isEOF, isFirst;
    int nCols;
    std::string pSQL;

private:
    wpSQLResultSet();  // - not defined - should not be used;
public:
    wpSQLResultSet(std::shared_ptr<wpSQLStatementManager> s, bool is_eof = false, bool is_first = true);
    bool NextRow();
    std::string GetSQL() { return pSQL; }
    int GetColumnType(int i) const;
    int GetColumnCount() const { return nCols; }
    std::string GetColumnName(int i) const;
    int GetColumnIndex(const std::string &name) const;
    bool IsNull(int i) { return GetColumnType(i) == SQLITE_NULL; }
    const unsigned char *GetBlob(int i, int64_t &len) const {
        if (GetColumnType(i) == SQLITE_NULL) {
            len = 0;
            return NULL;
        }
        len = sqlite3_column_bytes(stmt->GetStatement(), i);
        return (const unsigned char *)sqlite3_column_blob(stmt->GetStatement(), i);
    }
    template<typename T = std::string> T Get(int i, const T &defaultValue = T {}) const {
        if constexpr (std::is_same_v<T, long long>) {
            if (GetColumnType(i) == SQLITE_NULL) return defaultValue;
            return sqlite3_column_int64(stmt->GetStatement(), i);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            if (GetColumnType(i) == SQLITE_NULL) return defaultValue;
            return sqlite3_column_int64(stmt->GetStatement(), i);
        } else if constexpr (std::is_same_v<T, int>) {
            if (GetColumnType(i) == SQLITE_NULL) return defaultValue;
            return sqlite3_column_int(stmt->GetStatement(), i);
        } else if constexpr (std::is_same_v<T, bool>) {
            if (GetColumnType(i) == SQLITE_NULL) return defaultValue;
            return static_cast<bool>(sqlite3_column_int(stmt->GetStatement(), i));
        } else if constexpr (std::is_same_v<T, double>) {
            if (GetColumnType(i) == SQLITE_NULL) return defaultValue;
            return sqlite3_column_double(stmt->GetStatement(), i);
        } else if constexpr (std::is_same_v<T, google::protobuf::Timestamp>) {
            auto value = (GetColumnType(i) == SQLITE_NULL) ? epochNull_ms : sqlite3_column_int64(stmt->GetStatement(), i);
            return get_ts_value(value);
        } else if constexpr (std::is_same_v<T, TimePoint>) {
            auto value = (GetColumnType(i) == SQLITE_NULL) ? epochNull_ms : sqlite3_column_int64(stmt->GetStatement(), i);
            return to_chrono(value);
        } else if constexpr (std::is_same_v<T, ULID>) {
            auto value = (GetColumnType(i) == SQLITE_NULL) ? ULID::empty : ULID(reinterpret_cast<const uint8_t *>(sqlite3_column_blob(stmt->GetStatement(), i)));
            return value;
        } else if constexpr (std::is_same_v<T, boost::uuids::uuid>) {
            if (GetColumnType(i) == SQLITE_NULL) return boost::uuids::nil_uuid();
            int len = sqlite3_column_bytes(stmt->GetStatement(), i);
            const void *val = sqlite3_column_blob(stmt->GetStatement(), i);
            boost::uuids::uuid res {boost::uuids::nil_uuid()};
            if (val && len > 0) memcpy(res.data, val, len);
            return res;
        } else if constexpr (std::is_same_v<T, std::wstring>) {
            if (GetColumnType(i) == SQLITE_NULL) return std::wstring {};
            return boost::locale::conv::utf_to_utf<wchar_t>(reinterpret_cast<const char *>(sqlite3_column_text(stmt->GetStatement(), i)));
        } else if constexpr (std::is_same_v<T, char *>) {
            if (GetColumnType(i) == SQLITE_NULL) return nullptr;
            return reinterpret_cast<char *>(sqlite3_column_text(stmt->GetStatement(), i));
        } else if constexpr (std::is_same_v<T, std::string>) {
            if (GetColumnType(i) == SQLITE_NULL) return defaultValue;
            return boost::locale::conv::utf_to_utf<char>(reinterpret_cast<const char *>(sqlite3_column_text(stmt->GetStatement(), i)));
        }
        return T {};
    }

    bool IsEOF() const { return isEOF; }
};

class wpSQLStatement {
    std::shared_ptr<wpSQLStatementManager> stmt;
    bool needReset;
    std::string pSQL;

private:
    void Reset();
    wpSQLStatement();  // not defined - should not be used.
public:
    wpSQLStatement(std::shared_ptr<wpSQLStatementManager> s) : stmt(s), needReset(false) { pSQL = sqlite3_sql(stmt->GetStatement()); }
    int GetParamCount() const { return sqlite3_bind_parameter_count(stmt->GetStatement()); }
    int GetParamIndex(const std::string &name, bool throwIfError = false) const;
    std::string GetParamName(int i) const { return sqlite3_bind_parameter_name(stmt->GetStatement(), i); }
    std::string GetSQL() { return pSQL; }
    std::shared_ptr<wpSQLResultSet> Execute();
    std::shared_ptr<wpSQLResultSet> ExecuteQuery() { return Execute(); }
    int ExecuteUpdate();

    template<typename T = int64_t> T ExecuteScalar() {
        needReset = true;
        std::shared_ptr<wpSQLResultSet> rs = ExecuteQuery();
        T value {};
        while (rs->NextRow()) value += rs->Get<T>(0);
        return value;
    }

    template<typename T> void Bind(int idx, const T &val) {
        if (needReset) Reset();
        int rc;
        if constexpr (std::is_same_v<T, int64_t>)
            rc = sqlite3_bind_int64(stmt->GetStatement(), idx, val);
        else if constexpr (std::is_same_v<T, long>)
            rc = sqlite3_bind_int(stmt->GetStatement(), idx, val);
        else if constexpr (std::is_same_v<T, long long>)
            rc = sqlite3_bind_int64(stmt->GetStatement(), idx, val);
        else if constexpr (std::is_same_v<T, int>)
            rc = sqlite3_bind_int(stmt->GetStatement(), idx, val);
        else if constexpr (std::is_same_v<T, bool>)
            rc = sqlite3_bind_int(stmt->GetStatement(), idx, (int)val);
        else if constexpr (std::is_same_v<T, double>)
            rc = sqlite3_bind_double(stmt->GetStatement(), idx, val);
        else if constexpr (std::is_same_v<T, TimePoint>) {
            rc = (val == epochNull) ? sqlite3_bind_null(stmt->GetStatement(), idx) : sqlite3_bind_int64(stmt->GetStatement(), idx, to_number(val));
        } else if constexpr (std::is_same_v<T, google::protobuf::Timestamp>) {
            auto iv = get_int_value(val);
            rc = (iv == epochNull_ms) ? sqlite3_bind_null(stmt->GetStatement(), idx) : sqlite3_bind_int64(stmt->GetStatement(), idx, iv);
        } else if constexpr (std::is_same_v<T, boost::uuids::uuid>)
            rc = sqlite3_bind_blob(stmt->GetStatement(), idx, (const void *)val.data, (int)(val.size()), nullptr);
        else if constexpr (std::is_same_v<T, char *>)
            rc = sqlite3_bind_text(stmt->GetStatement(), idx, val, -1, SQLITE_TRANSIENT);  // sqlite_transient - sqlite use internal mem
        else if constexpr (std::is_same_v<T, std::string>)
            rc = sqlite3_bind_text(stmt->GetStatement(), idx, val.c_str(), -1, SQLITE_TRANSIENT);  // sqlite_transient - sqlite use internal mem
        else if constexpr (std::is_same_v<T, ULID>) {
            rc = sqlite3_bind_blob(stmt->GetStatement(), idx, (const void *)val.data(), val.size(), SQLITE_TRANSIENT);
        } else if constexpr (std::is_same_v<T, std::pair<const uint8_t*, size_t>>) {
            rc = sqlite3_bind_blob(stmt->GetStatement(), idx, (const void *)val.first, (int)(val.second), SQLITE_TRANSIENT);
        } else if constexpr (std::is_same_v<T, std::wstring>) {
            auto converted = to_string(val);
            rc = sqlite3_bind_text(stmt->GetStatement(), idx, converted.c_str(), -1, SQLITE_TRANSIENT);  // sqlite_transient - sqlite use internal mem
        } else
            rc = sqlite3_bind_text(stmt->GetStatement(), idx, val, -1, nullptr);  // nullptr -> sqlite won't copy/delete mem
        if (rc != SQLITE_OK) throw wpSQLException("cannot bind: ", rc, stmt->GetSQLite3());
    }

    template<typename T> void Bind(const std::string &paramName, const T &v) { Bind(GetParamIndex(paramName, true), v); }

    void Bind(int idx, const unsigned char *val, int len) {
        if (needReset) Reset();
        int rc = sqlite3_bind_blob(stmt->GetStatement(), idx, (const void *)val, len, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) throw wpSQLException("cannot bind blob: ", rc, stmt->GetSQLite3());
    }
    void Bind(const std::string &paramName, const unsigned char *blob, int len) { Bind(GetParamIndex(paramName, true), blob, len); }
    void BindNull(int idx) {
        if (needReset) Reset();
        int rc = sqlite3_bind_null(stmt->GetStatement(), idx);
        if (rc != SQLITE_OK) throw wpSQLException("cannot bind null: ", rc, stmt->GetSQLite3());
    }
    void BindNull(const std::string &paramName) { BindNull(GetParamIndex(paramName, true)); }
};

class wpAutoCommitter {
    bool isAuto;
    bool toRollBack {true};
    wpSQLDatabase *db;

public:
    wpAutoCommitter(wpSQLDatabase *a);
    void SetOK() { toRollBack = false; }
    virtual ~wpAutoCommitter();
};

enum OpenMode { ReadOnly, ReadWrite };

class wpSQLDatabase {
    std::shared_ptr<wpSQLManager> db;

    static int callback(void *fnLambda, int argc, char **argv, char **azColName);
    static void register_function(sqlite3_context *, int argc, sqlite3_value **data);
    sqlite3_stmt *Prepare(const std::string &sql);

public:
    wpSQLDatabase() {}
    bool IsOpen() { return db && GetDB() != NULL; }
    void Interrupt() {
        if (IsOpen()) sqlite3_interrupt(GetDB());
    }
    bool IsInTransaction() { return !IsAutoCommit(); }
    template<typename T = int64_t> T GetLastRowId() {
        if constexpr (std::is_same_v<T, std::string>)
            return IsOpen() ? std::to_string(sqlite3_last_insert_rowid(GetDB())) : "0";
        else
            return IsOpen() ? sqlite3_last_insert_rowid(GetDB()) : 0;
    }
    bool Begin();
    void Commit();
    bool IsAutoCommit();
    void Rollback(const std::string &checkpoint = "");
    void Vacuum() { Execute("vacuum", NULL); }
    std::unique_ptr<wpAutoCommitter> GetAutoCommitter() { return std::make_unique<wpAutoCommitter>(this); }
    std::shared_ptr<wpSQLStatement> PrepareStatement(const std::string &sql);
    std::vector<std::string> GetAllActivePreparedStatement();
    void FinalizeAllStatements();
    int GetNumberOfActivePreparedStatement();
    bool HasUniqueKey(const std::string &tabName);
    bool HasPrimaryKey(const std::string &tabName);

    template<typename T = int64_t> T ExecuteScalar(const std::string &sql) {
        T v {};
        Execute(sql, [&v](int /*argc*/, char **val, char **) {
            if (val && val[0]) { v += to_number<T>(std::string_view(val[0])); }
        });
        return v;
    }

    int Execute(const std::string &sql, std::function<void(int, char **, char **)> fn);
    std::shared_ptr<wpSQLResultSet> Execute(const std::string &sql);

    int ExecuteUpdate(const std::string &sql) { return Execute(sql, nullptr); }
    std::shared_ptr<wpSQLResultSet> ExecuteQuery(const std::string &sql) { return Execute(sql); }

    void Open(const std::string &fileName, OpenMode mode = OpenMode::ReadWrite);  // 1 minute
    sqlite3 *GetDB() { return db ? db->GetSQLite3() : NULL; }
    void Close() { db.reset(); }

    bool TableExists(const std::string &tableName, const std::string &databaseName = "");
    void CreateFunction(const std::string &functionName, int nArg, void (*fn)(sqlite3_context *ctx, int argc, sqlite3_value **data), void *data = nullptr, bool isDeterministic = true);
    int BackupTo(const std::string &backupName, std::function<bool()> fnIsStopping, std::function<void(int, int)> fnProgressFeedback = nullptr, int nPagesPerCall = 100, int msSleepPerCall = 250);
};
