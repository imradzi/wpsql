#include <thread>
#include <fmt/format.h>

#include <boost/tokenizer.hpp>
#include <unordered_set>
#include <map>
#include <filesystem>
#include <iostream>
#include <fstream>
#include "rDb.h"
#include "logging.hpp"

namespace fs = std::filesystem;
using namespace std::chrono_literals;

std::vector<DB::DBObjects> DB::SQLiteBase::objectList() const {
    return std::vector<DB::DBObjects> {
        {"ul_keys",
            DB::Table,
            {
                "create table <TABLENAME>("
                "  id integer primary key, "
                "  key text, "
                "  value text, "
                "  description text, "
                "  isDeleted integer, "
                "  unique(key)"
                ")",
                "create index idx_<TABLENAME>_key on <TABLENAME>(key)",
            }},

        {"UL_LocalKeys",
            DB::Table,
            {
                "create table <TABLENAME>("
                "  id integer primary key, "
                "  key text, "
                "  value text, "
                "  description text, "
                "  isDeleted integer, "
                "  unique(key) "
                ")",
                "create index idx_<TABLENAME>_key on <TABLENAME>(key)",
            }},

        {"Types",
            DB::Table,
            {
                "create table <TABLENAME>("
                "  id integer primary key, "
                "  parentID integer, "
                "  code text, "
                "  name text, "
                "  limitvalue text, "
                "  defaultvalue text, "
                "  isDeleted integer, "
                "  foreign key(parentID) references types(id)"
                ")",
            }},
    };
}

std::shared_ptr<DB::UserDBRegistry> DB::SQLiteBase::GetRegistry() {
    if (!userDBregistry)
        userDBregistry = std::make_shared<DB::UserDBRegistry>(this);
    return userDBregistry;
}

bool DB::SQLiteBase::IsDataBaseExist(const std::string &dbName, bool toCreate) {
    bool doExist = fs::exists(dbName);
    if (doExist && toCreate) {
        db->Close();
        if (!fs::remove(dbName)) return false;
    }
    return doExist;
}

bool DB::SQLiteBase::Create() {
    return !IsDataBaseExist(_dbName, true);
}

DB::SQLiteBase::SQLiteBase(const std::string &dbName, bool _turnOffSync, bool _isExclusive, bool _usingWAL, bool journal)
  : getSequence(0LL, ""),
    turnOffSynchronize(_turnOffSync),
    exclusiveMode(_isExclusive),
    usingWAL(_usingWAL),
    journalOff(journal),
    _dbName(dbName),
    isAggregatedDB(false),
    db(new wpSQLDatabase) {
    _dropAllObjects = false;
    if (!boost::iequals(dbName, ":memory:")) {
        fs::path path(_dbName);
        if (path.is_relative()) {
            _dbName = fmt::format("{}{}{}", fs::current_path().string(), std::string(1, fs::path::preferred_separator), _dbName);
            path = fs::path(_dbName);
        }
        _pathName = fmt::format("{}{}", fs::path(_dbName).parent_path().string(), std::string(1, fs::path::preferred_separator));
        fs::create_directories(path.parent_path());
    } else {
        _pathName = _dbName;
        mainDBname = _dbName;
    }
}

DB::SQLiteBase::SQLiteBase() : getSequence(0LL, ""),
                               turnOffSynchronize(false),
                               exclusiveMode(false),
                               journalOff(false),
                               isAggregatedDB(false),
                               db(new wpSQLDatabase) {
    _dropAllObjects = false;
}

bool DB::SQLiteBase::IsTriggerExist(wpSQLDatabase &, const std::string &) { return false; }

bool DB::SQLiteBase::IsTableExist(wpSQLDatabase &d, const std::string &fnName) {
    bool found = d.TableExists(fnName);
    if (found) return true;
    d.Execute("pragma database_list", [&](int, char **data, char **) {
        found = found || d.TableExists(fnName, data[1]);
    });
    return found;
}

bool DB::SQLiteBase::IsViewExist(wpSQLDatabase &d, const std::string &viewName) {
    std::shared_ptr<wpSQLStatement> stmt = d.PrepareStatement("select count(*) from sqlite_master where type='view' and name like ?");
    stmt->Bind(1, viewName);
    std::shared_ptr<wpSQLResultSet> rs = stmt->ExecuteQuery();
    if (rs->NextRow())
        return rs->Get<bool>(0);
    return false;
}

bool DB::SQLiteBase::IsTempTableExist(wpSQLDatabase &d, const std::string &tabName) {
    std::shared_ptr<wpSQLStatement> stt = d.PrepareStatement("select * from sqlite_temp_master where name = ? and type='table'");
    stt->Bind(1, tabName);
    std::shared_ptr<wpSQLResultSet> rs = stt->ExecuteQuery();
    if (rs->NextRow()) return true;
    return false;
}

bool DB::SQLiteBase::IsIndexExist(wpSQLDatabase &d, const std::string &fnName) {
    std::shared_ptr<wpSQLResultSet> rs = d.ExecuteQuery(fmt::format("pragma index_info({})", fnName));
    if (rs->NextRow()) return true;
    return false;
}

/*Output columns from the index_list pragma are as follows :

1.  A sequence number assigned to each index for internal tracking purposes.
2.  The name of the index.
3.  "1" if the index is UNIQUE and "0" if not.
4.  "c" if the index was created by a CREATE INDEX statement, "u" if the index was created by a UNIQUE constraint, or "pk" if the index was created by a PRIMARY KEY constraint.
5.  "1" if the index is a partial index and "0" if not.
*/

bool DB::SQLiteBase::IsPrimaryKeyExist(wpSQLDatabase &d, const std::string &fnName) {
    std::shared_ptr<wpSQLResultSet> rs = d.ExecuteQuery(fmt::format("pragma index_list({})", fnName));
    while (rs->NextRow()) {
        if (boost::iequals(rs->Get(3), "pk"))
            return true;
    }
    return false;
}

bool DB::SQLiteBase::IsColumnExist(wpSQLDatabase &d, const std::string &tabName, const std::string &colName) {
    std::shared_ptr<wpSQLResultSet> rs = d.ExecuteQuery(fmt::format("pragma table_info({})", tabName));
    while (rs->NextRow()) {
        std::string col = rs->Get(1);
        if (boost::iequals(col, colName)) return true;
    }
    return false;
}

bool DB::SQLiteBase::IsColumnExist(wpSQLDatabase &d, const std::string &alias, const std::string &tabName, const std::string &colName) {
    std::shared_ptr<wpSQLResultSet> rs = d.ExecuteQuery(fmt::format("pragma {}.table_info({})", alias, tabName));
    while (rs->NextRow()) {
        std::string col = rs->Get(1);
        if (boost::iequals(col, colName)) return true;
    }
    return false;
}

std::shared_ptr<TransactionDB> DB::SQLiteBase::GetTransactionDB() {
    auto x = std::make_shared<TransactionDB>(GetTransactionDBName(), this);
    x->Open();
    return x;
}

void DB::SQLiteBase::CreateObject(const std::string &objectName, const DB::ObjectType ty, const std::vector<std::string> crSQL, bool dropIfExist, const std::string &masterName, const std::string &siblingName) {
    bool objectExist = false;
    try {
        switch (ty) {
            case DB::Index: objectExist = IsIndexExist(objectName); break;
            case DB::Table: objectExist = IsTableExist(objectName); break;
            case DB::View: objectExist = IsViewExist(objectName); break;
            case DB::Trigger: objectExist = IsTriggerExist(objectName); break;
            default: objectExist = false; break;
        }

        if (objectExist && dropIfExist) {
            std::string sql;
            switch (ty) {
                case DB::Index: sql = fmt::format("DROP INDEX {}", objectName); break;
                case DB::Table: sql = fmt::format("DROP TABLE {}", objectName); break;
                case DB::View: sql = fmt::format("DROP VIEW {}", objectName); break;
                case DB::Trigger: sql = fmt::format("DROP TRIGGER {}", objectName); break;
                default: break;
            }
            if (!(sql.empty()))
                db->ExecuteUpdate(sql);
            objectExist = false;
        }

        if (!objectExist) {
            for (const auto &str : crSQL) {
                std::string v(str);
                boost::replace_all(v, "<TABLENAME>", objectName);
                boost::replace_all(v, "<MASTER>", masterName);
                boost::replace_all(v, "<SIBLING>", siblingName);
                if (!v.empty()) {
                    db->ExecuteUpdate(v);
                }
            }
        }
    } catch (wpSQLException &e) {
        LOG_ERROR("Error opening {}: {}", _dbName, e.message);
        exit(-1);
    }
}

void DB::SQLiteBase::TruncateAllTables() {
    std::shared_ptr<wpSQLResultSet> rs = GetSession().ExecuteQuery("select name from sqlite_master where type='table'");
    auto _x = GetSession().GetAutoCommitter();
    while (rs->NextRow()) {
        std::string tabName = rs->Get(0);
        GetSession().ExecuteUpdate(fmt::format("delete from {}", tabName));
    }
    _x->SetOK();
}

void DB::SQLiteBase::DropAllTables() {
    std::vector<std::string> tabNameList;
    {
        std::shared_ptr<wpSQLResultSet> rs = GetSession().ExecuteQuery("select name from sqlite_master where type='table'");
        while (rs->NextRow()) {
            std::string tabName = rs->Get(0);
            tabNameList.emplace_back(tabName);
        }
    }
    for (auto const &it : tabNameList) {
        GetSession().ExecuteUpdate(fmt::format("drop table {}", it));
    }
}

bool DB::SQLiteBase::DeleteDB() {
    db->Close();
    if (!std::filesystem::remove(_dbName)) return false;
    Open();
    return true;
}

int DB::SQLiteBase::ProcessBusyHandler(int nTimesCalled) {  // 0-nomore wait; non-zero continue wait;
    if (nTimesCalled == 0)
        LOG_WARN("Database locked. Sleep for 60 secs.");
    else
        LOG_WARN("Database is still locked. Sleep for another 60 secs.");
    std::this_thread::yield();
    std::this_thread::sleep_for(60s);
    return 1;
}

void DB::SQLiteBase::Open(bool checkAndCreate, OpenMode mode) {
    openMode = mode;
    bool toExecuteCommand = isNewDatabase = false;
    if (boost::iequals(_dbName, ":memory:"))
        toExecuteCommand = isNewDatabase = true;
    else
        toExecuteCommand = isNewDatabase = !std::filesystem::exists(_dbName);
    if (toExecuteCommand) mode = OpenMode::ReadWrite;
    db->Open(_dbName, openMode);

    if (journalOff)
        db->ExecuteUpdate("PRAGMA journal_mode=OFF");
    else if (usingWAL)
        db->ExecuteUpdate("PRAGMA journal_mode=WAL");
    if (turnOffSynchronize) db->ExecuteUpdate("PRAGMA synchronous=off");
    if (exclusiveMode) db->ExecuteUpdate("PRAGMA locking_mode=EXCLUSIVE");

    if (!isNewDatabase) {
        InitFunction();  // create userdefined functions - tables already exists;
    }
    if ((checkAndCreate || toExecuteCommand) && mode == OpenMode::ReadWrite) {
        CreateAllObjects(checkAndCreate, toExecuteCommand);
        if (toExecuteCommand) {
            PopulateTables();  // only executed once, first time created.
        }
        Initialize();
        CheckStructure();
    }
    ResetRegistry();  // clearing all remaining prepared statements;
    InitializeLocalVariables();
    if (isNewDatabase) {
        InitFunction();  // create userdefined functions - tables just created above.
    }
    _dropAllObjects = false;
}

void DB::SQLiteBase::Close() {
    if (db->IsOpen()) {
        db->Close();
    }
}

void DB::SQLiteBase::CreateAllObjects(bool checkAndCreate, bool toExecuteCommand) {
    if (!(checkAndCreate || toExecuteCommand)) return;
    std::vector<DBObjects> list = objectList();
    for (auto const &p : list) {
        if (p.objectType == DB::Command) {
            if (toExecuteCommand) CreateObject(p.objectName, p.objectType, p.createSQL, false, _masterName, _siblingName);
        } else
            CreateObject(p.objectName, p.objectType, p.createSQL, _dropAllObjects, _masterName, _siblingName);
    }
}

enum ColumnTypeTabDelim { None,
    KeyCode,
    DivFactor,
    date,
    year,
    month,
    expiry };

// erase nonprintable char : t.erase(std::remove_if(t.begin(), t.end(), [](unsigned char x) { return !std::isprint(x); }), t.end());
// replace std::replace_if(t.begin(), t.end(), [](unsigned char v) { return !std::isalnum(v); }, '.');

std::string ConvertRowValue(int *colDef, std::shared_ptr<wpSQLResultSet> rs, int i, std::function<std::string(std::shared_ptr<wpSQLResultSet>)> fn = nullptr) {
    std::string t = fn ? fn(rs) : rs->Get(i);
    switch (colDef[i]) {
        case ColumnTypeTabDelim::DivFactor: {
            double x = rs->Get<double>(i);
            t = std::to_string(x);
            break;
        }
        case ColumnTypeTabDelim::date: {
            auto x = rs->Get<TimePoint>(i);
            if (IsValidTimePoint(x))
                t = FormatDate(x, "%d-%m-%Y");
            break;
        }
        case ColumnTypeTabDelim::year: {
            auto x = rs->Get<TimePoint>(i);
            t = FormatDate(x, "%Y");
            break;
        }
        case ColumnTypeTabDelim::month: {
            auto x = rs->Get<TimePoint>(i);
            if (IsValidTimePoint(x))
                t = FormatDate(x, "%b-%Y");
            break;
        }
        case ColumnTypeTabDelim::expiry: {
            auto x = rs->Get<TimePoint>(i);
            if (IsValidTimePoint(x))
                t = FormatDate(x, wpEXPIRYDATEFORMAT);
            break;
        }
        default:
            break;
    }
    return t;
}

void CreateColumnDefinition(int *colDef, std::shared_ptr<wpSQLResultSet> rs, int i) {
    colDef[i] = -1;
    std::string colName = rs->GetColumnName(i);
    if (boost::icontains(colName, "@keycode")) {
        colDef[i] = ColumnTypeTabDelim::KeyCode;
    } else if (boost::icontains(colName, "@dividefactor")) {
        colDef[i] = ColumnTypeTabDelim::DivFactor;
    } else if (boost::icontains(colName, "@date")) {
        colDef[i] = ColumnTypeTabDelim::date;
    } else if (boost::icontains(colName, "@year")) {
        colDef[i] = ColumnTypeTabDelim::year;
    } else if (boost::icontains(colName, "@month")) {
        colDef[i] = ColumnTypeTabDelim::month;
    } else if (boost::icontains(colName, "@expiry")) {
        colDef[i] = ColumnTypeTabDelim::expiry;
    }
}

std::string DB::SQLiteBase::GetResultTabDelimited(std::shared_ptr<wpSQLResultSet> rs, int nRows, bool useActualTab, bool showColHeader, const std::string &filename) {
    std::string delim0;
    std::string delim;
    const std::string eolChar = useActualTab ? std::string("\r\n") : std::string(1, EOLINECHAR);
    const std::string eofChar = useActualTab ? std::string("\t") : std::string(1, EOFFIELDCHAR);

    // auto sout = std::make_unique<std::ostringstream>(std::ios_base::out);
    std::unique_ptr<std::ostream> out;
    if (filename.empty())
        out = std::make_unique<std::ostringstream>(std::ios_base::out);
    else
        out = std::make_unique<std::ofstream>(filename, std::ios_base::out);

    if (nRows >= 0) {
        *out << std::to_string(nRows) << eolChar;
    }
    int *colDef = new int[rs->GetColumnCount()];
    memset(colDef, -1, rs->GetColumnCount());
    //	std::vector<ColumnTypeTabDelim> colDef; colDef.resize(rs->GetColumnCount(), ColumnTypeTabDelim::None);
    for (int i = 0; i < rs->GetColumnCount(); i++) {
        CreateColumnDefinition(colDef, rs, i);
        if (showColHeader) {
            std::string str {rs->GetColumnName(i)};
            boost::tokenizer<boost::char_separator<char>> tok(str, boost::char_separator<char>("@", "", boost::keep_empty_tokens));
            auto it = tok.begin();
            std::string ttl = rs->GetColumnName(i);
            if (it != tok.end()) ttl = *it;

            *out << delim;
            *out << ttl;
            delim = eofChar;
        }
    }
    if (showColHeader) {
        delim0 = eolChar;
    }
    int nRow = 0;
    for (; rs->NextRow(); nRow++) {
        *out << delim0;
        delim = "";
        std::string rowRec;
        for (int i = 0; i < rs->GetColumnCount(); i++) {
            rowRec.append(delim);
            std::string t = ConvertRowValue(colDef, rs, i);
            if (useActualTab)
                rowRec.append(boost::trim_copy(t));
            else
                rowRec.append(t);
            delim = eofChar;
        }
        *out << rowRec;
        delim0 = eolChar;
    }
    delete[] colDef;
    if (filename.empty())
        return static_cast<std::ostringstream *>(out.get())->str();
    else
        return std::to_string(nRow);
}

std::vector<std::vector<std::string>> DB::SQLiteBase::GetVectorResult(std::shared_ptr<wpSQLResultSet> rs, int nRows, bool showColHeader) {
    std::vector<std::vector<std::string>> result;
    try {
        int *colDef = new int[rs->GetColumnCount()];
        memset(colDef, -1, rs->GetColumnCount());
        std::vector<std::string> *currentRow {nullptr};
        if (showColHeader) currentRow = &result.emplace_back();
        for (int i = 0; i < rs->GetColumnCount(); i++) {
            CreateColumnDefinition(colDef, rs, i);
            if (showColHeader) {
                std::string str {rs->GetColumnName(i)};
                boost::tokenizer<boost::char_separator<char>> tok(str, boost::char_separator<char>("@", "", boost::keep_empty_tokens));
                auto ttl = rs->GetColumnName(i);
                auto it = tok.begin();
                if (it != tok.end()) ttl = *it;
                currentRow->emplace_back(ttl);
            }
        }
        int nRow = 0;
        for (; rs->NextRow(); nRow++) {
            currentRow = &result.emplace_back();
            for (int i = 0; i < rs->GetColumnCount(); i++) {
                currentRow->emplace_back(ConvertRowValue(colDef, rs, i));
            }
        }
        delete[] colDef;
    } catch (...) {
        LOG_ERROR("GetVectorResult throw exception");
    }
    return result;
}

std::string DB::SQLiteBase::GetDayName(int i) {
    std::string v;
    switch (i) {
        case 0: v = Translate("Sunday"); break;
        case 1: v = Translate("Monday"); break;
        case 2: v = Translate("Tuesday"); break;
        case 3: v = Translate("Wednesday"); break;
        case 4: v = Translate("Thursday"); break;
        case 5: v = Translate("Friday"); break;
        case 6: v = Translate("Saturday"); break;
    }
    return v;
}

std::string DB::SQLiteBase::GetMonthName(int i) {
    std::string v;
    switch (i) {
        case 1: v = Translate("January"); break;
        case 2: v = Translate("February"); break;
        case 3: v = Translate("March"); break;
        case 4: v = Translate("April"); break;
        case 5: v = Translate("May"); break;
        case 6: v = Translate("June"); break;
        case 7: v = Translate("July"); break;
        case 8: v = Translate("August"); break;
        case 9: v = Translate("September"); break;
        case 10: v = Translate("October"); break;
        case 11: v = Translate("November"); break;
        case 12: v = Translate("December"); break;
    }
    return v;
}

auto concat(sqlite3_context *ctx, int argc, sqlite3_value **data, const std::string &delim) -> void {
    try {
        std::string res = "";
        std::string d = "";
        for (int i = 0; i < argc; i++) {
            auto str = (const char *)sqlite3_value_text(data[i]);
            if (str == nullptr) continue;
            auto ss = std::string(str);
            if (ss.empty()) continue;

            std::string v = boost::trim_copy(ss);
            if (!v.empty()) {
                res += d + v;
                d = delim;
            }
        }
        sqlite3_result_text(ctx, res.c_str(), -1, SQLITE_TRANSIENT);
    } catch (...) {
        LOG_ERROR("concat: throw exception");
    }
}

auto concatComma(sqlite3_context *ctx, int argc, sqlite3_value **data) -> void { concat(ctx, argc, data, ", "); }
auto concatSpaces(sqlite3_context *ctx, int argc, sqlite3_value **data) -> void { concat(ctx, argc, data, " "); }
auto concatMultilines(sqlite3_context *ctx, int argc, sqlite3_value **data) -> void { concat(ctx, argc, data, "\n"); }
auto getAgeInMonth(sqlite3_context *ctx, int argc, sqlite3_value **data) -> void {
    try {
        if (argc != 1) {
            sqlite3_result_int(ctx, 0);
            return;
        }
        auto t = to_chrono(sqlite3_value_int64(data[0]));
        TimePoint tn = TimePoint::clock::now();
        auto months = std::chrono::duration_cast<std::chrono::months>(tn - t).count();
        sqlite3_result_double(ctx, months);
    } catch (...) {
        LOG_ERROR("getAgeInMonth: throw exception");
    }
}
auto fillZero(sqlite3_context *ctx, int /*argc*/, sqlite3_value **data) -> void {
    try {
        std::string param = (const char *)sqlite3_value_text(data[0]);
        boost::trim(param);
        int len = sqlite3_value_int(data[1]);
        int padLen = len - param.length();
        if (padLen >= 0)
            param.insert(0, padLen, '0');  // param.Pad(padLen, '0', false);
        else
            throw wpSQLException(fmt::format(" FillZero - length exceed required length: ({}, {}, {})", param, len, padLen), 0, 0);
        sqlite3_result_text(ctx, param.c_str(), -1, SQLITE_TRANSIENT);
    } catch (...) {
        LOG_ERROR("fillZero: throw exception");
    }
}
auto tokenizer(sqlite3_context *ctx, int argc, sqlite3_value **data) -> void {
    try {
        std::string v;
        if (argc >= 2) {
            auto s = (const char *)sqlite3_value_text(data[0]);
            if (s != nullptr) {
                int idx = sqlite3_value_int(data[1]);  // zero based index;
                std::string str {s};
                boost::tokenizer<boost::char_separator<char>> tok(str, boost::char_separator<char>(attributeMark_delimiter, "", boost::keep_empty_tokens));
                int i = 0;
                for (const auto& it : tok) {
                    if (i == idx) {
                        v = it;
                        break;
                    }
                    i++;
                }
            }
        } else if (argc == 1)
            v = (const char *)sqlite3_value_text(data[0]);

        sqlite3_result_text(ctx, v.c_str(), -1, SQLITE_TRANSIENT);
    } catch (...) {
        LOG_ERROR("tokenizer: throw exception");
    }
}

auto settoken(sqlite3_context *ctx, int argc, sqlite3_value **data) -> void {
    try {
        std::string res;
        if (argc == 3) {
            std::string s = (const char *)sqlite3_value_text(data[0]);
            int idx = sqlite3_value_int(data[1]);  // zero based index;
            std::string newVal = (const char *)sqlite3_value_text(data[2]);
            std::string delim;
            boost::tokenizer<boost::char_separator<char>> tok(s, boost::char_separator<char>(attributeMark_delimiter, "", boost::keep_empty_tokens));
            int i = 0;
            for (auto it = tok.begin(); it != tok.end(); it++, i++) {
                std::string v;
                if (i == idx) v = newVal;
                res.append(delim);
                res.append(v);
                delim = attributeMark_delimiter;
            }
        } else
            res = (const char *)sqlite3_value_text(data[0]);

        sqlite3_result_text(ctx, res.c_str(), -1, SQLITE_TRANSIENT);
    } catch (...) {
        LOG_ERROR("settoken: throw exception");
    }
}

auto getSequenceNo(sqlite3_context *ctx, int argc, sqlite3_value **data) -> void {
    try {
        auto db = (DB::SQLiteBase *)sqlite3_user_data(ctx);
        int64_t ofs = 0;
        std::string prev;
        if (argc == 0)
            db->getSequence.seqNo++;
        else if (argc == 1) {
            ofs = sqlite3_value_int64(data[0]);
            db->getSequence.seqNo++;
        } else if (argc > 1) {
            prev = (const char *)sqlite3_value_text(data[0]);
            ofs = sqlite3_value_int64(data[1]);
            if (prev != db->getSequence.prevNo)
                db->getSequence.seqNo++;
            else
                db->getSequence.prevNo = prev;
        }
        sqlite3_result_int64(ctx, db->getSequence.seqNo + ofs);
    } catch (...) {
        LOG_ERROR("getSequenceNo: throw exception");
    }
}

auto getDayName(sqlite3_context *ctx, int argc, sqlite3_value **data) -> void {
    try {
        auto db = (DB::SQLiteBase *)sqlite3_user_data(ctx);
        if (argc != 1) {
            sqlite3_result_text(ctx, "", -1, SQLITE_TRANSIENT);
            return;
        }
        sqlite3_result_text(ctx, db->GetDayName(sqlite3_value_int(data[0])).c_str(), -1, SQLITE_TRANSIENT);
    } catch (...) {
        LOG_ERROR("getDayName: throw exception");
    }
}

auto setTimeZero(sqlite3_context *ctx, int argc, sqlite3_value **data) -> void {
    TimePoint v;
    try {
        if (argc >= 1) {
            v = to_chrono(sqlite3_value_int64(data[0]));
            v = GetBeginOfDay(v);
        }
    } catch (std::bad_cast &e) {
        LOG_ERROR(fmt::format("setTimeZero: bad_cast --> {}", e.what()));
    } catch (...) {
        LOG_ERROR("setTimeZero: unknown exception");
    }
    sqlite3_result_int64(ctx, to_number(v));
}

std::string DB::SQLiteBase::FormatNumber(double value, int nDec) {
    struct Numpunct : public std::numpunct<char> {
    protected:
        virtual char do_thousands_sep() const { return ','; }
        virtual std::string do_grouping() const { return "\03"; }
    };
    std::stringstream ss;
    ss.imbue({std::locale(), new Numpunct});
    ss << std::setprecision(nDec) << std::fixed << value;
    return ss.str();
}

auto formatNumber(sqlite3_context *ctx, int argc, sqlite3_value **data) -> void {
    try {
        if (argc != 1) {
            sqlite3_result_text(ctx, "", -1, SQLITE_TRANSIENT);
            return;
        }
        std::string v = DB::SQLiteBase::FormatNumber(sqlite3_value_double(data[0]));
        sqlite3_result_text(ctx, v.c_str(), -1, SQLITE_TRANSIENT);
    } catch (...) {
        LOG_ERROR("formatNumber: throw exception");
    }
}

void DB::SQLiteBase::InitFunction() {
    ResetRegistry();
    GetSession().CreateFunction("comma", -1, concatComma);
    GetSession().CreateFunction("space", -1, concatSpaces);
    GetSession().CreateFunction("spaces", -1, concatSpaces);
    GetSession().CreateFunction("multilines", -1, concatMultilines);
    GetSession().CreateFunction("agemonth", 1, getAgeInMonth);
    GetSession().CreateFunction("fillzero", 2, fillZero);
    GetSession().CreateFunction("gettoken", 2, tokenizer);
    GetSession().CreateFunction("settoken", 3, settoken);
    GetSession().CreateFunction("seqno", -1, getSequenceNo, this);
    GetSession().CreateFunction("getdayname", 1, getDayName, this);
    GetSession().CreateFunction("removetime", 1, setTimeZero);
    GetSession().CreateFunction("formatnumber", 1, formatNumber);
}

std::string DB::SQLiteBase::GetAllRows(const std::string &t, const std::string &sql) {
    if (!IsTableExist(t)) return "";
    std::string res, rowDelim;
    GetSession().Execute(sql, [&res, &rowDelim](int argc, char **data, char ** /*colNames*/) {
        std::string EOFFIELDSTRING(1, EOFFIELDCHAR);
        std::string EOLINESTRING(1, EOLINECHAR);
        res.append(rowDelim);
        std::string colDelim;
        for (int i = 0; i < argc; i++) {
            res.append(colDelim + data[i]);
            colDelim = EOFFIELDSTRING;
        }
        rowDelim = EOLINESTRING;
    });
    return res;
}

std::string DB::SQLiteBase::GetJSON(const std::string &sql) {
    return "";
}

std::string DB::SQLiteBase::GetKeyValueJSON(const std::string &t, const std::string &sql) {
    return "";
}

DB::Inserter::Inserter(wpSQLDatabase &_db, const std::string &findSQL, const std::string &insertSQL, const std::string &countThisIDstr, const std::string &getLastIDsql) : db(_db) {
    find = db.PrepareStatement(findSQL);
    insert = db.PrepareStatement(insertSQL);
    if (!getLastIDsql.empty())
        getLastID = db.PrepareStatement(getLastIDsql);
    if (!countThisIDstr.empty())
        countThisID = db.PrepareStatement(countThisIDstr);
}

int64_t DB::Inserter::InsertIfNotExists(const std::string &code, const std::string &name) {
    find->Bind(1, code);
    std::shared_ptr<wpSQLResultSet> rs = find->ExecuteQuery();
    if (rs->NextRow()) return rs->Get<int64_t>(0);

    insert->Bind(1, code);
    insert->Bind(2, name);
    insert->ExecuteUpdate();
    return db.GetLastRowId();
}

int64_t DB::Inserter::InsertIfNotExists(int64_t id, const std::string &code, const std::string &name) {
    find->Bind(1, code);
    std::shared_ptr<wpSQLResultSet> rs = find->ExecuteQuery();
    if (rs->NextRow()) return rs->Get<int64_t>(0);

    countThisID->Bind(1, id);
    if (countThisID->ExecuteScalar() > 0) {
        id = getLastID->ExecuteScalar();
    }

    insert->Bind(1, id);
    insert->Bind(2, code);
    insert->Bind(3, name);
    insert->ExecuteUpdate();
    return id;
}

DB::UserDBRegistry::UserDBRegistry(DB::SQLiteBase *db) : sqlDB(db) {
    fn_sttFindKey = [&]() -> std::shared_ptr<wpSQLStatement> { return sqlDB->IsTableExist("ul_keys") ? sqlDB->GetSession().PrepareStatement("select id, value from ul_keys where key = ?") : NULL; };
    fn_sttFindLocalKey = [&]() -> std::shared_ptr<wpSQLStatement> { return sqlDB->IsTableExist("ul_localkeys") ? sqlDB->GetSession().PrepareStatement("select id, value from ul_LocalKeys where key = ?") : NULL; };
    fn_sttEraseKey = [&]() -> std::shared_ptr<wpSQLStatement> { return sqlDB->IsTableExist("ul_keys") ? sqlDB->GetSession().PrepareStatement("delete from ul_keys where key = ?") : NULL; };
    fn_sttEraseLocalKey = [&]() -> std::shared_ptr<wpSQLStatement> { return sqlDB->IsTableExist("ul_localkeys") ? sqlDB->GetSession().PrepareStatement("delete from ul_LocalKeys where key = ?") : NULL; };
}

std::shared_ptr<wpSQLStatement> DB::UserDBRegistry::GetUpdateKeyStatement() { return sqlDB->IsTableExist("ul_keys") ? sqlDB->GetSession().PrepareStatement("update ul_keys set value=?, isDeleted=NULL where key = ?") : NULL; }
std::shared_ptr<wpSQLStatement> DB::UserDBRegistry::GetInsertKeyStatement() { return sqlDB->IsTableExist("ul_keys") ? sqlDB->GetSession().PrepareStatement("insert into ul_keys (key, value) values (?,?)") : NULL; }
std::shared_ptr<wpSQLStatement> DB::UserDBRegistry::GetLocalUpdateKeyStatement() { return sqlDB->IsTableExist("ul_localkeys") ? sqlDB->GetSession().PrepareStatement("update ul_LocalKeys set value=?, isDeleted=NULL where key = ?") : NULL; }
std::shared_ptr<wpSQLStatement> DB::UserDBRegistry::GetLocalInsertKeyStatement() { return sqlDB->IsTableExist("ul_localkeys") ? sqlDB->GetSession().PrepareStatement("insert into ul_LocalKeys (key, value) values (?,?)") : NULL; }


void DB::UserDBRegistry::EraseKey(const std::string &key) {
    std::shared_ptr<wpSQLStatement> stt = fn_sttEraseKey();
    if (!stt) return;

    std::shared_ptr<wpSQLStatement> sttFind = fn_sttFindKey();
    if (!sttFind) return;
    sttFind->Bind(1, key);
    std::shared_ptr<wpSQLResultSet> rs = sttFind->ExecuteQuery();
    if (!rs->NextRow()) return;
    stt->Bind(1, key);
    stt->ExecuteUpdate();
}

void DB::UserDBRegistry::EraseLocalKey(const std::string &key) {
    std::shared_ptr<wpSQLStatement> stt = fn_sttEraseLocalKey();
    if (!stt) return;
    stt->Bind(1, key);
    stt->ExecuteUpdate();
}

bool DB::UserDBRegistry::FindLocalKey(const std::string &key) {
    if (!sqlDB->IsTableExist("ul_localkeys")) return false;
    std::shared_ptr<wpSQLStatement> stt = fn_sttFindLocalKey();
    stt->Bind(1, key);

    std::shared_ptr<wpSQLResultSet> rs = stt->ExecuteQuery();
    if (rs->NextRow())
        return true;
    return false;
}

bool DB::UserDBRegistry::IsKeyExists(const std::string &key, bool isLocalKey) {
    if (isLocalKey) {
        if (isLocalKey && !sqlDB->IsTableExist("ul_localkeys")) return false;
        std::shared_ptr<wpSQLStatement> stt = fn_sttFindLocalKey();
        stt->Bind(1, key);
        std::shared_ptr<wpSQLResultSet> rs = stt->ExecuteQuery();
        if (rs->NextRow())
            return true;
        return false;
    }
    std::shared_ptr<wpSQLStatement> stt = fn_sttFindKey();
    stt->Bind(1, key);
    std::shared_ptr<wpSQLResultSet> rs = stt->ExecuteQuery();
    if (rs->NextRow())
        return true;
    return false;
}

std::vector<DB::DBObjects> TransactionDB::objectList() const {
    std::vector<DB::DBObjects> list {
        {"UL_Sessions", DB::Table,
            {"create table <TABLENAME>("
             "id integer primary key, "
             "timeIn integer, "
             "timeOut integer, "
             "ipAddress text, "
             "UserID integer, "
             "terminalID integer,"
             "listenerPortNo integer,"
             "remark text"
             ")",
                "create index idx_session_uid on UL_sessions(userid)", "create index idx_session_terminalID on UL_sessions(terminalID, userID)"}},
        {"UL_LocalKeys", DB::Table,
            {"create table <TABLENAME>("
             "id integer primary key, "
             "key text, "
             "value text, "
             "description text, "
             "isDeleted integer, "
             "unique(key) "
             ")",
                "create index idx_<TABLENAME>_key on <TABLENAME>(key)"}}};
    auto ol = DB::SQLiteBase::objectList();
    ol.insert(ol.end(), list.begin(), list.end());
    return ol;
}

bool TransactionDB::AttachMasterDB() {
    bool found = false;
    GetSession().Execute("pragma database_list", [&found](int, char **data, char **) {
        found = found || (std::strcmp("master", data[1]) == 0);
    });
    if (!found) return false;
    GetSession().ExecuteUpdate(fmt::format("attach database '{}' as master", GetMasterDBName()));
    return true;
}

bool TransactionDB::DetachMasterDB() {
    bool found = false;
    GetSession().Execute("pragma database_list", [&found](int, char **data, char **) {
        found = found || (std::strcmp("master", data[1]) == 0);
    });
    if (found) return false;
    // Close();
    // Open();
    GetSession().ExecuteUpdate("detach database master");
    return true;
}

extern bool NeedDivision(const std::string &colName);

extern std::vector<std::string> tableToDrop;

bool TransactionDB::Migrate(DB::SQLiteBase *master, std::vector<std::string> &tList) {
    std::vector<std::string> tblList = tList;
    if (tblList.empty()) {
        GetSession().Execute("select name from sqlite_master where type='table'", [&tblList](int, char **data, char **) {
            tblList.emplace_back(data[0]);
        });
    }
    bool processed = false;
    LOG_INFO("Migration from {} to {}", master->GetDBName(), GetDBName());
    for (auto const &tabName : tblList) {
        try {
            auto _x = GetSession().GetAutoCommitter();
            if (master->IsTableExist(tabName)) {
                std::shared_ptr<wpSQLResultSet> rs = master->GetSession().ExecuteQuery(fmt::format("select sql from sqlite_master where name like '{}'", tabName));
                if (rs->NextRow()) {
                    if (GetSession().TableExists(tabName)) {
                        GetSession().ExecuteUpdate("delete from " + tabName);
                    } else
                        GetSession().ExecuteUpdate(rs->Get(0));  // create table;
                    rs.reset();                                  // close the cursor on Master

                    std::map<std::string, std::string> masterColumns;
                    std::vector<double> divideFactor;
                    auto rssMaster = master->GetSession().ExecuteQuery(fmt::format("pragma table_info({})", tabName));  // origin;
                    while (rssMaster->NextRow()) {
                        masterColumns[rssMaster->Get(1)] = rssMaster->Get(2);
                    }

                    auto rss = GetSession().ExecuteQuery(fmt::format("pragma table_info({})", tabName));  // destination;
                    std::string selSQL = "select ", insSQL = fmt::format("insert into {}(", tabName), vals = " values(";
                    std::string delim;
                    while (rss->NextRow()) {
                        std::string colName = rss->Get(1);
                        if (masterColumns.find(colName) == masterColumns.end()) continue;  // not found in master;
                        if (boost::iequals(masterColumns[colName], "integer") && boost::iequals(rss->Get(2), "real") && NeedDivision(colName)) {
                            divideFactor.push_back(10000.0);
                        } else
                            divideFactor.push_back(1.0);
                        selSQL.append(delim);
                        selSQL.append(colName);
                        insSQL.append(delim);
                        insSQL.append(colName);
                        vals.append(delim);
                        vals.append("?");
                        delim = ",";
                    }
                    selSQL.append(fmt::format(" from {}", tabName));
                    insSQL.append(")");
                    vals.append(")");
                    std::shared_ptr<wpSQLStatement> sttSel = master->GetSession().PrepareStatement(selSQL);
                    std::shared_ptr<wpSQLStatement> sttIns = GetSession().PrepareStatement(insSQL + vals);
                    LOG_INFO("Migrating...{} ", tabName);
                    rss = sttSel->ExecuteQuery();
                    while (rss->NextRow()) {
                        for (int i = 0; i < rss->GetColumnCount(); i++) {
                            if (rss->IsNull(i)) {
                                sttIns->BindNull(i + 1);
                            } else {
                                if (divideFactor[i] > 1)
                                    sttIns->Bind(i + 1, rss->Get<double>(i) / divideFactor[i]);
                                else
                                    sttIns->Bind(i + 1, rss->Get(i));
                            }
                        }
                        sttIns->ExecuteUpdate();
                    }
                    rss.reset();
                    sttSel.reset();
                    sttIns.reset();
                    tableToDrop.push_back(tabName);
                    processed = true;
                    LOG_INFO("Migrating... {} > Ok", tabName);
                }
            }
            _x->SetOK();
        } catch (wpSQLException &e) {
            LOG_ERROR("wpSQLException {}", e.message);
            return false;
        } catch (std::exception &e) {
            LOG_ERROR("std::exception {}", e.what());
            return false;
        }
    }
    LOG_INFO("Migration from {} to {} completed.", master->GetDBName(), GetDBName());
    return processed;
}
