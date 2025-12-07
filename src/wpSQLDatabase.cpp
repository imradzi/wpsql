#include "precompiled/libcommon.h"
#ifdef __clang__
#if __has_warning("-Wdeprecated-enum-enum-conversion")
#pragma clang diagnostic ignored "-Wdeprecated-enum-enum-conversion"  // warning: bitwise operation between different enumeration types ('XXXFlags_' and 'XXXFlagsPrivate_') is deprecated
#endif
#endif

#ifdef __WX__
#include "wx/wxprec.h"
#ifndef WX_PRECOMP
#include "wx/wx.h"
#endif
#include <wx/tokenzr.h>
#include <wx/filename.h>
#endif

#include <chrono>
#include <deque>
#include <set>
#include <iomanip>
#include <sstream>
#include <thread>
#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>
#include <boost/tokenizer.hpp>
#ifdef PPOS_DB
#include "global.h"
#include "logger.h"
#include "words.h"
#endif

#include "wpSQLDatabase.h"

// std::string BuildFTSSearch(const std::string &param) {
//     std::string res, delim;
//     wxStringTokenizer tok(param, " \t\n");
//     while (tok.HasMoreTokens()) {
//         wxString v = tok.GetNextToken();
//         v.Replace("\"", "\"\"", true);
//         res.append(delim + "\"" + v + "\"" + "*");
//         delim = " ";
//     }
//     if (res.empty()) res = "ALL";
//     return res;
// }

std::string BuildFTSSearch(const std::string& param) {
    std::string res, delim;
    boost::tokenizer<boost::char_separator<char>> tok(param, boost::char_separator<char>(" \t\n", "", boost::drop_empty_tokens));
    for (auto x: tok) {
        boost::ireplace_all(x, "\"", "\"\"");
        res.append(delim + "\"" + x + "\"" + "*");
        delim = " ";
    }
    if (res.empty()) res = "";
    return res;
}

std::string FormatDate(const std::chrono::system_clock::time_point &tp, const char *format) {
    auto ymd = std::chrono::year_month_day(floor<std::chrono::days>(tp));
    auto ymd_max = std::chrono::year_month_day(floor<std::chrono::days>(std::chrono::system_clock::now() + std::chrono::years(100)));
    if (ymd.year() > ymd_max.year()) {
        return "forever";
    }
    std::time_t s_time = std::chrono::system_clock::to_time_t(tp);
    std::ostringstream oss;
    oss << std::put_time(std::localtime(&s_time), format);
    return oss.str();
}

std::string FormatDateUTC(const std::chrono::system_clock::time_point &tp, const char *format) {
    std::time_t s_time = std::chrono::system_clock::to_time_t(tp);
    std::ostringstream oss;
    oss << std::put_time(std::gmtime(&s_time), format);
    return oss.str();
}

// std::to_string(std::this_thread::get_id())

#ifndef PPOS_DB
std::string GetThreadID() {
    std::ostringstream ss;
    ss << std::this_thread::get_id();
    return ss.str();
}
#endif

wpSQLException::wpSQLException(const std::string m, int rc_, sqlite3 *db) : rc(rc_) {
    message = fmt::format("[{thread}] {msg} :rc=[{rc}], sqlerror=[{sqlerror}] db=[{dbname}]",
        fmt::arg("thread", GetThreadID()),
        fmt::arg("msg", m),
        fmt::arg("rc", rc),
        fmt::arg("sqlerror", (db ? sqlite3_errmsg(db) : "")),
        fmt::arg("dbname", (db ? sqlite3_db_filename(db, "main") : "name?")));
}

wpAutoCommitter::wpAutoCommitter(wpSQLDatabase *a) : toRollBack(true), db(a) {
    try {
        isAuto = db->IsAutoCommit();
        if (isAuto) db->Begin();
    } catch (wpSQLException &e) {
        toRollBack = true;
        throw(e);
    } catch (std::exception &e) {
        toRollBack = true;
        throw(e);
    } catch (...) {
        toRollBack = true;
        throw;
    }
}

wpAutoCommitter::~wpAutoCommitter() {
    if (isAuto && !toRollBack) db->Commit();
}

const void *GetSQLFunctionParamBlob(sqlite3_value *data, int &len) {
    len = sqlite3_value_bytes(data);
    ;
    return sqlite3_value_blob(data);
}

wpSQLManager::~wpSQLManager() {
    sqlite3_close_v2(db);
    // std::string fName(sqlite3_db_filename(db, "main"));
    // if (rc == SQLITE_OK) ShowLog("closed " + fName + " OK.");
    // else ShowLog("closed " + fName + " failed."); //	throw wpSQLException("error closing DB", rc, db);
    db = NULL;
}

// wpSQLResultSet::~wpSQLResultSet() {
//	if (toFinalize) sqlite3_finalize(stmt);
// }

wpSQLResultSet::wpSQLResultSet(std::shared_ptr<wpSQLStatementManager> s, bool is_eof, bool is_first)
  : stmt(s),
    isEOF(is_eof),
    isFirst(is_first) {
    nCols = sqlite3_column_count(stmt->GetStatement());
    char *p = sqlite3_expanded_sql(stmt->GetStatement());
    if (p) {
        pSQL = p;
        sqlite3_free(p);
    }
}

bool wpSQLResultSet::NextRow() {
    int rc;
    if (isFirst) {
        isFirst = false;
        rc = (isEOF) ? SQLITE_DONE : SQLITE_ROW;
    } else
        rc = sqlite3_step(stmt->GetStatement());

    if (rc == SQLITE_DONE) {
        isEOF = true;
        return false;
    } else if (rc == SQLITE_ROW)
        return true;
    else
        throw wpSQLException("resultset:nextrow error: ", rc, stmt->GetSQLite3());
}

int wpSQLResultSet::GetColumnIndex(const std::string &colName) const {
    if (!colName.empty()) {
        for (int i = 0; i < nCols; i++)
            if (boost::iequals(colName, sqlite3_column_name(stmt->GetStatement(), i)) == 0) return i;
    }
    return -1;
}

int wpSQLResultSet::GetColumnType(int i) const {
    if (i < 0 || i >= nCols) throw wpSQLException("GetColumnType: index out of bound", 0, NULL);
    return sqlite3_column_type(stmt->GetStatement(), i);
}

std::string wpSQLResultSet::GetColumnName(int i) const {
    if (i < 0 || i >= nCols)
        throw wpSQLException("GetColumnName: index out of bound", 0, NULL);
    return reinterpret_cast<const char *>(sqlite3_column_name(stmt->GetStatement(), i));
}

void wpSQLStatement::Reset() {
    int rc = sqlite3_reset(stmt->GetStatement());
    needReset = false;
    if (rc != SQLITE_OK) throw wpSQLException("reset statement error", rc, stmt->GetSQLite3());
}

std::shared_ptr<wpSQLResultSet> wpSQLStatement::Execute() {
    int rc = sqlite3_step(stmt->GetStatement());
    needReset = true;
    if (rc == SQLITE_DONE) {                                        // no more rows
        return std::make_shared<wpSQLResultSet>(stmt, true, true);  // resultset won't finalize statement; it's wpSQLStatement to finalize;
    } else if (rc == SQLITE_ROW) {                                  // one or more rows
        return std::make_shared<wpSQLResultSet>(stmt, false, true);
    } else {
        std::string sql(sqlite3_sql(stmt->GetStatement()));
        sqlite3_reset(stmt->GetStatement());
        throw wpSQLException("Execute statement error: [" + sql + "] ", rc, stmt->GetSQLite3());
    }
}

int wpSQLStatement::ExecuteUpdate() {
    needReset = true;
    int rc = sqlite3_step(stmt->GetStatement());
    if (rc == SQLITE_DONE) {
        int rowsChanged = sqlite3_changes(stmt->GetSQLite3());
        rc = sqlite3_reset(stmt->GetStatement());
        if (rc != SQLITE_OK) {
            std::string sql(sqlite3_sql(stmt->GetStatement()));
            throw wpSQLException("wpSQLStatement::ExecuteUpdate> [" + sql + "] ", rc, stmt->GetSQLite3());
        }
        return rowsChanged;
    } else {
        std::string sql(sqlite3_sql(stmt->GetStatement()));
        sqlite3_reset(stmt->GetStatement());
        throw wpSQLException("wpSQLStatement::ExecuteUpdate> expect no rowset [" + sql + "] ", rc, stmt->GetSQLite3());
    }
}

int wpSQLStatement::GetParamIndex(const std::string &name, bool throwIfError) const {
    int rc = sqlite3_bind_parameter_index(stmt->GetStatement(), name.c_str());
    if (rc <= 0 && throwIfError) {
        throw wpSQLException(fmt::format("wpSQLStatement::GetParamIndex> invalid parameter {} for [{}]", name, pSQL), 0, stmt->GetSQLite3());
    }
    return rc;
}

extern int noOfWaitingIteration;
extern int secPerSleep;

bool wpSQLDatabase::Begin() {
    Execute("begin immediate transaction", NULL);
    //Execute("begin transaction", NULL);
    return true;

    // for (int i = 0; true; i++) {
    //     try {
    //         Execute("begin immediate transaction", NULL);
    //         return true;
    //     } catch (wpSQLException &e) {
    //         if (e.rc == SQLITE_BUSY) {
    //             std::this_thread::yield();
    //             std::this_thread::sleep_for(std::chrono::seconds(secPerSleep));
    //         }
    //     }
    //     if (i >= noOfWaitingIteration) {
    //         auto fmt = fmt::format("Lock: not freed after {} sec", (noOfWaitingIteration * secPerSleep));
    //         throw wpSQLException(fmt, SQLITE_BUSY, db->GetSQLite3());
    //     }
    // }
}

void wpSQLDatabase::Commit() {
    Execute("commit transaction", NULL);
    //	beginMutex.Unlock();
}

bool wpSQLDatabase::IsAutoCommit() {
    return IsOpen() ? sqlite3_get_autocommit(GetDB()) != 0 : false;
}

sqlite3_stmt *wpSQLDatabase::Prepare(const std::string &sql) {
    if (!GetDB()) throw wpSQLException("database already closed", 0, NULL);
    const char *tail = 0;
    sqlite3_stmt *stmt;

    int rc = sqlite3_prepare_v2(GetDB(), sql.c_str(), -1, &stmt, &tail);

    if (!stmt) {
        throw wpSQLException(fmt::format("Error preparing {} null pointer!", sql), rc, GetDB());
    }
    if (rc != SQLITE_OK) {
        throw wpSQLException(fmt::format("Error preparing {}", sql), rc, GetDB());
    }
    return stmt;
}

std::vector<std::string> wpSQLDatabase::GetAllActivePreparedStatement() {
    sqlite3_stmt *p = NULL;
    std::vector<std::string> sqlList;
    for (; true;) {
        p = sqlite3_next_stmt(db->GetSQLite3(), p);
        if (!p) return sqlList;
        sqlList.emplace_back(sqlite3_sql(p));
    }
    return sqlList;
}

int wpSQLDatabase::GetNumberOfActivePreparedStatement() {
    sqlite3_stmt *p = NULL;
    int i = 0;
    for (; true;) {
        p = sqlite3_next_stmt(db->GetSQLite3(), p);
        if (!p) return i;
        i++;
    }
    return i;
}

void wpSQLDatabase::FinalizeAllStatements() {
    sqlite3_stmt *p = NULL;
    for (; true;) {
        p = sqlite3_next_stmt(db->GetSQLite3(), p);
        if (!p) return;
        sqlite3_finalize(p);
        p = NULL;
    }
}

std::shared_ptr<wpSQLStatement> wpSQLDatabase::PrepareStatement(const std::string &sql) {
    auto stmt = std::make_shared<wpSQLStatementManager>(db, Prepare(sql));
    return std::make_shared<wpSQLStatement>(stmt);
}

std::shared_ptr<wpSQLResultSet> wpSQLDatabase::Execute(const std::string &sql) {
    if (!GetDB()) throw wpSQLException("database already closed", 0, NULL);
    auto stmt = std::make_shared<wpSQLStatementManager>(db, Prepare(sql));
    for (int i = 0; true; i++) {
        int rc = sqlite3_step(stmt->GetStatement());

        if (rc == SQLITE_DONE)
            return std::make_shared<wpSQLResultSet>(stmt, true, true);  // pass resultset responsibility to finalize statement;
        else if (rc == SQLITE_ROW)
            return std::make_shared<wpSQLResultSet>(stmt, false, true);
        else if (rc == SQLITE_BUSY) {
            if (IsAutoCommit() || boost::icontains(sql, "commit")) {
                if (i >= noOfWaitingIteration) {
                    auto v = fmt::format("Lock: not freed after {} sec", (noOfWaitingIteration * secPerSleep));
                    throw wpSQLException(v, SQLITE_BUSY, db->GetSQLite3());
                }
            } else 
                throw wpSQLException("Lock: db locked!!!", SQLITE_BUSY, db->GetSQLite3());
        }
        throw wpSQLException("wpSQLDatabase::execute error", rc, db->GetSQLite3());
    }
}

int wpSQLDatabase::Execute(const std::string &sql, std::function<void(int, char **, char **)> fn) {
    //std::cout << "wpSQL:execute: " << sql << std::endl;
    if (!GetDB()) throw wpSQLException("database already closed", 0, NULL);
    char *zErrMsg = NULL;
    int rc = 0;
    for (int i = 0; true; i++) {
        if (fn)
            rc = sqlite3_exec(GetDB(), sql.c_str(), callback, (void *)&fn, &zErrMsg);
        else
            rc = sqlite3_exec(GetDB(), sql.c_str(), NULL, NULL, &zErrMsg);
        if (rc == SQLITE_OK) break;
        else if (rc == SQLITE_BUSY) {
            if (IsAutoCommit() || boost::icontains(sql, "commit") || boost::icontains(sql, "begin")) {
                if (i >= noOfWaitingIteration) {
                    auto v = fmt::format("Lock: not freed after {} sec. Error = {}", (noOfWaitingIteration * secPerSleep), (zErrMsg ? zErrMsg : ""));
                    if (zErrMsg) sqlite3_free(zErrMsg);
                    throw wpSQLException(v, SQLITE_BUSY, db->GetSQLite3());
                }
                std::this_thread::yield();
                std::this_thread::sleep_for(std::chrono::seconds(secPerSleep));
            } else {
                auto v = fmt::format("Lock: db locked!!! Error = {}", (noOfWaitingIteration * secPerSleep), (zErrMsg ? zErrMsg : ""));
                if (zErrMsg) sqlite3_free(zErrMsg);
                throw wpSQLException(v, SQLITE_BUSY, db->GetSQLite3());
            }
        } else {
            auto v = fmt::format("Error execute command [{}] {}", sql, (zErrMsg ? zErrMsg : ""));
            if (zErrMsg) sqlite3_free(zErrMsg);
            throw wpSQLException(v, rc, nullptr);
        }
    }
    if (zErrMsg) sqlite3_free(zErrMsg);
    //std::cout << "wpSQL:execute: OK: " << sql << std::endl;
    return sqlite3_changes(GetDB());
}

/*
pragma index_list(ul_localkeys);
0|idx_UL_LocalKeys_key|0|c|0
1|sqlite_autoindex_UL_LocalKeys_1|1|u|0
*/
bool wpSQLDatabase::HasUniqueKey(const std::string &tabName) {
    bool found = false;
    Execute(fmt::format("pragma index_list({})", tabName), [&](int, char **col, char **) {
        if (col[3][0] == 'u' || boost::iequals(std::string(col[3], 2), "pk") || col[2][0] == '1')
            found = true;
    });
    return found;
}

/*
pragma  table_info(workshift);
0|id|integer|0||1
1|terminalID|integer|0||0
2|timeStart|integer|0||0
3|timeEnd|integer|0||0
4|createdBy|integer|0||0
5|closedBy|integer|0||0
6|openBalance|real|0||0
7|closingBalance|real|0||0
*/
bool wpSQLDatabase::HasPrimaryKey(const std::string &tabName) {
    bool found = false;
    Execute(fmt::format("pragma table_info({})", tabName), [&](int, char **col, char **) {
        if (col[5][0] == '1')
            found = true;
    });
    return found;
}

extern int waitPerSlice; // ms
extern int waitingFunction(void *, int nCall);

void wpSQLDatabase::Open(const std::string &fileName, int) {  // one minute wait lock by default
    if (GetDB()) Close();
    sqlite3 *d;
    int rc = sqlite3_open_v2(fileName.c_str(), &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
    if (rc == SQLITE_OK)
        ;  // ShowLog(fileName + " opened ok => " + std::string::FromUTF8(sqlite3_db_filename(d, "main")));
    else {
        // ShowLog(fileName + " failed to open.");
        throw wpSQLException(fmt::format("{} failed to open", fileName), rc, d);
    }
    db = std::make_shared<wpSQLManager>(d);
    sqlite3_busy_timeout(db->GetSQLite3(), waitPerSlice); 
    sqlite3_busy_handler(db->GetSQLite3(), &waitingFunction, this);
    Execute("PRAGMA encoding = \"UTF-16\"", NULL);
}

void wpSQLDatabase::Rollback(const std::string &checkPoint) {
    if (checkPoint.empty())
        ExecuteUpdate("rollback transaction");
    else
        ExecuteUpdate(fmt::format("rollback transaction to savepoint :{}", checkPoint));
}

bool wpSQLDatabase::TableExists(const std::string &tableName, const std::string &databaseName) {
    std::string sql;
    if (databaseName.empty())
        sql = "select count(*) from sqlite_master where type='table' and name like ?";
    else
        sql = fmt::format("select count(*) from {}.sqlite_master where type='table' and name like ?", databaseName);
    auto stmt = PrepareStatement(sql);
    stmt->Bind(1, tableName);
    auto value = stmt->ExecuteScalar();
    return (value > 0);
}

int wpSQLDatabase::callback(void *fnLambda, int argc, char **argv, char **azColName) {
    if (fnLambda) {
        std::function<void(int, char **, char **)> fn = *(std::function<void(int, char **, char **)> *)(fnLambda);  // very dangerous; but could it be done? YES!
        fn(argc, argv, azColName);
    }
    return 0;
}

void wpSQLDatabase::register_function(sqlite3_context *ctx, int argc, sqlite3_value **data) {
    try {
        void *p = sqlite3_user_data(ctx);
        if (p) {
            auto fn = (std::function<void(sqlite3_context *, int, sqlite3_value **)> *)p;
            if (fn) {
                (*fn)(ctx, argc, data);
            } else
                throw wpSQLException("register_function return NULL lambda", 0, nullptr);
        } else
            throw wpSQLException("register_function: return NULL from sqlite3_user_data(context)", 0, nullptr);
    } catch (std::bad_cast &e) {
        throw wpSQLException(fmt::format("wpSQLDatabase::register_function: throw std::bad_cast {}", e.what()), 0, nullptr);
    }
}

void wpSQLDatabase::CreateFunction(const std::string &functionName, int nArg, void (*fn)(sqlite3_context *ctx, int argc, sqlite3_value **data), void *data, bool isDeterministic) {
    int flag = SQLITE_UTF8 | (isDeterministic ? SQLITE_DETERMINISTIC : 0);
    int rc = sqlite3_create_function(GetDB(), functionName.c_str(), nArg, flag, data, fn, NULL, NULL);
    if (rc != SQLITE_OK) throw wpSQLException(fmt::format("creating function {}", functionName), rc, GetDB());
}

int wpSQLDatabase::BackupTo(const std::string &backupName, std::function<bool()> fnIsStopping, std::function<void(int, int)> fnProgressFeedback, int nPagesPerCall, int msSleepPerCall) {
    int rc;
    sqlite3 *backupDB;
    sqlite3_backup *backupHandle;

    rc = sqlite3_open(backupName.c_str(), &backupDB);
    if (rc == SQLITE_OK) {
        backupHandle = sqlite3_backup_init(backupDB, "main", GetDB(), "main");
        if (backupHandle) {
            do {
                if (fnIsStopping && fnIsStopping()) break;
                rc = sqlite3_backup_step(backupHandle, nPagesPerCall);
                if (fnProgressFeedback) fnProgressFeedback(sqlite3_backup_remaining(backupHandle), sqlite3_backup_pagecount(backupHandle));
                if (rc == SQLITE_OK || rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
                    sqlite3_sleep(msSleepPerCall);
                }
            } while (rc == SQLITE_OK || rc == SQLITE_BUSY || rc == SQLITE_LOCKED);
            sqlite3_backup_finish(backupHandle);
        }
    }
    sqlite3_close(backupDB);
    return rc == SQLITE_DONE ? 0 : -1;
}
