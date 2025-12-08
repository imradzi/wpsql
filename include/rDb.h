#pragma once
#ifndef NO_XLS
#include "libxl.h"
class ExcelReader;
#endif
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include <boost/json.hpp>
#include "guid.h"
#include "net.h"
#include "logger.h"
#include "wpSQLDatabase.h"

using ConvertFunction = std::function<std::string(int, const std::string &)>;

namespace DB {
    class SQLiteBase;
}

#ifdef __WX__
#include "guid.h"
#include "sqlexception.h"
#include "xmlParser.h"
#include "events.h"

class TableUpdateTimeSetter {
    std::shared_ptr<wpSQLStatement> sttInsert, sttUpdate;
    std::shared_ptr<wpSQLStatement> sttFind;
    DB::SQLiteBase *db;

public:
    TableUpdateTimeSetter() = default;
    TableUpdateTimeSetter(DB::SQLiteBase *db, Synch::SyncTables type);
    void SetUpdateDate(const std::string &id);
    void SetSyncDate(const std::string &id, const wxDateTime &dt = wxDateTime::UNow());
};
#endif

class ReportPDF;
class TransactionDB;

namespace DB {
    inline std::string concat(std::string_view a, std::string_view b) {
        auto joined = boost::join(a, b);
        return std::string(joined.begin(), joined.end());
    }
    inline std::wstring concat(std::wstring_view a, std::wstring_view b) {
        auto joined = boost::join(a, b);
        return std::wstring(joined.begin(), joined.end());
    }

    template<typename T, typename V>
    bool contains(T list, V value) {
        for (auto x : list) {
            if (x == value) return true;
        }
        return false;
    }

    enum ObjectType { EOT,
        Table,
        Function,
        Procedure,
        View,
        Index,
        Command,
        Constraint,
        Trigger };

    struct DBObjects {
        std::string objectName;
        ObjectType objectType;
        std::vector<std::string> createSQL;

    public:
        DBObjects() = default;
        DBObjects(const std::string &oName, const ObjectType &objType, const std::vector<std::string> &crSQL) noexcept
          : objectName(oName),
            objectType(objType),
            createSQL(crSQL) {}
        DBObjects(const DBObjects &r) noexcept
          : objectName(r.objectName),
            objectType(r.objectType),
            createSQL(r.createSQL) {}
        DBObjects(DBObjects &&r) noexcept
          : objectName(std::move(r.objectName)),
            objectType(std::move(r.objectType)),
            createSQL(std::move(r.createSQL)) {}
        DBObjects &operator=(DBObjects &r) noexcept {
            objectName = r.objectName;
            objectType = r.objectType;
            createSQL = r.createSQL;
            return *this;
        }
        DBObjects &operator=(DBObjects &&r) noexcept {
            if (this != &r) {
                objectName = std::move(r.objectName);
                objectType = std::move(r.objectType);
                createSQL = std::move(r.createSQL);
            }
            return *this;
        }
    };

#ifdef __USE_MSSQL__
    class Base {
        //		DBInitializer *initializer;
        std::vector<SQLCommand::Simple *> commandList;

    protected:
        DBServer::MsSQL *ds;
        WorkUnit::MsSQLSession *session;
        bool isOpened;

        std::string _dbName;
        std::string _userID;
        std::string _password;
        std::string _serverName;
        std::string _masterName;   // for some view <MASTER>
        std::string _siblingName;  // for some view <SIBLING>

        bool _useIntegratedSecurity;
        bool _dropAllObjects;
        virtual std::vector<DBObjects> GetObjectList() { return std::vector<DBObjects> {}; }
        virtual void LoadDBAttributes() {}
        virtual bool Create();

    public:
        Base(const std::string &serverName, const std::string &dbName);
        virtual ~Base();
        bool IsOpened() { return isOpened && ds; }
        void Close();
        void SetUser(const std::string &userId, const std::string &password);
        void ReCreateObjects() { _dropAllObjects = true; }
        void SetRelatedDB(const std::string &master, const std::string &sibling) {
            _masterName = master;
            _siblingName = sibling;
        }
        virtual void Open(bool checkAndCreate = true);
        static void Initialize();
        static void UnInitialize();
        void RegisterCommand(SQLCommand::Simple *p) {
            commandList.emplace_back(p);
            if (!p->IsPrepared()) p->Prepare();
        }
        void CancelCommand(DWORD tid) {
            if (session) session->CancelCommand(tid);
        }
        virtual DBServer::MsSQL *GetServer() { return ds; }
        virtual WorkUnit::Session *GetSession() { return session; }
        IDBInitialize *GetIDBInitialize() { return ds->pIDBInitialize; }
        const std::string GetDBName() const { return _dbName; }
        const std::string GetServerName() const { return _serverName; }
        const std::string GetUserID() const { return _userID; }
        const std::string GetPassword() const { return _password; }
        bool IsDataBaseExist(const std::string &s, bool toCreate = false);
        virtual void PopulateTables() {}
    };
#endif

    class UserDBRegistry;
    class SQLiteBase;

#ifndef NO_REPORT
#ifndef NO_XLS
    typedef libxl::Sheet *(SQLiteBase::*XLSGeneratorFunction)(ExcelReader *xlr, libxl::Sheet *sheet, std::shared_ptr<ReportPDF> rep, std::shared_ptr<wpSQLResultSet> rs, wxJSONValue &param, bool freezeHeader);
#else
    typedef void (SQLiteBase::*XLSGeneratorFunction)(std::shared_ptr<ReportPDF> rep, std::shared_ptr<wpSQLResultSet> rs, wxJSONValue &param, bool freezeHeader);
#endif
#endif
    class SQLiteBase {
        std::string _empty;
        // these two is used for computing sequence no
    public:
        struct GetSequence {
            int64_t seqNo;
            std::string prevNo;

        public:
            GetSequence(int64_t ll, const std::string &v) : seqNo(ll), prevNo(v) {}
        };
        GetSequence getSequence;
        bool turnOffSynchronize;
        bool exclusiveMode;
        bool usingWAL;
        bool journalOff;
        std::shared_ptr<UserDBRegistry> userDBregistry;
        std::string logMessage;

        static std::string FormatNumber(double value, int nDec = 2);

    protected:
        bool _dropAllObjects;
        std::string _masterName;   // for some view <MASTER>
        std::string _siblingName;  // for some view <SIBLING>
        std::string _dbName;
        std::string _pathName;
        std::string mainDBname;
        bool isAggregatedDB;

        std::shared_ptr<wpSQLDatabase> db;
        bool dropAllObjects;
        bool isNewDatabase;
        virtual std::vector<DBObjects> objectList() const { return std::vector<DBObjects> {}; }  // returns a copy of emptyObject;
        virtual bool Create();

    private:
        void CreateObject(const std::string &f, const DB::ObjectType ty, const std::vector<std::string> crSQL, bool dropIfExist = false, const std::string &replMaster = "", const std::string &replSibling = "");
        void CreateAllObjects(bool checkAndCreate, bool toExecuteCommand);

    public:
        SQLiteBase();
        SQLiteBase(const std::string &dbName, bool turnOffSynchronize = false, bool exclusiveMode = false, bool usingWal = true, bool journalOff = false);
        virtual ~SQLiteBase() {
            userDBregistry.reset();
            if (IsOpened()) Close();
        }
        virtual bool IsSynchAble() { return false; }
        void ResetRegistry() { userDBregistry.reset(); }
        virtual std::shared_ptr<DB::UserDBRegistry> GetRegistry();

        bool IsOpened() { return db ? db->IsOpen() : false; }
        bool IsNewDatabase() { return isNewDatabase; }
        virtual void Close();
        void SetExclusiveMode() { exclusiveMode = true; }
        void SetUser(const std::string &, const std::string &);
        void ReCreateObjects() { dropAllObjects = true; }
        void SetRelatedDB(const std::string &master, const std::string &sibling) {
            _masterName = master;
            _siblingName = sibling;
        }
        bool DeleteDB();
        void TruncateAllTables();
        void DropAllTables();
        bool BackupDB(int noOfBackupToKeep, std::string folderName, std::function<bool()> fnIsStopping) const;

        // wxLongLong InsertIfNotExists(wpSQLStatement &sttFind, wpSQLStatement &sttInsert, long id, const std::string &code, const std::string &name);
        // wxLongLong InsertIfNotExists(wpSQLStatement &sttFind, wpSQLStatement &sttInsert, const std::string &code, const std::string &name);
        wpSQLDatabase &GetSession() { return *db.get(); }
        std::shared_ptr<wpSQLDatabase> Get() { return db; }
        const std::string GetServerName() const { return "local"; }

        virtual void Open(bool checkAndCreate = false);
        virtual void OpenWithoutSession() { Open(true); }
        virtual void CheckSchemaAndRestructure();
        virtual void PopulateTables() {}
        virtual void CheckStructure() {}
        virtual void Initialize() {}
        virtual void InitFunction();
        virtual void InitializeLocalVariables() {}
        virtual void RestructureTable(const std::string &tabName, std::unordered_map<std::string, std::string> transformColumn = std::unordered_map<std::string, std::string>());
        virtual void RestructureTable(const std::string &tabName, const DB::DBObjects &objSchema, std::unordered_map<std::string, std::string> transformColumn = std::unordered_map<std::string, std::string>());
        virtual int ProcessBusyHandler(int nTimesCalled);  // 0-nomore wait; non-zero continue wait;

        std::string GetAllRows(const std::string &tableName, const std::string &sql);
        std::string GetKeyValueJSON(const std::string &tableName, const std::string &sql);
        std::string GetJSON(const std::string &sql);

        virtual std::string GetDatabaseIdentity();
        static std::string GetDatabaseIdentity(const std::string &extraInfo, const std::string &m, const std::string &t) { return GetShortHashedString(fmt::format("{}~{}~{}", extraInfo, boost::to_upper_copy(m), boost::to_upper_copy(t))); }
        std::string GetActivationKey(const std::string &key) { return DB::GetActivationKey(GetDatabaseIdentity(), key); }
        std::string GetAccountingActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::accounting]); }
        std::string GetTicketingActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::ticketing]); }
        std::string GetCateringActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::catering]); }
        std::string GetOnlineActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::online]); }
        std::string GetOnlineMarketPlaceActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::onlineMarketPlace]); }
        std::string GetRentalActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::rental]); }
        std::string GetStockTrackingActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::stockTracking]); }
        std::string GetMyKadActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::mykad]); }
        std::string GetBackupActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::backup]); }
        std::string GetDispensingActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::dispensing]); }
        std::string GetPatientRecordActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::patientRecord]); }
        std::string Get_eInvoiceActivationKey() { return GetActivationKey(DB::softwareExtensionKey[DB::SoftwareExtensions::eInvoice]); }

#ifdef HAS_EXPIRY
        virtual void SetSoftwareExpiry(wxDateTime &) {}
        virtual void ResetSoftwareExpiry() {}
        virtual bool IsSoftwareExpired() {
            wxDateTime t = GetSoftwareExpiryDate();
            return t.IsValid() ? t < GetServerNow() : false;
        }
        virtual wxDateTime GetSoftwareExpiryDate() { return GetServerNow().Add(wxDateSpan(1)); }
#endif
        void CancelCommand() { db->Interrupt(); }
        const std::string GetDBName() const { return _dbName; }
        virtual const std::string GetTransactionDBName() const { return _empty; }
        virtual std::shared_ptr<TransactionDB> GetTransactionDB();
        const std::string GetDBPath() const { return _pathName; }
        void SetDBName(const std::string &n) { _dbName = n; }
        virtual bool IsDataBaseExist(const std::string &s, bool toCreate = false);

        bool IsTriggerExist(const std::string &fnName) { return IsTriggerExist(GetSession(), fnName); }
        bool IsViewExist(const std::string &viewName) { return IsViewExist(GetSession(), viewName); }
        bool IsTableExist(const std::string &fnName) { return IsTableExist(GetSession(), fnName); }
        bool IsTempTableExist(const std::string &tabName) { return IsTempTableExist(GetSession(), tabName); }
        bool IsIndexExist(const std::string &fnName) { return IsIndexExist(GetSession(), fnName); }
        bool IsPrimaryKeyExist(const std::string &fnName) { return IsPrimaryKeyExist(GetSession(), fnName); }
        bool IsColumnExist(const std::string &tabName, const std::string &colName) { return IsColumnExist(GetSession(), tabName, colName); }

        static bool IsTriggerExist(wpSQLDatabase &d, const std::string &fnName);
        static bool IsViewExist(wpSQLDatabase &d, const std::string &fnName);
        static bool IsTableExist(wpSQLDatabase &d, const std::string &fnName);
        static bool IsTempTableExist(wpSQLDatabase &d, const std::string &tabName);
        static bool IsIndexExist(wpSQLDatabase &d, const std::string &fnName);
        static bool IsPrimaryKeyExist(wpSQLDatabase &d, const std::string &fnName);
        static bool IsColumnExist(wpSQLDatabase &d, const std::string &tabName, const std::string &colName);
        static bool IsColumnExist(wpSQLDatabase &d, const std::string &aliasName, const std::string &tabName, const std::string &colName);

        virtual std::string Translate(const std::string &s) { return s; }
        virtual std::wstring Translate(const std::wstring &s) { return to_wstring(Translate(to_string(s))); }

        std::string GetDayName(int i);
        std::string GetMonthName(int i);

#ifndef NO_REPORT
        virtual DB::XLSGeneratorFunction GetProcessor(const std::string &cmdLine, wxJSONValue &param);
#ifndef NO_XLS
        virtual libxl::Sheet *CreateNewSheet(ExcelReader *xlr, int nCols, const std::string &sheetName, const std::string &title, const std::string &subTitle);
        virtual libxl::Sheet *AppendToExcelSheet(ExcelReader *xlr, libxl::Sheet *sheet, std::shared_ptr<ReportPDF>, std::shared_ptr<wpSQLResultSet> rs, wxJSONValue &param, bool freezeHeader);
        virtual libxl::Sheet *AppendToPDF(ExcelReader *xlsReader, libxl::Sheet *sheet, std::shared_ptr<ReportPDF> report, std::shared_ptr<wpSQLResultSet> rs, wxJSONValue &param, bool freezeHeader);
        virtual libxl::Sheet *AppendGroupingToExcelSheet(ExcelReader *xlr, libxl::Sheet *sheet, std::shared_ptr<ReportPDF>, std::shared_ptr<wpSQLResultSet> rs, wxJSONValue &param, bool freezeHeader);
        virtual std::string WriteToExcel(std::shared_ptr<wpSQLResultSet> rs, const std::string &title, const std::string &subTitle, const std::string &sheetName);
        virtual std::string WriteToExcel(std::vector<std::pair<std::shared_ptr<wpSQLResultSet>, std::vector<int>>> &rs, const std::string &title, const std::string &subTitle, const std::string &sheetName);
        // virtual void GetResultForXCelsius(XMLTag &tg, std::shared_ptr<wpSQLResultSet> rs, const std::string &variableName);
#else
        virtual void AppendToPDF(std::shared_ptr<ReportPDF> report, std::shared_ptr<wpSQLResultSet> rs, wxJSONValue &param, bool freezeHeader);
#endif
        virtual std::shared_ptr<ReportPDF> CreateNewPDF(std::shared_ptr<wpSQLResultSet> rs, const std::wstring &orientation, const std::wstring &sectionName, const std::wstring &title, const std::wstring &subTitle, wxJSONValue &param, const std::wstring outletName);
        virtual wxJSONValue GetResultInJSON(std::shared_ptr<wpSQLResultSet> rs, std::unordered_map<std::string, std::function<std::string(std::shared_ptr<wpSQLResultSet>)>> fn);
#endif
        virtual std::string GetResultTabDelimited(std::shared_ptr<wpSQLResultSet> rs, bool useActualTab = false, bool showColumnHeader = false) { return GetResultTabDelimited(rs, -1, useActualTab, showColumnHeader); }
        virtual std::string GetResultTabDelimited(std::shared_ptr<wpSQLResultSet> rs, int nRows, bool useActualTab = false, bool showColumnHeader = false, const std::string &filename = "");
#ifdef PPOS_DB
        int64_t GetReturnData(std::shared_ptr<wpSQLResultSet> rs, PPOS::ReturnData *result, ConvertFunction fnConvert = [](int, const std::string s) { return s; });
#endif
        std::vector<std::vector<std::string>> GetVectorResult(std::shared_ptr<wpSQLResultSet> rs, int nRows = -1, bool showColumnHeader = false);
    };

    class UserDBRegistry {
        friend class SQLiteBase;
        SQLiteBase *sqlDB;
#ifdef __WX__
        std::shared_ptr<TableUpdateTimeSetter> syncUpdater;
        std::shared_ptr<TableUpdateTimeSetter> CreateSyncUpdater();
#endif

        std::unordered_map<std::string, int> autoLengthMap;
        std::shared_ptr<wpSQLStatement> GetUpdateKeyStatement(), GetInsertKeyStatement();
        std::shared_ptr<wpSQLStatement> GetLocalUpdateKeyStatement(), GetLocalInsertKeyStatement();
        std::function<std::shared_ptr<wpSQLStatement>()> fn_sttFindLocalKey;
        std::function<std::shared_ptr<wpSQLStatement>()> fn_sttFindKey;
        std::function<std::shared_ptr<wpSQLStatement>()> fn_sttEraseLocalKey;
        std::function<std::shared_ptr<wpSQLStatement>()> fn_sttEraseKey;

    public:
        bool IsActivated(const std::string &key);
        UserDBRegistry(SQLiteBase *db);
        virtual ~UserDBRegistry() {}
#ifdef __WX__
        std::shared_ptr<TableUpdateTimeSetter> GetSyncUpdater();
#endif
        template<typename T = std::string>
        void SetKey(const std::string &key, const T &val) {
            if (sqlDB->IsTableExist("ul_localkeys")) {
                if (FindLocalKey(key)) {
                    SetLocalKey(key, val);
                    return;
                }
            }
            std::shared_ptr<wpSQLStatement> stt = fn_sttFindKey();
            if (!stt) return;
#ifdef __WX__
            std::shared_ptr<TableUpdateTimeSetter> sy = GetSyncUpdater();
#endif
            stt->Bind(1, key);
            std::shared_ptr<wpSQLResultSet> rs = stt->ExecuteQuery();
            if (rs->NextRow()) {
                std::shared_ptr<wpSQLStatement> sttUpdateKey = GetUpdateKeyStatement();
                sttUpdateKey->Bind(1, val);
                sttUpdateKey->Bind(2, key);
                sttUpdateKey->ExecuteUpdate();
#ifdef __WX__
                sy->SetUpdateDate(rs->Get<std::string>(0));
#endif
            } else {
                std::shared_ptr<wpSQLStatement> sttInsertKey = GetInsertKeyStatement();
                sttInsertKey->Bind(1, key);
                sttInsertKey->Bind(2, val);
                sttInsertKey->ExecuteUpdate();
                auto id = sqlDB->GetSession().GetLastRowId<std::string>();
#ifdef __WX__
                sy->SetUpdateDate(id);
#endif
            }
        }
        virtual void EraseKey(const std::string &key);

        template<typename T = std::string>
        void SetLocalKey(const std::string &key, const T &val) {
            try {
                std::shared_ptr<wpSQLStatement> stt = fn_sttFindLocalKey();
                if (!stt) return;
                stt->Bind(1, key);
                std::shared_ptr<wpSQLResultSet> rs = stt->ExecuteQuery();
                if (rs->NextRow()) {
                    std::shared_ptr<wpSQLStatement> sttUpdateLocalKey = GetLocalUpdateKeyStatement();
                    sttUpdateLocalKey->Bind(1, val);
                    sttUpdateLocalKey->Bind(2, key);
                    sttUpdateLocalKey->ExecuteUpdate();
                } else {
                    std::shared_ptr<wpSQLStatement> sttInsertLocalKey = GetLocalInsertKeyStatement();
                    sttInsertLocalKey->Bind(1, key);
                    sttInsertLocalKey->Bind(2, val);
                    sttInsertLocalKey->ExecuteUpdate();
                }
            } catch (wpSQLException &e) {
                ShowLog(e.message);
            } catch (...) {
                ShowLog("???");
            }
        }

        virtual void EraseLocalKey(const std::string &key);

        template<typename T = std::string>
        T GetKey(const std::string &key, const T &defaultKey = {}) {
            T res {};
            if (FindLocalKey(key, res)) return res;
            // res is empty at this point.
            if (!sqlDB->IsTableExist("ul_keys")) return defaultKey;
            std::shared_ptr<wpSQLStatement> stt = fn_sttFindKey();
            stt->Bind(1, key);

            std::shared_ptr<wpSQLResultSet> rs = stt->ExecuteQuery();
            if (rs->NextRow())
                return rs->Get<T>(1);
            if (defaultKey != res)  // not empty
                SetKey(key, defaultKey);
            return defaultKey;
        }
        template<typename T = std::string>
        T GetLocalKey(const std::string &key, const T &defaultKey = {}) {
            if (sqlDB->IsTableExist("ul_localkeys")) {
                std::shared_ptr<wpSQLStatement> stt = fn_sttFindLocalKey();
                stt->Bind(1, key);
                std::shared_ptr<wpSQLResultSet> rs = stt->ExecuteQuery();
                if (rs->NextRow())
                    return rs->Get<T>(1);
                if (defaultKey != T {})  // not empty
                    SetLocalKey(key, defaultKey);
            }
            return defaultKey;
        }

        template<typename T>
        std::vector<T> GetStringList(const T &key) {
            std::vector<T> res;
            std::string t = GetKey(to_string(key));
            if (t.empty()) return res;
            boost::tokenizer<boost::char_separator<char>> tok(t, boost::char_separator<char>("\n;", "", boost::keep_empty_tokens));
            for (auto &x : tok) {
                if constexpr (std::is_same_v<T, std::wstring>)
                    res.emplace_back(to_wstring(x));
                else
                    res.emplace_back(x);
            }
            return res;
        }

        template<typename T>
        std::vector<T> GetLocalStringList(const T &key) {
            std::vector<T> res;
            std::string t = GetLocalKey(to_string(key));
            if (t.empty()) return res;
            boost::tokenizer<boost::char_separator<char>> tok(t, boost::char_separator<char>("\n;", "", boost::keep_empty_tokens));
            for (auto &x : tok) {
                if constexpr (std::is_same_v<T, std::wstring>)
                    res.emplace_back(to_wstring(x));
                else
                    res.emplace_back(x);
            }
            return res;
        }

        // std::vector<std::string> GetStringList(const std::string &key);
        // std::vector<std::wstring> GetStringList(const std::wstring &key);
        // std::vector<std::string> GetLocalStringList(const std::string &key);
        // std::vector<std::wstring> GetLocalStringList(const std::wstring &key);

        bool FindLocalKey(const std::string &key);
        template<typename T>
        bool FindLocalKey(const std::string &key, T &res) {
            if (!sqlDB->IsTableExist("ul_localkeys")) return false;
            std::shared_ptr<wpSQLStatement> stt = fn_sttFindLocalKey();
            stt->Bind(1, key);

            res = T {};
            std::shared_ptr<wpSQLResultSet> rs = stt->ExecuteQuery();
            if (rs->NextRow()) {
                res = rs->Get<T>(1);
                return true;
            }
            return false;
        }

        bool IsKeyExists(const std::string &key, bool isLocalKey = false);
    };

    class Inserter {
        wpSQLDatabase &db;
        std::shared_ptr<wpSQLStatement> find, insert, getLastID, countThisID;

    public:
        Inserter(wpSQLDatabase &db, const std::string &findSQL, const std::string &insertSQL, const std::string &countLastIDsql = "", const std::string &getLastIDsql = "");
        int64_t InsertIfNotExists(int64_t id, const std::string &code, const std::string &name);
        int64_t InsertIfNotExists(const std::string &code, const std::string &name);
    };

    class TypeRegistry {
    protected:
        std::shared_ptr<DB::UserDBRegistry> reg;
        std::shared_ptr<wpSQLStatement> sttFindGroup, sttInsGroup;
        std::shared_ptr<wpSQLStatement> stt, insStt;
        DB::SQLiteBase &d;

    public:
        TypeRegistry(DB::SQLiteBase &db);
        std::string SetGroup(const std::string &regKey, const std::string &name);
        std::string Set(const std::string &regKey, const std::string &code, const std::string &name, const std::string &limit, const std::string &groupID, bool &isNew);
        std::string Set(const std::string &regKey, const std::string &code, const std::string &name, const std::string &limit, const std::string &groupID) {
            bool isNew;
            return Set(regKey, code, name, limit, groupID, isNew);
        }
        std::string GetValue(const std::string &regKey, const std::string &colName);
        std::string GetLimitValue(const std::string &key) { return GetValue(key, "limitvalue"); }
        std::string GetDefaultValue(const std::string &key) { return GetValue(key, "defaultvalue"); }

        void SetValue(const std::string &regKey, const std::string &colName, const std::string &value);
        void SetDefaultValue(const std::string &key, const std::string &value) { SetValue(key, "defaultvalue", value); }
        void SetLimitValue(const std::string &key, const std::string &value) { SetValue(key, "limitvalue", value); }
    };

}  // namespace DB

class TransactionDB : public DB::SQLiteBase {
protected:
    std::vector<DB::DBObjects> objectList() const override;
    DB::SQLiteBase *masterDB;

public:
    TransactionDB(const std::string &dbName, DB::SQLiteBase *mstr) : DB::SQLiteBase(dbName),
                                                                     masterDB(mstr) {}
    virtual ~TransactionDB() { Close(); }
    std::string GetMasterDBName() { return masterDB->GetDBName(); }

    void CheckStructure() override { DB::SQLiteBase::CheckStructure(); }
    void InitializeLocalVariables() override { DB::SQLiteBase::InitializeLocalVariables(); }
    void Initialize() override { DB::SQLiteBase::Initialize(); }
    virtual bool Migrate(DB::SQLiteBase *d, std::vector<std::string> &tableList);
    bool AttachMasterDB();
    bool DetachMasterDB();
    bool IsSynchAble() override { return true; }
};

void CreateNonExistingFolders(const std::string &fileName);
