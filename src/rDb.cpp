#include "precompiled/libcommon.h"
#ifdef _WIN32
#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>
#endif

#ifdef _WIN32
#include "winsock2.h"
#endif

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

#include "wx/file.h"
#include "wx/filename.h"
#include "wx/dir.h"
#include "wx/xml/xml.h"
#include "wx/tokenzr.h"

#include <unordered_set>
#include <iostream>
#include <fstream>
#include "global.h"
#include "words.h"
#include "rDb.h"
#include "ExcelReader.h"
#include "xmlParser.h"
#else
#include <unordered_set>
#include <iostream>
#include <fstream>
#include "rDb.h"
#endif

#include <filesystem>

#ifdef __USE_MSSQL__
void COM_Init() {
    CoInitializeEx(NULL, COINIT_MULTITHREADED);
}

void COM_UnInit() {
    CoUninitialize();
}

void DB::Base::Initialize() { CoInitializeEx(NULL, COINIT_MULTITHREADED); }

void DB::Base::UnInitialize() { CoUninitialize(); }

bool DB::Base::IsDataBaseExist(const wxString &dbName, bool toCreate) {
    DBServer::MsSQL *dsMaster = new DBServer::MsSQL(_serverName, wxT("master"));
    if (!dsMaster) throw SQLException::rException(wxT("cannot allocate memory"));
    if (_useIntegratedSecurity)
        dsMaster->UseIntegratedSecurity();
    else {
        dsMaster->SetUser(_userID);
        dsMaster->SetPassword(_password);
    }
    dsMaster->Open();

    WorkUnit::MsSQLSession *s = new WorkUnit::MsSQLSession(*dsMaster);
    if (!s) {
        delete dsMaster;
        dsMaster = NULL;
        throw SQLException::rException(wxT("cannot start session"));
    }
    //	s->toTraceCommand = true;
    bool retval = s->IsDatabaseExist(dbName);
    if (!retval && toCreate) {
        wxString buf;
        buf.Printf(wxT("CREATE DATABASE [%s]"), _dbName);
        s->Execute(buf);
    }
    delete s;
    s = NULL;
    dsMaster->Close();
    delete dsMaster;
    dsMaster = NULL;
    return retval;
}

bool DB::Base::Create() {
    return !IsDataBaseExist(_dbName, true);
}

DB::Base::Base(const wxString &serverName, const wxString &dbName) : _serverName(serverName),
                                                                     _dbName(dbName) {
    isOpened = false;
    ds = new DBServer::MsSQL(_serverName, dbName);
    if (!ds) throw SQLException::rException(wxT("cannot intantiate DBServer::MsSQL"));
    _useIntegratedSecurity = true;
    _dropAllObjects = false;
    session = NULL;
}

DB::Base::~Base() {
    std::vector<SQLCommand::Simple *>::iterator it = commandList.begin();
    for (; it != commandList.end(); it++) {
        SQLCommand::Simple *s = *it;
        delete s;
    }
    if (session) {
        delete session;
        session = NULL;
    }
    if (isOpened && ds)
        ds->Close(false);
    if (ds) delete ds;
    ds = NULL;
}

void DB::Base::Close() {
    if (session) delete session;
    if (ds) delete ds;
    isOpened = false;
    session = NULL;
    ds = NULL;
}

void DB::Base::SetUser(const wxString &userId, const wxString &password) {
    _userID = userId;
    _password = password;
    _useIntegratedSecurity = false;
    ds->SetUser(userId);
    ds->SetPassword(password);
}

void DB::Base::Open(bool checkAndCreate) {
    bool toExecuteCommand = Create();

    if (_useIntegratedSecurity) ds->UseIntegratedSecurity();

    ds->Open();
    isOpened = true;
    std::vector<DB::DBObjects> objList = objectList();

    session = new WorkUnit::MsSQLSession(*ds);
    if (!session) throw SQLException::rException(wxT("cannot allocate memory for session"));

    //	session->toTraceCommand = true;
    if (checkAndCreate || toExecuteCommand) {
        for (auto p : objList) {
            // if (p.objectType == WorkUnit::EOT) break;
            if (p.objectType == WorkUnit::Command) {
                if (toExecuteCommand) session->CreateObject(p.objectName, p.objectType, p.createSQL, false, _masterName, _siblingName);
            } else
                session->CreateObject(p.objectName, p.objectType, p.createSQL, _dropAllObjects, _masterName, _siblingName);
        }
        if (toExecuteCommand) PopulateTables();  // this is done if the new table is created.
    }
    LoadDBAttributes();
    if (!session) throw SQLException::rException(wxT("session is NULL"));
    _dropAllObjects = false;
    //	session->toTraceCommand = false;
}
#endif

void LogStrFile(const std::string &filename, const std::string &content) {
    std::ofstream out(filename);
    out << content;
}

void CreateNonExistingFolders(const std::string &filename) {
    namespace fs = std::filesystem;
    fs::path path {filename};
    fs::create_directories(path.parent_path());
}

DB::TypeRegistry::TypeRegistry(DB::SQLiteBase &db)
  : reg(db.GetRegistry()),
    d(db) {
    sttInsGroup = d.GetSession().PrepareStatement("insert into types(name) values(?)");
    sttFindGroup = d.GetSession().PrepareStatement("select * from types where name=? and parentid is null");
    stt = d.GetSession().PrepareStatement("select id from types where code=? and name=? and (parentid=? or parentid is null)");
    if (d.IsColumnExist("types", "limitvalue"))
        insStt = d.GetSession().PrepareStatement("insert into types(code, name, parentid, limitvalue) values(?,?,?,?)");
    else {
        d.GetSession().ExecuteUpdate("alter table types add llv_temp text");
        insStt = d.GetSession().PrepareStatement("insert into types(code, name, parentid, llv_temp) values(?,?,?,?)");
    }
}

std::string DB::TypeRegistry::SetGroup(const std::string &regKey, const std::string &name) {
    sttFindGroup->Bind(1, name);
    std::shared_ptr<wpSQLResultSet> rs = sttFindGroup->ExecuteQuery();
    std::string id;
    if (rs->NextRow()) {
        auto v = reg->GetKey(regKey);
        if (!v.empty()) return v;
        id = rs->Get(0);
    }
    if (id.empty()) {
        auto _x = d.GetSession().GetAutoCommitter();
        sttInsGroup->Bind(1, name);
        sttInsGroup->ExecuteUpdate();
        id = d.GetSession().GetLastRowId<std::string>();
        _x->SetOK();
    }
    reg->SetKey(regKey, id);
    return id;
}

std::string DB::TypeRegistry::GetValue(const std::string &key, const std::string &colName) {
    std::shared_ptr<wpSQLStatement> sttSelect = d.GetSession().PrepareStatement(fmt::format("select {} from Types where id=?", colName));
    sttSelect->Bind(1, reg->GetKey(key));
    std::shared_ptr<wpSQLResultSet> rs = sttSelect->ExecuteQuery();
    if (rs->NextRow())
        return rs->Get(0);
    return "";
}

void DB::TypeRegistry::SetValue(const std::string &key, const std::string &colName, const std::string &value) {
    auto stt = d.GetSession().PrepareStatement(fmt::format("update Types set {}=@value where id=@id", colName));
    stt->Bind("@id", reg->GetKey(key));
    stt->Bind("@value", value);
    stt->ExecuteUpdate();
}

std::string DB::TypeRegistry::Set(const std::string &regKey, const std::string &code, const std::string &name, const std::string &limit, const std::string &group, bool &isNew) {
    std::string id;
    stt->Bind(1, code);
    stt->Bind(2, name);
    stt->Bind(3, group);
    isNew = false;
    std::shared_ptr<wpSQLResultSet> rs = stt->ExecuteQuery();
    if (rs->NextRow()) {
        id = rs->Get(0);
    } else {
        insStt->Bind(1, code);
        insStt->Bind(2, name);
        insStt->Bind(3, group);
        insStt->Bind(4, limit);
        insStt->ExecuteUpdate();
        id = d.GetSession().GetLastRowId<std::string>();
        isNew = true;
    }
    if (reg && !regKey.empty())
        reg->SetKey(regKey, id);
    return id;
}
