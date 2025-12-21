#include <unordered_set>
#include <iostream>
#include <fstream>
#include "rDb.h"
#include <filesystem>

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
