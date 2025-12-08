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
#include <wx/filefn.h>

#include "global.h"
#endif
#include "rDb.h"
#include "logger.h"
#include <filesystem>
namespace fs = std::filesystem;

bool NeedDivision(const std::string &colName) {
    static const std::vector<std::string> exceptionList {"qoh", "reorderqty", "reorderlevel", "qty", "quantity", "packsize"};
    for (auto &x : exceptionList) {
        if (boost::icontains(colName, x)) return false;
    }
    return true;
}

void DB::SQLiteBase::RestructureTable(const std::string &tabName, const DB::DBObjects &schema, std::unordered_map<std::string, std::string> transformColumn) {
    // deleting all indexes, because the new table name with use the same index name; 0=id, 1=indexname
    try {
        std::vector<std::string> commandDropIndex;
        GetSession().Execute(fmt::format("pragma index_list({})", tabName), [&](int /*argc*/, char **data, char ** /*colNames*/) {
            commandDropIndex.emplace_back(fmt::format("drop index if exists {}", data[1]));
        });

        std::string oldName = DB::concat(tabName, "_old");

        GetSession().ExecuteUpdate(fmt::format("drop table if exists {}_old", tabName));
        GetSession().ExecuteUpdate(fmt::format("alter table {} rename to {}", tabName, oldName));

        for (auto x : commandDropIndex) {
            GetSession().ExecuteUpdate(x);
        }
        CreateObject(schema.objectName, schema.objectType, schema.createSQL, _dropAllObjects, _masterName, _siblingName);

        std::shared_ptr<wpSQLResultSet> rss = GetSession().ExecuteQuery(fmt::format("pragma table_info({})", tabName));
        std::string insColList, selColList;
        std::string delim;
        while (rss->NextRow()) {
            std::string colName = boost::to_lower_copy(rss->Get(1));
            if (!IsColumnExist(oldName, colName)) continue;  // not exist in old one
            insColList.append(delim);
            insColList.append(colName);
            selColList.append(delim);
            auto it = transformColumn.find(colName);
            if (it != transformColumn.end())
                selColList.append(it->second);
            else
                selColList.append(colName);
            delim = ",";
        }
        {
            std::string insSQL = fmt::format("insert into {}({}) select {} from {}", tabName, insColList, selColList, oldName);
            GetSession().ExecuteUpdate(insSQL);
            GetSession().ExecuteUpdate("drop table " + oldName);
        }
    } catch (wpSQLException &e) {
        ShowLog(fmt::format("RestructureTable: error = {}", e.message));
    } catch (std::exception &e) {
        ShowLog(fmt::format("RestructureTable: error = {}", e.what()));
    }
}

std::vector<std::string> tableToDrop {};

void DB::SQLiteBase::RestructureTable(const std::string &tabName, std::unordered_map<std::string, std::string> transformColumn) {
    // deleting all indexes, because the new table name with use the same index name; 0=id, 1=indexname

    std::vector<std::string> commandDropIndex;
    GetSession().Execute(fmt::format("pragma index_list({})", tabName), [&](int /*argc*/, char **data, char ** /*colNames*/) {
        commandDropIndex.push_back(fmt::format("drop index if exists {}", data[1]));
    });

    std::string oldName = DB::concat(tabName, "_old");
    GetSession().ExecuteUpdate(fmt::format("drop table if exists {}_old", tabName));
    GetSession().ExecuteUpdate(fmt::format("alter table {} rename to {}", tabName, oldName));

    for (auto x : commandDropIndex) {
        GetSession().ExecuteUpdate(x);
    }
    // creating new table;
    std::vector<DBObjects> list = objectList();
    for (auto const &p : list) {
        if (p.objectType == DB::EOT) break;
        if (boost::iequals(tabName, p.objectName)) {
            if (p.objectType == DB::Command) {
                CreateObject(p.objectName, p.objectType, p.createSQL, false, _masterName, _siblingName);
            } else
                CreateObject(p.objectName, p.objectType, p.createSQL, _dropAllObjects, _masterName, _siblingName);
            break;
        }
    }

    std::shared_ptr<wpSQLResultSet> rss = GetSession().ExecuteQuery(fmt::format("pragma table_info({})", tabName));
    std::string insColList, selColList;
    std::string delim;
    while (rss->NextRow()) {
        std::string colName = boost::to_lower_copy(rss->Get(1));
        if (!IsColumnExist(oldName, colName)) continue;  // not exist in old one
        insColList.append(delim);
        insColList.append(colName);
        selColList.append(delim);
        auto it = transformColumn.find(colName);
        if (it != transformColumn.end())
            selColList.append(it->second);
        else
            selColList.append(colName);
        delim = ",";
    }

    std::string insSQL = fmt::format("insert into {}({}) select {} from {}", tabName, insColList, selColList, oldName);
    GetSession().ExecuteUpdate(insSQL);
    tableToDrop.push_back(oldName);
}

void DB::SQLiteBase::CheckSchemaAndRestructure() {
    try {
        ShowLog(fmt::format("CheckSchemaAndRestructure: checking {}", GetDBName()));
        std::string tempDBName = ":memory:";
        std::string selectTableList = "select name from sqlite_master where type='table' order by name";  // check only tables exist in current. The extra will auto-create with correct struct
        
        std::vector<DBObjects> schemaList = objectList();

        DB::SQLiteBase temp(tempDBName);
        temp.Open(false);
        for (auto const &p : schemaList) {
            temp.CreateObject(p.objectName, p.objectType, p.createSQL, false, temp._masterName, temp._siblingName);
        }
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> tableToChange;
        GetSession().Execute(selectTableList, [&](int, char **colValue, char **) {
            std::unordered_map<std::string, std::string> currStruct, newStruct;
            std::string tabName = boost::to_lower_copy(std::string(colValue[0]));
            
            GetSession().Execute("pragma table_info(" + tabName + ")", [&](int, char **v, char **) {
                std::string x = boost::to_lower_copy(std::string(v[1]));
                currStruct[x] = v[2];
            });
            temp.GetSession().Execute("pragma table_info(" + tabName + ")", [&](int, char **v, char **) {
                std::string x = boost::to_lower_copy(std::string(v[1]));
                newStruct[x] = v[2];
            });

            if (newStruct.empty()) return;
            bool toRebuild = currStruct.size() != newStruct.size();
            std::unordered_map<std::string, std::string> colMap;
            for (auto it = currStruct.begin(); it != currStruct.end(); it++) {
                auto nit = newStruct.find(it->first);
                if (nit == newStruct.end()) {
                    toRebuild = true;
                } else if (!boost::iequals(nit->second, it->second)) {                                   // not same type. int->real ?
                    if (boost::iequals(it->second, "integer") && boost::iequals(nit->second, "real")) {  // from int to real
                        toRebuild = true;
                        if (NeedDivision(it->first))
                            colMap[it->first] = fmt::format("cast({} as real)/10000.0", it->first);
                        else
                            colMap[it->first] = it->first;
                    } else
                        colMap[it->first] = it->first;
                } else
                    colMap[it->first] = it->first;
            }
            if (toRebuild) tableToChange[tabName] = colMap;
        });
        Close();
        Open(false);
        int nChanged = 0;
        for (const auto &x : tableToChange) {
            auto it = std::find_if(schemaList.begin(), schemaList.end(), [&](const DB::DBObjects &o) {
                return boost::iequals(o.objectName, x.first);
            });
            if (it != schemaList.end()) {
                nChanged++;
                RestructureTable(x.first, *it, x.second);
                if (boost::iequals(x.first, "SalesDetail")) {
                    GetSession().ExecuteUpdate("update salesdetail set priceb4disc=price where priceb4disc is null or priceb4disc=0");
                }
            }
        }
        Close();
        Open(false);
        int nDropped {0};
        for (const auto &tabName : tableToDrop) {
            try {
                //ShowLog("Dropping..." + tabName);
                GetSession().ExecuteUpdate(fmt::format("drop table {}", tabName));
                nDropped++;
            } catch (wpSQLException &e) {
                ShowLog("Failed to drop " + tabName + e.message);
            } catch (std::exception &e) {
                ShowLog(std::string("Failed to drop ") + tabName + e.what());
            }
        }
        if (nDropped > 0) {
            GetSession().Vacuum();
        }
        tableToDrop.clear();
        ShowLog(fmt::format("CheckSchemaAndRestructure: {} DONE. No of tables changed: {}, no of tables dropped: {}", GetDBName(), nChanged, nDropped));
    }
    catch (const std::exception& e) {
        ShowLog(fmt::format("CheckSchemaAndRestructure caught std::exception: {}", e.what()));
    }
}
