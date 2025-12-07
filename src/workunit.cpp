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


#include "wx/wxprec.h"
#ifndef WX_PRECOMP
#include "wx/wx.h"
#endif
#include "wx/tokenzr.h"

#include "global.h"
#include "words.h"
#include "sqlexception.h"
#include "workunit.h"

#if defined(_WIN32) && defined(__MSVC__)
#include "command.h"
#include "rowset.h"
#include <atlbase.h>
#include <stdio.h>

/*std::ofstream* WorkUnit::Session::fOut = NULL;

void WorkUnit::Session::TraceStart(const wxString &s) {
	if (fOut == NULL) return;
	if (cmdTracer != NULL) delete cmdTracer;
	cmdTracer = new Tools::Tracer( fOut, s );
	if (!cmdTracer) throw SQLException::rException(wxT("cannot allocate memory"));

}

void WorkUnit::Session::TraceStop() {
	if (cmdTracer != NULL) {
		delete cmdTracer;
		cmdTracer=NULL;
	}
}
*/

WorkUnit::Session::Session(DBServer::Base &ds) {  //:toTraceCommand(false), tracer(fOut, "Session") {
                                                  //	cmdTracer=NULL;
    HRESULT hr = ds.pIDBInitialize->QueryInterface(IID_IDBCreateSession, (void **)&pIDBCreateSession);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(ds.pIDBInitialize, IID_IDBInitialize);
        throw SQLException::rException(wxString::Format(wxT("Session: Cannot create session: %s"), err));
    }
    hr = pIDBCreateSession->CreateSession(NULL, IID_IDBCreateCommand, (IUnknown **)&pIDBCreateCommand);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pIDBCreateSession, IID_IDBCreateSession);
        throw SQLException::rException(wxString::Format(wxT("rSession: Cannot create session: %s"), err));
    }
    hr = pIDBCreateCommand->QueryInterface(IID_ITransactionLocal, (void **)&pITransactionLocal);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pIDBCreateCommand, IID_ITransactionLocal);
        throw SQLException::rException(wxString::Format(wxT("rSession: Cannot query ITransactionLocal: %s"), err));
    }
}

WorkUnit::Session::~Session() {
    mtxCommandSet.Lock();
    for (auto const &itd : commandSet) {
        for (auto const &it : itd.second) {
            ICommand *pIcmd = it;
            pIcmd->Cancel();
            //			pIcmd->Release();
        }
    }
    mtxCommandSet.Unlock();
}

// commandSet should be locked by thread!
void WorkUnit::Session::CancelCommand(DWORD tid) {
    mtxCommandSet.Lock();
    std::unordered_map<DWORD, WorkUnit::CommandDeque>::iterator itd = commandSet.find(tid);
    if (itd != commandSet.end()) {
        for (auto const &it : itd->second) {
            ICommand *pIcmd = it;
            pIcmd->Cancel();
            //			pIcmd->Release();
        }
    }
    //	commandSet.clear();
    mtxCommandSet.Unlock();
}

void WorkUnit::Session::RegisterCommand(ICommand *c) {
    mtxCommandSet.Lock();
    DWORD tid = wxThread::GetCurrentId();
    std::unordered_map<DWORD, WorkUnit::CommandDeque>::iterator itd = commandSet.find(tid);
    if (itd != commandSet.end()) {
        itd->second.emplace_back(c);
    } else {
        WorkUnit::CommandDeque xc;
        std::pair<std::unordered_map<DWORD, WorkUnit::CommandDeque>::iterator, bool> i = commandSet.insert(std::pair<DWORD, WorkUnit::CommandDeque>(tid, xc));
        WorkUnit::CommandDeque &x = i.first->second;
        x.emplace_back(c);
    }
    mtxCommandSet.Unlock();
}

void WorkUnit::Session::DeRegisterCommand() {
    mtxCommandSet.Lock();
    DWORD tid = wxThread::GetCurrentId();
    std::unordered_map<DWORD, WorkUnit::CommandDeque>::iterator itd = commandSet.find(tid);
    if (itd != commandSet.end()) {
        if (itd->second.size() > 0)
            itd->second.pop_back();
    }
    mtxCommandSet.Unlock();
}

void WorkUnit::Session::BeginTransaction() {
    if (pITransactionLocal == NULL) return;
    ULONG tLevel;
    HRESULT hr = pITransactionLocal->StartTransaction(ISOLATIONLEVEL_READCOMMITTED, 0, NULL, &tLevel);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pITransactionLocal, IID_ITransactionLocal);
        throw SQLException::rException(wxString::Format(wxT("rSession: Cannot create transaction: %s"), err));
    }
}

void WorkUnit::Session::CommitTransaction() {
    if (pITransactionLocal == NULL) return;
    HRESULT hr = pITransactionLocal->Commit(FALSE, XACTTC_SYNC, 0);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pITransactionLocal, IID_ITransactionLocal);
        throw SQLException::rException(wxString::Format(wxT("rSession: Cannot commit transaction: %s"), err));
    }
}

void WorkUnit::Session::AbortTransaction() {
    if (pITransactionLocal == NULL) return;
    HRESULT hr = pITransactionLocal->Abort(NULL, FALSE, FALSE);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pITransactionLocal, IID_ITransactionLocal);
        throw SQLException::rException(wxString::Format(wxT("rSession: Cannot abort transaction:%s"), err));
    }
}

void WorkUnit::Session::CreateObject(const wxString &objectName, const ObjectType &ty, const std::vector<wxString> crSQL, bool dropIfExist, const wxString &masterName, const wxString &siblingName) {
    bool objectExist = false;
    switch (ty) {
        case Constraint: objectExist = IsConstraintExist(objectName); break;
        case Function: objectExist = IsFunctionExist(objectName); break;
        case Index: objectExist = IsIndexExist(objectName); break;
        case Procedure: objectExist = IsProcedureExist(objectName); break;
        case Table: objectExist = IsTableExist(objectName); break;
        case View: objectExist = IsViewExist(objectName); break;
        case Trigger: objectExist = IsTriggerExist(objectName); break;
    }

    if (objectExist) {
        if (dropIfExist) {
            wxString sql;
            switch (ty) {
                case Constraint: sql.Printf(wxT("DROP CONSTRAINT %s"), objectName); break;
                case Function: sql.Printf(wxT("DROP FUNCTION %s"), objectName); break;
                case Index: sql.Printf(wxT("DROP INDEX %s"), objectName); break;
                case Procedure: sql.Printf(wxT("DROP PROCEDURE %s"), objectName); break;
                case Table: sql.Printf(wxT("DROP TABLE %s"), objectName); break;
                case View: sql.Printf(wxT("DROP VIEW %s"), objectName); break;
                case Trigger: sql.Printf(wxT("DROP TRIGGER %s"), objectName); break;
            }
            if (!sql.IsEmpty()) {
                Execute(sql);
            }
            objectExist = false;
        }
    }

    if (!objectExist) {
        for (auto v : crSQL) {
            v.Replace(wxT("<MASTER>"), masterName);
            v.Replace(wxT("<SIBLING>"), siblingName);
            Execute(v);
        }
    } else {
        for (auto v : crSQL) {
            const wxChar *p = v.c_str();
            p = String::SkipWhiteSpace(p);
            if (wxStrnicmp(p, wxT("create"), 6) != 0) continue;
            p = String::SkipUntilSpace(p);
            p = String::SkipWhiteSpace(p);
            if (wxStrnicmp(p, wxT("table"), 5) != 0) continue;
            p = String::SkipUntilSpace(p);
            p = String::SkipWhiteSpace(p);
            wxChar tabName[200];
            p = String::CopyUntil('(', tabName, p, sizeof(tabName));
            wxStringTokenizer tok(p, wxT(","), wxTOKEN_RET_EMPTY);
            wxString alterCmd(wxString::Format(wxT("alter table %s add "), tabName));
            wxString delim(wxT(""));
            bool toExecuteAlter = false;
            while (tok.HasMoreTokens()) {
                wxString columnCmd = tok.GetNextToken();
                const wxChar *pColCmdStr = columnCmd.c_str();
                pColCmdStr = String::SkipWhiteSpace(pColCmdStr);
                wxChar colName[200];
                memset(colName, 0, sizeof(colName));
                bool colMayContainSpace = false;
                if (*pColCmdStr == '[') {
                    colMayContainSpace = true;
                    pColCmdStr++;
                    pColCmdStr = String::CopyUntil(']', colName, pColCmdStr, sizeof(colName));
                } else
                    pColCmdStr = String::CopyUntilCharOrSpace(colName, pColCmdStr, ')', sizeof(colName));
                if (String::IsEmpty(colName)) continue;
                if (wxStricmp(colName, wxT("primary")) == 0) continue;
                if (!IsColumnExist(tabName, colName)) {
                    toExecuteAlter = true;
                    wxChar colType[20], colLen[20];
                    memset(colLen, 0, sizeof(colLen));
                    pColCmdStr = String::SkipWhiteSpace(pColCmdStr);
                    pColCmdStr = String::CopyUntilCharOrSpace(colType, pColCmdStr, ')', sizeof(colType));
                    if (*(pColCmdStr - 1) == ')') wxStrcat(colType, wxT(")"));
                    pColCmdStr = String::SkipWhiteSpace(pColCmdStr);
                    if (*pColCmdStr == '(') {
                        pColCmdStr++;
                        pColCmdStr = String::CopyUntilChar(colLen, pColCmdStr, ')', sizeof(colLen));
                    }
                    wxString cmd;
                    if (colMayContainSpace) {
                        cmd.Append(wxT("["));
                        cmd.Append(colName);
                        cmd.Append(wxT("]"));
                    } else
                        cmd.Append(colName);
                    cmd.Append(wxT(" "));
                    cmd.Append(colType);
                    if (!String::IsEmpty(colLen)) {
                        cmd.Append(wxT("("));
                        cmd.Append(colLen);
                        cmd.Append(wxT(")"));
                    }
                    alterCmd.Append(delim);
                    alterCmd.Append(cmd);
                    delim = wxT(",");
                }
            }
            if (toExecuteAlter) {
                Execute(alterCmd);
            }
        }
    }
}

bool WorkUnit::MsSQLSession::IsDatabaseExist(const wxString &dbName) {
    wxString cmdString;
    IRowset *pIRowSet;
    cmdString.Printf(wxT("SELECT name FROM SYSDATABASES WHERE NAME='%s'"), dbName);
    SQLCommand::Simple *cmd = new SQLCommand::Simple(*this, cmdString);
    if (!cmd) throw SQLException::rException(wxT("cannot allocate memory"));
    cmd->Prepare();
    cmd->Execute(pIRowSet);
    bool found = false;
    if (pIRowSet != NULL) {
        RowSet::All *rowset = new RowSet::All(pIRowSet);
        if (!rowset) throw SQLException::rException(wxT("cannot allocate memory"));
        rowset->Prepare();
        found = (rowset->GetNextRows() > 0);
        pIRowSet->Release();
        pIRowSet = NULL;
        delete rowset;
        rowset = NULL;
    }
    delete cmd;
    cmd = NULL;
    return (found);
}

bool WorkUnit::MsSQLSession::IsFilegroupExist(const wxString &filegroup) {
    wxString cmdString;
    IRowset *pIRowSet;
    cmdString.Printf(wxT("select groupname from sysfilegroups where groupname='%s'"), filegroup);
    SQLCommand::Simple *cmd = new SQLCommand::Simple(*this, cmdString);
    if (!cmd) throw SQLException::rException(wxT("cannot allocate memory"));
    cmd->Prepare();
    cmd->Execute(pIRowSet);
    bool found = false;
    if (pIRowSet != NULL) {
        RowSet::All *rowset = new RowSet::All(pIRowSet);
        if (!rowset) throw SQLException::rException(wxT("cannot allocate memory"));
        rowset->Prepare();
        found = (rowset->GetNextRows() > 0);
        pIRowSet->Release();
        pIRowSet = NULL;
        delete rowset;
        rowset = NULL;
    }
    delete cmd;
    cmd = NULL;
    return (found);
}

//select c.* from syscolumns c
//inner join sysobjects t on t.id=c.id
//where t.name='Relationship'
//order by colorder

bool WorkUnit::MsSQLSession::IsColumnExist(const wxString &tabName, const wxString &colName) {
    wxString tName = String::StripBracket(tabName);
    wxString cName = String::StripBracket(colName);
    wxString cmdString;
    IRowset *pIRowSet;
    cmdString.Printf(
        wxT("select c.name from syscolumns c ")
            wxT("inner join sysobjects t on t.id=c.id ")
                wxT("where t.name='%s' and c.name='%s'"),
        tName,
        cName);

    SQLCommand::Simple *cmd = new SQLCommand::Simple(*this, cmdString);
    if (!cmd) throw SQLException::rException(wxT("cannot allocate memory"));
    cmd->Prepare();
    cmd->Execute(pIRowSet);
    bool found = false;
    if (pIRowSet != NULL) {
        RowSet::All *rowset = new RowSet::All(pIRowSet);
        if (!rowset) throw SQLException::rException(wxT("cannot allocate memory"));
        rowset->Prepare();
        found = (rowset->GetNextRows() > 0);
        pIRowSet->Release();
        pIRowSet = NULL;
        delete rowset;
        rowset = NULL;
    }
    delete cmd;
    cmd = NULL;
    return (found);
}

std::vector<wxString> WorkUnit::MsSQLSession::GetColumnList(const wxString &tabName) {
    wxString cmdString;
    IRowset *pIRowSet;
    cmdString.Printf(
        wxT("select c.name from syscolumns c ")
            wxT("inner join sysobjects t on t.id=c.id ")
                wxT("where t.name='%s' ")
                    wxT("order by colorder"),
        tabName);

    SQLCommand::Simple *cmd = new SQLCommand::Simple(*this, cmdString);
    if (!cmd) throw SQLException::rException(wxT("cannot allocate memory"));
    cmd->Prepare();
    cmd->Execute(pIRowSet);
    std::vector<wxString> colList;

    if (pIRowSet != NULL) {
        RowSet::All *rowset = new RowSet::All(pIRowSet);
        if (!rowset) throw SQLException::rException(wxT("cannot allocate memory"));
        rowset->Prepare(100);
        long nRows = 0;
        while ((nRows = rowset->GetNextRows()) > 0) {
            for (long i = 0; i < nRows; i++) {
                rowset->GetData(i);
                colList.emplace_back(rowset->GetColumnValue(0));
            }
        }
        pIRowSet->Release();
        pIRowSet = NULL;
        delete rowset;
        rowset = NULL;
    }
    delete cmd;
    cmd = NULL;
    return colList;
}

bool WorkUnit::MsSQLSession::IsFunctionExist(const wxString &fnName) {
    wxString cmdString;
    IRowset *pIRowSet;
    cmdString.Printf(wxT("SELECT name FROM SYSOBJECTS WHERE XTYPE in ('TF', 'FN') AND NAME LIKE '%s'"), fnName);
    SQLCommand::Simple *cmd = new SQLCommand::Simple(*this, cmdString);
    if (!cmd) throw SQLException::rException(wxT("cannot allocate memory"));
    cmd->Prepare();
    cmd->Execute(pIRowSet);
    bool found = false;
    if (pIRowSet != NULL) {
        RowSet::All *rowset = new RowSet::All(pIRowSet);
        if (!rowset) throw SQLException::rException(wxT("cannot allocate memory"));
        rowset->Prepare();
        found = (rowset->GetNextRows() > 0);
        pIRowSet->Release();
        pIRowSet = NULL;
        delete rowset;
        rowset = NULL;
    }
    delete cmd;
    cmd = NULL;
    return (found);
}

bool WorkUnit::MsSQLSession::IsObjectExist(const wxString &objType, const wxString &fnName) {
    wxString cmdString;
    IRowset *pIRowSet;
    wxString serverName, schemaName, objName;
    if (fnName.Contains(wxT("."))) {
        wxStringTokenizer tok(fnName, wxT("."), wxTOKEN_RET_EMPTY);
        for (int i = 0; tok.HasMoreTokens(); i++) {
            wxString v = tok.GetNextToken();
            switch (i) {
                case 0: serverName = v; break;
                case 1: schemaName = v; break;
                case 2: objName = v; break;
            }
        }
    }
    if (serverName.IsEmpty()) {
        cmdString.Printf(wxT("SELECT name FROM SYSOBJECTS WHERE XTYPE='%s' AND NAME LIKE '%s'"), objType, fnName);
    } else {
        cmdString.Printf(wxT("SELECT name FROM %s.%s.SYSOBJECTS WHERE XTYPE='%s' AND NAME LIKE '%s'"), serverName, schemaName, objType, objName);
    }
    SQLCommand::Simple *cmd = new SQLCommand::Simple(*this, cmdString);
    if (!cmd) throw SQLException::rException(wxT("cannot allocate memory"));
    cmd->Prepare();
    cmd->Execute(pIRowSet);
    bool found = false;
    if (pIRowSet != NULL) {
        RowSet::All *rowset = new RowSet::All(pIRowSet);
        if (!rowset) throw SQLException::rException(wxT("cannot allocate memory"));
        rowset->Prepare();
        found = (rowset->GetNextRows() > 0);
        pIRowSet->Release();
        pIRowSet = NULL;
        delete rowset;
        rowset = NULL;
    }
    delete cmd;
    cmd = NULL;
    return (found);
}

bool WorkUnit::MsSQLSession::IsIndexExist(const wxString &fnName) {
    wxString cmdString;
    IRowset *pIRowSet;
    cmdString.Printf(wxT("SELECT name FROM SYSINDEXES WHERE NAME LIKE '%s'"), fnName);
    SQLCommand::Simple *cmd = new SQLCommand::Simple(*this, cmdString);
    if (!cmd) throw SQLException::rException(wxT("cannot allocate memory"));
    cmd->Prepare();
    cmd->Execute(pIRowSet);
    bool found = false;
    if (pIRowSet != NULL) {
        RowSet::All *rowset = new RowSet::All(pIRowSet);
        if (!rowset) throw SQLException::rException(wxT("cannot allocate memory"));
        rowset->Prepare();
        found = (rowset->GetNextRows() > 0);
        pIRowSet->Release();
        pIRowSet = NULL;
        delete rowset;
        rowset = NULL;
    }
    delete cmd;
    cmd = NULL;
    return (found);
}

void WorkUnit::Session::Execute(const wxString &sql) {
    SQLCommand::Simple cmd(*this, sql);
    cmd.Prepare();
    cmd.Execute();
}

long WorkUnit::Session::GetLong(const wxString &sql) {
    SQLCommand::Simple cmd(*this, sql);
    cmd.Prepare();
    IRowset *p;
    cmd.Execute(p);
    long r = 0;
    if (p != NULL) {
        RowSet::Long rowset(p);
        rowset.Prepare(1);
        long k = rowset.GetNextRows();
        if (k > 0) {
            rowset.GetData(0);
            if (rowset.number.stt == DBSTATUS_S_OK)
                r = rowset.number.number;
        }
        p->Release();
    }
    return r;
}
#endif
