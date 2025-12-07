#pragma once
#include "dbserver.h"
#include "words.h"

#include <iostream>
#include <fstream>
#include <deque>
#include <deque>
#include "wx/thread.h"

#if defined(_WIN32) && defined(__VISUALC__)
#include <unordered_map>
#endif

namespace SQLCommand {
    class Simple;
}

namespace WU {
    enum ObjectType { EOT,
        Table,
        Function,
        Procedure,
        View,
        Index,
        Command,
        Constraint,
        Trigger };
}

namespace WorkUnit {
#if defined(_WIN32) && defined(__VISUALC__)

    typedef std::deque<ICommand *> CommandDeque;
    class Session {
        //		Tools::Tracer tracer;
        //		Tools::Tracer *cmdTracer;
        wxMutex mtxCommandSet;
        std::unordered_map<DWORD, CommandDeque> commandSet;

    public:
        //		static std::ofstream *fOut;
    public:
        CComPtr<IDBCreateSession> pIDBCreateSession;
        CComPtr<IDBCreateCommand> pIDBCreateCommand;
        CComPtr<ITransactionLocal> pITransactionLocal;
        //		bool toTraceCommand;
        void RegisterCommand(ICommand *c);
        void DeRegisterCommand();
        void CancelCommand(DWORD tid);

    public:
        Session(DBServer::Base &ds);
        Session(const Session &);
        virtual ~Session();
        void BeginTransaction();
        void CommitTransaction();
        void AbortTransaction();
        //		void TraceStart(const wxString &s);
        //		void Log(const wxString &s) { mvLogMessage(s);  }
        //		Tools::Tracer* GetTracer( const wxString &s ) { return new Tools::Tracer( fOut, s ); }
        //		void TraceStop();

        void Execute(const wxString &cmd);
        long GetLong(const wxString &sql);
        virtual bool IsDatabaseExist(const wxString & /*dbName*/) { return false; }
        virtual bool IsObjectExist(const wxString & /*objType*/, const wxString & /*fnName*/) { return false; }
        virtual bool IsFunctionExist(const wxString & /*fnName*/) { return false; }
        virtual bool IsTriggerExist(const wxString &fnName) { return IsObjectExist("TR", fnName); }
        virtual bool IsViewExist(const wxString &fnName) { return IsObjectExist("V", fnName); }
        virtual bool IsTableExist(const wxString &fnName) { return IsObjectExist("U", fnName); }
        virtual bool IsProcedureExist(const wxString &fnName) { return IsObjectExist("P", fnName); }
        virtual bool IsConstraintExist(const wxString &fnName) { return IsObjectExist("C", fnName); }
        virtual bool IsIndexExist(const wxString & /*fnName*/) { return false; }
        virtual bool IsFilegroupExist(const wxString &) { return false; }
        virtual bool IsColumnExist(const wxString & /*tabName*/, const wxString & /*colName*/) { return false; }
        virtual void CreateObject(const wxString &f, const WU::ObjectType &ty, const std::vector<wxString> crSQL, bool dropIfExist = false, const wxString &replMaster = wxEmptyString, const wxString &replSibling = wxEmptyString);
        virtual std::vector<wxString> GetColumnList(const wxString & /*tabName*/) { return std::vector<wxString>(); }
    };

    class MsSQLSession : public Session {
    public:
        MsSQLSession(DBServer::Base &ds) : Session(ds) {}
        bool IsDatabaseExist(const wxString &dbName);
        bool IsObjectExist(const wxString &objType, const wxString &fnName);
        virtual bool IsIndexExist(const wxString &fnName);
        virtual bool IsFunctionExist(const wxString &fnName);
        virtual bool IsFilegroupExist(const wxString &);
        virtual bool IsColumnExist(const wxString &tabName, const wxString &colName);
        virtual std::vector<wxString> GetColumnList(const wxString &tabName);
    };
#endif
}
