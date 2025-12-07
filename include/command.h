#ifndef COMMAND_H
#define COMMAND_H

#include "workunit.h"
#include "sqlexception.h"
#include "rowset.h"

#if defined(_WIN32) && defined(__VISUALC__)
namespace SQLCommand {

    DBSTATUS SetDate(DBTIMESTAMP &timeStamp, const wxString &dt);
    std::string ShowStatus(const DBSTATUS &s);

    class Simple {
    protected:
        WorkUnit::Session *ss;
        bool isPrepared;
        wxString cmd;
        RowSet::Base *rowset;

    protected:
        ICommand *pICommand;
        wxString GetCommand();        // get, create if not exist
        wxString GetCommandString();  // just get the string
        virtual long GetNumParams() { return 0; }

    public:
        Simple(WorkUnit::Session &ssP, const wxString &_cmd);
        Simple(WorkUnit::Session &_ss);
        virtual ~Simple();
        virtual void ClearMemory();
        bool IsPrepared() { return isPrepared; }
        virtual void Prepare(bool doPrepare = true, bool doFetchBackwards = false, bool updateable = false, bool deferredUpdate = true);
        void Set(RowSet::Base *r) { rowset = r; }
        RowSet::Base *GetRowSet() { return rowset; }

        //		void Log(const wxString &s) { ss.Log(s); }

        virtual long GetMaxParams() { return 0; }  // number of parameter values... for bulk update/query.
        virtual RowSet::Base *CreateRowSet(IRowset *p) { return new RowSet::All(p); }

        virtual long Execute();
        bool Cancel();
        virtual long Execute(IRowset *&pIRowSet, long nParam = 0, void *paramValues = NULL);

        virtual wxString CreateProcedureSQL() { throw SQLException::rException("Function to create procedure was not defined!"); }
        virtual wxString GetProcedureName() { return wxEmptyString; }
//		virtual void RetrieveSingletonResult( wxString &a1, ...);
#ifndef NDEBUG
        virtual void DumpStatus(void *, long) {}
#endif
    };

    class WithParameters : public Simple {
        DBPARAMBINDINFO *_paramBindInfo;
        DBBINDING *_binding;
        DB_UPARAMS *_ordinal;
        DBBINDSTATUS *_bindingStatus;
        DB_UPARAMS nParams;
        long iParamCnt;
        BYTE *paramBuffer;
        BYTE *pOffset;
        long iMaxParams;

    protected:
        IAccessor *pIAccessor;
        DBPARAMS params;
        std::vector<DBPARAMBINDINFO> *bindInfoList;
        std::vector<DBBINDING> *bindingList;

        DBPARAMBINDINFO *GetParamBindInfo();
        DBBINDING *GetBinding();
        virtual long GetBindingRowSize();
        virtual long GetNumParams();  // number of parameter
        DB_UPARAMS *GetParamOrdinals();

        long r_bindingRowSize, r_statusOffset, r_lengthOffset;

    public:
        enum ParamType { Input,
            Output,
            IO };

        WithParameters(WorkUnit::Session &ss, long size);
        WithParameters(WorkUnit::Session &ss, const wxString &sqlcmd, long size);
        virtual ~WithParameters();
        long GetBindSize() { return GetBindingRowSize(); }
        virtual void ClearMemory();
        virtual void Init() {}
        virtual void Prepare(bool doPrepare = true, bool doFetchBackward = false, bool updateable = false, bool deferredUpdate = true);
        virtual long Execute(IRowset *&pIRowSet, long nParam = 0, void *paramValues = NULL);
        void *GetBuffer() {
            if (isPrepared) return paramBuffer;
            throw SQLException::rException("Cannot run GetBuffer() until ::Prepare() is called!");
        }
        void SetSize(long bindingRowSize, long statusOffset, long lengthOffset = -1) {
            r_bindingRowSize = bindingRowSize;
            r_statusOffset = statusOffset;
            r_lengthOffset = lengthOffset;
        }

        virtual long GetStatusOffset() { return r_statusOffset; }
        virtual long GetLengthOffset() { return r_lengthOffset; }
        void ResetStatus(void *p);
        void ResetStatus(void *p, int i);
        void SetNull(void *p);
        void SetNull(void *p, int i);
        void SetLength(void *p, int i, int sz);
        bool IsNull(void *p, int i);
        void SetDate(void *p, DBTIMESTAMP &timeStamp, const wxString &dt, int index);
        virtual long GetMaxParams() { return iMaxParams; }  // number of parameter values... for bulk update/query.
        void *GetNextParam(long &noOfUpdatedRecords, bool autoUpdate = true);
        long Done();

        void AddParam(const wxString &paramName, int v, long offsetValue, ParamType paramType = Input, long offsetStatus = -1, long offsetLength = -1);
        void AddParam(const wxString &paramName, long v, long offsetValue, ParamType paramType = Input, long offsetStatus = -1, long offsetLength = -1);
        void AddParam(const wxString &paramName, double v, long offsetValue, ParamType paramType = Input, long offsetStatus = -1, long offsetLength = -1);
        void AddParam(const wxString &paramName, float v, long offsetValue, ParamType paramType = Input, long offsetStatus = -1, long offsetLength = -1);
        void AddParam(const wxString &paramName, char *v, long offsetValue, long len, ParamType paramType = Input, long offsetStatus = -1, long offsetLength = -1);
        void AddParam(const wxString &paramName, wchar_t *v, long offsetValue, long len, ParamType paramType = Input, long offsetStatus = -1, long offsetLength = -1);
        void AddParam(const wxString &paramName, DBTIMESTAMP v, long offsetValue, ParamType paramType = Input, long offsetStatus = -1, long offsetLength = -1);
    };

    void Execute(WorkUnit::Session &session, const wxString &sql);
    long GetLong(WorkUnit::Session &session, const wxString &sql);
}
#endif
#endif