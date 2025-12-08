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


#if defined(_WIN32) && defined(__MSVC__)
#include "command.h"
#include "rowset.h"
#include "sqlexception.h"
#include <atlbase.h>
#include <atlconv.h>
#include <iostream>
#include <strstream>
#include "words.h"
#include <stdarg.h>
#include <stdio.h>
#include "wx/string.h"

//#include "d4all.hpp"

std::string SQLCommand::ShowStatus(const DBSTATUS &x) {
    switch (x) {
        case DBSTATUS_S_OK: return "DBSTATUS_S_OK"; break;
        case DBSTATUS_S_ISNULL: return "DBSTATUS_S_ISNULL"; break;
        case DBSTATUS_S_TRUNCATED: return "DBSTATUS_S_TRUNCATED"; break;
        case DBSTATUS_E_BADACCESSOR: return "DBSTATUS_E_BADACCESSOR"; break;
        case DBSTATUS_E_CANTCONVERTVALUE: return "DBSTATUS_E_CANTCONVERTVALUE"; break;
        case DBSTATUS_E_CANTCREATE: return "DBSTATUS_E_CANTCREATE"; break;
        case DBSTATUS_E_DATAOVERFLOW: return "DBSTATUS_E_DATAOVERFLOW"; break;
        case DBSTATUS_E_SIGNMISMATCH: return "DBSTATUS_E_SIGNMISMATCH"; break;
        case DBSTATUS_E_UNAVAILABLE: return "DBSTATUS_E_UNAVAILABLE"; break;
    }
    return "Not Found!";
}

DBSTATUS SQLCommand::SetDate(DBTIMESTAMP &fDate, const wxString &dateField) {
    if (dateField.IsEmpty())
        return DBSTATUS_S_ISNULL;
    else {
        memset(&fDate, 0, sizeof(fDate));
        long t;
        if (!dateField.Mid(0, 4).ToLong(&t)) return DBSTATUS_S_ISNULL;
        fDate.year = t;
        if (fDate.year < 100)
            fDate.year += 1900;
        else if (fDate.year < 1900)
            fDate.year = 1900;
        else if (fDate.year > 2002)
            fDate.year = 1900 + (fDate.year % 100);
        if (!dateField.Mid(4, 2).ToLong(&t)) return DBSTATUS_S_ISNULL;
        fDate.month = t;
        if (!dateField.Mid(6, 2).ToLong(&t)) return DBSTATUS_S_ISNULL;
        fDate.day = t;
        if (!String::IsValidDate(fDate.year, fDate.month, fDate.day))
            return DBSTATUS_S_ISNULL;
    }
    return DBSTATUS_S_OK;
}

void SQLCommand::WithParameters::SetDate(void *p, DBTIMESTAMP &fDate, const wxString &dateField, int sttIndex) {
    if (!SQLCommand::SetDate(fDate, dateField))
        SetNull(p, sttIndex);
}

wxString SQLCommand::Simple::GetCommandString() {
    if (GetProcedureName().IsEmpty()) {
        return cmd;
    } else {
        wxString buffer;
        buffer.Printf(wxT("{?=call %s("), GetProcedureName());
        for (int i = 0; i < GetNumParams() - 2; i++)
            buffer.Append(wxT("?,"));
        buffer.Append(wxT("?)}"));
        return buffer;
    }
}

SQLCommand::Simple::Simple(WorkUnit::Session &ssP, const wxString &_cmd) : cmd(_cmd),
                                                                           ss(&ssP),
                                                                           isPrepared(false),
                                                                           rowset(NULL),
                                                                           pICommand(NULL) {
}

SQLCommand::Simple::Simple(WorkUnit::Session &_ss) : ss(&_ss),
                                                     isPrepared(false),
                                                     rowset(NULL),
                                                     pICommand(NULL) {
}

SQLCommand::Simple::~Simple() {
    if (rowset != NULL) delete rowset;
    if (pICommand != NULL) pICommand->Release();
    rowset = NULL;
    pICommand = NULL;
}

void SQLCommand::Simple::ClearMemory() {
    if (rowset != NULL) delete rowset;
    if (pICommand != NULL) pICommand->Release();
    rowset = NULL;
    pICommand = NULL;
}

wxString SQLCommand::Simple::GetCommand() {
    wxString buf = GetCommandString();

    if (GetProcedureName().IsEmpty())
        return buf;
    else {
        if (!ss->IsProcedureExist(GetProcedureName())) {
            wxString sql = CreateProcedureSQL();
            if (!sql.IsEmpty()) {
                SQLCommand::Simple *s = new SQLCommand::Simple(*ss, sql);
                if (!s) {
                    ClearMemory();
                    throw SQLException::rException(wxT("cannot create command"));
                }
                s->Prepare();
                s->Execute();
                delete s;
                s = NULL;
            } else {
                ClearMemory();
                throw SQLException::rException(wxString::Format(wxT("Script for %s not found"), GetProcedureName()));
            }
        }
        return buf;
    }
}

void SQLCommand::Simple::Prepare(bool /*doPrepare*/, bool doFetchBackwards, bool updateable, bool deferredUpdate) {
    HRESULT hr = ss->pIDBCreateCommand->CreateCommand(NULL, IID_ICommand, (IUnknown **)&pICommand);
    CComPtr<ICommandProperties> pICommandProperties;
    hr = pICommand->QueryInterface(IID_ICommandProperties, (LPVOID *)&pICommandProperties);
    if (SUCCEEDED(hr)) {
        DBPROPSET propset[1];
        DBPROP props[20];
        int nProps = 0;
        if (updateable) {
            props[nProps].dwPropertyID = DBPROP_UPDATABILITY;
            props[nProps].dwOptions = DBPROPOPTIONS_REQUIRED;
            props[nProps].dwStatus = DBPROPSTATUS_OK;
            props[nProps].colid = DB_NULLID;
            props[nProps].vValue.vt = VT_I4;
            props[nProps++].vValue.lVal = DBPROPVAL_UP_CHANGE | DBPROPVAL_UP_INSERT | DBPROPVAL_UP_DELETE;

            props[nProps].dwPropertyID = DBPROP_IRowsetChange;
            props[nProps].dwOptions = DBPROPOPTIONS_REQUIRED;
            props[nProps].dwStatus = DBPROPSTATUS_OK;
            props[nProps].colid = DB_NULLID;
            props[nProps].vValue.vt = VT_BOOL;
            props[nProps++].vValue.boolVal = VARIANT_TRUE;

            props[nProps].dwPropertyID = DBPROP_OWNUPDATEDELETE;
            props[nProps].dwOptions = DBPROPOPTIONS_REQUIRED;
            props[nProps].dwStatus = DBPROPSTATUS_OK;
            props[nProps].colid = DB_NULLID;
            props[nProps].vValue.vt = VT_BOOL;
            props[nProps++].vValue.boolVal = VARIANT_TRUE;

            if (deferredUpdate) {
                props[nProps].dwPropertyID = DBPROP_IRowsetUpdate;
                props[nProps].dwOptions = DBPROPOPTIONS_REQUIRED;
                props[nProps].dwStatus = DBPROPSTATUS_OK;
                props[nProps].colid = DB_NULLID;
                props[nProps].vValue.vt = VT_BOOL;
                props[nProps++].vValue.boolVal = VARIANT_TRUE;

                props[nProps].dwPropertyID = DBPROP_CANHOLDROWS;
                props[nProps].dwOptions = DBPROPOPTIONS_REQUIRED;
                props[nProps].dwStatus = DBPROPSTATUS_OK;
                props[nProps].colid = DB_NULLID;
                props[nProps].vValue.vt = VT_BOOL;
                props[nProps++].vValue.boolVal = VARIANT_TRUE;
            }
            if (doFetchBackwards) {
                props[nProps].dwPropertyID = DBPROP_CANFETCHBACKWARDS;
                props[nProps].dwOptions = DBPROPOPTIONS_REQUIRED;
                props[nProps].dwStatus = DBPROPSTATUS_OK;
                props[nProps].colid = DB_NULLID;
                props[nProps].vValue.vt = VT_BOOL;
                props[nProps++].vValue.boolVal = VARIANT_TRUE;
            }

            propset[0].rgProperties = props;
            propset[0].cProperties = nProps;
            propset[0].guidPropertySet = DBPROPSET_ROWSET;
            hr = pICommandProperties->SetProperties(1, propset);
            if (FAILED(hr)) {
                wxString err = SQLException::DumpErrorInfo(pICommandProperties, IID_ICommandProperties);
                ClearMemory();
                throw SQLException::rException(wxString::Format(wxT("Command: Cannot create command text: %s"), err));
            }
        }
    }

    CComPtr<ICommandText> pICommandText;
    hr = pICommand->QueryInterface(IID_ICommandText, (LPVOID *)&pICommandText);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(ss->pIDBCreateCommand, IID_IDBCreateCommand);
        ClearMemory();
        throw SQLException::rException(wxString::Format(wxT("Command: Cannot create command text: %s"), err));
    }
    hr = pICommandText->SetCommandText(DBGUID_DBSQL, GetCommand().wc_str(*wxConvCurrent));  //A2COLE(GetCommand().c_str()));
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pICommandText, IID_ICommandText);
        ClearMemory();
        throw SQLException::rException(wxString::Format(wxT("Command: Cannot set command text : %s"), err));
    }
    isPrepared = true;
}

bool SQLCommand::Simple::Cancel() {
    if (pICommand != NULL) {
        HRESULT hr = pICommand->Cancel();
        if (hr == DB_E_CANTCANCEL) return false;
        if (FAILED(hr)) {
            wxString err = SQLException::DumpErrorInfo(pICommand, IID_ICommand);
            ClearMemory();
            throw SQLException::rException(wxString::Format(wxT("Command execution failed: %s"), err));
        }
    }
    return true;
}

long SQLCommand::Simple::Execute(IRowset *&pIRowset, long, void *param) {
    DBROWCOUNT rowsAffected;
    if (pICommand == NULL) {
        ClearMemory();
        throw SQLException::rException(wxT("::Prepare - how come pICommand is NULL!!!"));
    }

    if (!isPrepared) {
        ClearMemory();
        throw SQLException::rException(wxT("::Prepare is not called yet!"));
    }
    HRESULT hr;

    ss->RegisterCommand(pICommand);
    if (pIRowset == (IRowset *)0xCCCC) {
        hr = pICommand->Execute(NULL, IID_NULL, (DBPARAMS *)param, &rowsAffected, NULL);
        pIRowset = NULL;
    } else
        hr = pICommand->Execute(NULL, IID_IRowset, (DBPARAMS *)param, &rowsAffected, (IUnknown **)&pIRowset);
    ss->DeRegisterCommand();

    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pICommand, IID_ICommand);
        //		if (hr == DB_E_CANCELED) pICommand = NULL;
        pICommand = NULL;
        ClearMemory();
        throw SQLException::rException(wxString::Format(wxT("Command execution failed: %s"), err));
    }
    return rowsAffected;
}

long SQLCommand::Simple::Execute() {
    DBROWCOUNT rowsAffected;
    if (pICommand == NULL) {
        ClearMemory();
        throw SQLException::rException(wxT("::Prepare - how come pICommand is NULL!!!"));
    }
    if (!isPrepared) {
        ClearMemory();
        throw SQLException::rException(wxT("::Prepare is not called yet!"));
    }
    ss->RegisterCommand(pICommand);
    HRESULT hr = pICommand->Execute(NULL, IID_NULL, NULL, &rowsAffected, NULL);
    ss->DeRegisterCommand();
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pICommand, IID_ICommand);
        if (rowset != NULL) delete rowset;
        rowset = NULL;
        pICommand = NULL;
        //ClearMemory();
        throw SQLException::rException(wxString::Format(wxT("Command execution failed : %s"), err));
    }
    return rowsAffected;
}

SQLCommand::WithParameters::WithParameters(WorkUnit::Session &ss, long size) : Simple(ss) {
    pIAccessor = NULL;
    iMaxParams = size;
    paramBuffer = NULL;
    _paramBindInfo = NULL;
    _binding = NULL;
    _ordinal = NULL;
    _bindingStatus = NULL;
    bindingList = new std::vector<DBBINDING>;
    bindInfoList = new std::vector<DBPARAMBINDINFO>;
    if (!bindingList || !bindInfoList) throw SQLException::rException(wxT("cannot allocate memory"));
    r_bindingRowSize = r_statusOffset = r_lengthOffset = -1;
}

SQLCommand::WithParameters::WithParameters(WorkUnit::Session &ss, const wxString &sqlcmd, long size) : Simple(ss, sqlcmd) {
    pIAccessor = NULL;
    iMaxParams = size;
    paramBuffer = NULL;
    _paramBindInfo = NULL;
    _binding = NULL;
    _ordinal = NULL;
    _bindingStatus = NULL;
    bindingList = new std::vector<DBBINDING>;
    bindInfoList = new std::vector<DBPARAMBINDINFO>;
    if (!bindingList || !bindInfoList) throw SQLException::rException(wxT("cannot allocate memory"));
    r_bindingRowSize = r_statusOffset = r_lengthOffset = -1;
}

void SQLCommand::WithParameters::ClearMemory() {
    if (pIAccessor != NULL) {
        pIAccessor->ReleaseAccessor(params.hAccessor, NULL);
        pIAccessor = NULL;
    }
    Simple::ClearMemory();
    if (paramBuffer != NULL) {
        delete[] paramBuffer;
        paramBuffer = NULL;
    }
    if (_paramBindInfo != NULL) {
        DBPARAMBINDINFO *p = _paramBindInfo;
        for (long i = 0; i < nParams; i++, p++) {
            if (p->pwszDataSourceType != NULL) SysFreeString(p->pwszDataSourceType);
            if (p->pwszName != NULL) SysFreeString(p->pwszName);
            p->pwszDataSourceType = NULL;
            p->pwszName = NULL;
        }
        delete[] _paramBindInfo;
        _paramBindInfo = NULL;
    }
    if (_binding != NULL) {
        delete[] _binding;
        _binding = NULL;
    }
    if (_ordinal != NULL) {
        delete[] _ordinal;
        _ordinal = NULL;
    }
    if (_bindingStatus != NULL) {
        delete[] _bindingStatus;
        _bindingStatus = NULL;
    }
    if (bindInfoList != NULL) {
        delete bindInfoList;
        bindInfoList = NULL;
    }
    if (bindingList != NULL) {
        delete bindingList;
        bindingList = NULL;
    }
}

SQLCommand::WithParameters::~WithParameters() {
    if (paramBuffer != NULL) {
        delete[] paramBuffer;
        paramBuffer = NULL;
    }
    if (pIAccessor != NULL) {
        pIAccessor->ReleaseAccessor(params.hAccessor, NULL);
        pIAccessor = NULL;
    }
    if (_paramBindInfo != NULL) {
        DBPARAMBINDINFO *p = _paramBindInfo;
        for (long i = 0; i < nParams; i++, p++) {
            if (p->pwszDataSourceType != NULL) SysFreeString(p->pwszDataSourceType);
            if (p->pwszName != NULL) SysFreeString(p->pwszName);
            p->pwszDataSourceType = NULL;
            p->pwszName = NULL;
        }
        delete[] _paramBindInfo;
        _paramBindInfo = NULL;
    }
    if (_binding != NULL) {
        delete[] _binding;
        _binding = NULL;
    }
    if (_ordinal != NULL) {
        delete[] _ordinal;
        _ordinal = NULL;
    }
    if (_bindingStatus != NULL) {
        delete[] _bindingStatus;
        _bindingStatus = NULL;
    }
    if (bindInfoList != NULL) {
        delete bindInfoList;
        bindInfoList = NULL;
    }
    if (bindingList != NULL) {
        delete bindingList;
        bindingList = NULL;
    }
}

void SQLCommand::WithParameters::ResetStatus(void *param, int i) {
    /*
	if (GetStatusOffset() < 0) return;
	if (!isPrepared) {
		ClearMemory();
		throw SQLException::rException(wxT("Cannot run SetNull() until ::Prepare() is called!"));
	}
	DBSTATUS *p = (DBSTATUS *)((BYTE *)param + GetStatusOffset());
	if (i >= 0 && i < nParams)
		p[i] = DBSTATUS_S_OK;
	else {
		ClearMemory();
		throw SQLException::rException(wxT("ResetStatus(p, i): array out of bound!"));
	}
*/
    if (!isPrepared) {
        ClearMemory();
        throw SQLException::rException(wxT("Cannot run SetNull() until ::Prepare() is called!"));
    }

    if (i >= 0 && i < nParams) {
        DBBINDING &b = _binding[i];
        if ((b.dwPart & DBPART_STATUS) != 0) {
            DBSTATUS *st = (DBSTATUS *)((BYTE *)param + b.obStatus);
            *st = DBSTATUS_S_OK;
        }
    } else {
        ClearMemory();
        throw SQLException::rException(wxT("SetNull(): array out of bound!"));
    }
}
void SQLCommand::WithParameters::ResetStatus(void *param) {
    /*
	if (GetStatusOffset() < 0) return;
	if (!isPrepared) {
		ClearMemory();
		throw SQLException::rException(wxT("Cannot run ResetStatus() until ::Prepare() is called!"));
	}
	DBSTATUS *p = (DBSTATUS *)((BYTE *)param + GetStatusOffset());
	for (int i=0; i<nParams; i++, p++) 
		*p = DBSTATUS_S_OK;

*/
    if (!isPrepared) {
        ClearMemory();
        throw SQLException::rException(wxT("Cannot run ResetStatus() until ::Prepare() is called!"));
    }

    DBBINDING *p = _binding;
    for (int i = 0; i < nParams; i++, p++) {
        if ((p->dwPart & DBPART_STATUS) != 0) {
            DBSTATUS *st = (DBSTATUS *)((BYTE *)param + p->obStatus);
            *st = DBSTATUS_S_OK;
        }
    }
}

void SQLCommand::WithParameters::SetNull(void *param) {
    if (!isPrepared) {
        ClearMemory();
        throw SQLException::rException(wxT("Cannot run ResetStatus() until ::Prepare() is called!"));
    }

    DBBINDING *p = _binding;
    for (int i = 0; i < nParams; i++, p++) {
        if ((p->dwPart & DBPART_STATUS) != 0) {
            DBSTATUS *st = (DBSTATUS *)((BYTE *)param + p->obStatus);
            *st = DBSTATUS_S_ISNULL;
        }
    }
}

bool SQLCommand::WithParameters::IsNull(void *param, int i) {
    if (!isPrepared) {
        ClearMemory();
        throw SQLException::rException(wxT("Cannot run SetNull() until ::Prepare() is called!"));
    }

    if (i >= 0 && i < nParams) {
        DBBINDING &b = _binding[i];
        if ((b.dwPart & DBPART_STATUS) != 0) {
            DBSTATUS *st = (DBSTATUS *)((BYTE *)param + b.obStatus);
            return *st == DBSTATUS_S_ISNULL;
        }
    }
    return true;

    /*
	DBSTATUS *p = (DBSTATUS *)((BYTE *)param + GetStatusOffset());
	if (i >= 0 && i < nParams)
		return (p[i] == DBSTATUS_S_ISNULL);
	else {
		ClearMemory();
		throw SQLException::rException(wxT("IsNull(): array out of bound!"));
	}
*/
}

void SQLCommand::WithParameters::SetNull(void *param, int i) {
    /*
	if (GetStatusOffset() < 0) return;
	if (!isPrepared) {
		ClearMemory();
		throw SQLException::rException(wxT("Cannot run SetNull() until ::Prepare() is called!"));
	}
	DBSTATUS *p = (DBSTATUS *)((BYTE *)param + GetStatusOffset());
	if (i >= 0 && i < nParams)
		p[i] = DBSTATUS_S_ISNULL;
	else {
		ClearMemory();
		throw SQLException::rException(wxT("SetNull(): array out of bound!"));
	}
*/

    if (!isPrepared) {
        ClearMemory();
        throw SQLException::rException(wxT("Cannot run SetNull() until ::Prepare() is called!"));
    }

    if (i >= 0 && i < nParams) {
        DBBINDING &b = _binding[i];
        if ((b.dwPart & DBPART_STATUS) != 0) {
            DBSTATUS *st = (DBSTATUS *)((BYTE *)param + b.obStatus);
            *st = DBSTATUS_S_ISNULL;
        }
    } else {
        ClearMemory();
        throw SQLException::rException(wxT("SetNull(): array out of bound!"));
    }
}

void SQLCommand::WithParameters::SetLength(void *param, int i, int sz) {
    if (GetLengthOffset() < 0) return;
    if (!isPrepared) {
        ClearMemory();
        throw SQLException::rException(wxT("Cannot run SetLength() until ::Prepare() is called!"));
    }
    DBLENGTH *p = (DBLENGTH *)((BYTE *)param + GetLengthOffset());
    if (i >= 0 && i < nParams)
        p[i] = sz;
    else {
        ClearMemory();
        throw SQLException::rException(wxT("SetNull(): array out of bound!"));
    }
}

void *SQLCommand::WithParameters::GetNextParam(long &noOfUpdatedRecords, bool autoUpdate) {
    noOfUpdatedRecords = 0;
    if (iParamCnt >= GetMaxParams()) {
        if (autoUpdate)
            noOfUpdatedRecords = Done();
        else {
            wxString buf;
            buf.Printf(wxT("iParamCnt > MaxParam: %ld, %ld"), iParamCnt, GetMaxParams());
            ClearMemory();
            throw SQLException::rException(buf);
        }
    }
    iParamCnt++;
    pOffset += GetBindingRowSize();

    // 	should not clear since when nUpd > 0 the caller might want to read result
    //  from GetBuffer(); from offset 0!

    //	memset(pOffset, 0, GetBindingRowSize());

    return pOffset;
}

long SQLCommand::WithParameters::Done() {
    long v = iParamCnt;
    if (iParamCnt > 0) {
        IRowset *pIRowset;
        if (GetRowSet() == NULL) {
            pIRowset = (IRowset *)0xCCCC;
            Execute(pIRowset, iParamCnt);
        } else {
            Execute(pIRowset, iParamCnt);
            GetRowSet()->Set(pIRowset);
        }
        iParamCnt = 0;
        pOffset = (BYTE *)GetBuffer() - GetBindingRowSize();
    }
    return v;
}

void SQLCommand::WithParameters::Prepare(bool doPrepare, bool doFetchBackward, bool updateable, bool deferredUpdate) {
    Init();

    SQLCommand::Simple::Prepare(doPrepare, doFetchBackward, updateable, deferredUpdate);

    paramBuffer = new BYTE[GetBindingRowSize() * iMaxParams];
    if (!paramBuffer) {
        ClearMemory();
        throw SQLException::rException(wxT("cannot allocate memory"));
    }
    pOffset = (BYTE *)GetBuffer() - GetBindingRowSize();
    iParamCnt = 0;

    CComPtr<ICommandWithParameters> pICmdWithParams;
    CComPtr<ICommandPrepare> pICmdPrepare;

    HRESULT hr = pICommand->QueryInterface(IID_ICommandWithParameters, (void **)&pICmdWithParams);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pICommand, IID_ICommand);
        ClearMemory();
        throw SQLException::rException(wxString::Format(wxT("CommandWithParameters::Prepare - Cannot Query Interface ICommandWithParameters %s"), err));
    }

    _ordinal = GetParamOrdinals();
    _paramBindInfo = GetParamBindInfo();

    nParams = GetNumParams();
    hr = pICmdWithParams->SetParameterInfo(nParams, _ordinal, _paramBindInfo);

    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pICmdWithParams, IID_ICommandWithParameters);
        ClearMemory();
        throw SQLException::rException(wxString::Format(wxT("Setting Param Info : %s"), err));
    }

    if (doPrepare) {
        hr = pICommand->QueryInterface(IID_ICommandPrepare, (void **)&pICmdPrepare);
        if (FAILED(hr)) {
            wxString err = SQLException::DumpErrorInfo(pICommand, IID_ICommand);
            ClearMemory();
            throw SQLException::rException(wxString::Format(wxT("CommandWithParameters::Prepare - Cannot query interface ICommandPrepare :%s"), err));
        }
        hr = pICmdPrepare->Prepare(0);
        if (FAILED(hr)) {
            wxString err = SQLException::DumpErrorInfo(pICmdPrepare, IID_ICommandPrepare);
            ClearMemory();
            throw SQLException::rException(wxString::Format(wxT("rCommandWithParameters::Prepare - Cannot execute prepare: %s"), err));
        }
    }
    hr = pICommand->QueryInterface(IID_IAccessor, (void **)&pIAccessor);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pICommand, IID_ICommand);
        ClearMemory();
        pIAccessor = NULL;
        throw SQLException::rException(wxString::Format(wxT("rCommandWithParameters::Prepare - Cannot query interface IAccessor: %s"), err));
    }

    _bindingStatus = new DBBINDSTATUS[nParams];
    if (!_bindingStatus) {
        ClearMemory();
        throw SQLException::rException(wxT("cannot allocate memory: bindingstatus is NULL"));
    }
    _binding = GetBinding();
    hr = pIAccessor->CreateAccessor(DBACCESSOR_PARAMETERDATA, nParams, _binding, GetBindingRowSize(), &params.hAccessor, _bindingStatus);

    if (FAILED(hr)) {
        wxString msg;
        for (int iB = 0; iB < GetNumParams(); iB++) {
            if (_bindingStatus[iB] == DBBINDSTATUS_OK) {
            } else if (_bindingStatus[iB] == DBBINDSTATUS_BADORDINAL) {
                msg.Append(wxString::Format(wxT("column %d -> BAD ORDINAL\n"), iB + 1));
            } else if (_bindingStatus[iB] == DBBINDSTATUS_UNSUPPORTEDCONVERSION) {
                msg.Append(wxString::Format(wxT("column %d -> UNSUPPORTED CONVERSION"), iB + 1));
            } else if (_bindingStatus[iB] == DBBINDSTATUS_BADBINDINFO) {
                msg.Append(wxString::Format(wxT("column %d -> DBBINDSTATUS_BADBINDINFO"), iB + 1));
            } else if (_bindingStatus[iB] == DBBINDSTATUS_BADSTORAGEFLAGS) {
                msg.Append(wxString::Format(wxT("column %d -> DBBINDSTATUS_BADSTORAGEFLAGS"), iB + 1));
            } else if (_bindingStatus[iB] == DBBINDSTATUS_NOINTERFACE) {
                msg.Append(wxString::Format(wxT("column %d -> DBBINDSTATUS_NOINTERFACE"), iB + 1));
            }
        }
        wxString err = SQLException::DumpErrorInfo(pIAccessor, IID_IAccessor);
        ClearMemory();
        throw SQLException::rException(wxString::Format(wxT("CreateAccessor error: %s - %s"), err, msg));
    }
}

long SQLCommand::WithParameters::Execute(IRowset *&pIRowset, long numOfParams, void *) {
    params.pData = GetBuffer();
    if (numOfParams > GetMaxParams()) {
        ClearMemory();
        throw SQLException::rException(wxT("No of params greater than maxSize"));
    }
    params.cParamSets = numOfParams;
    return Simple::Execute(pIRowset, 0, &params);  // nParams is ignored in the Simple::Execute implementation.
}

void SQLCommand::WithParameters::AddParam(const wxString &paramName, int /*v*/, long offsetValue, ParamType paramType, long offsetStatus, long offsetLength) {
    DBBINDING &b = bindingList->emplace_back();
    DBPARAMBINDINFO &bI = bindInfoList->emplace_back();
    //			b.iOrdinal;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength >= 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    switch (paramType) {
        case Input:
            b.eParamIO = DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case Output:
            b.eParamIO = DBPARAMIO_OUTPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case IO:
            b.eParamIO = DBPARAMIO_OUTPUT | DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
    }
    b.cbMaxLen = bI.ulParamSize = sizeof(int);
    b.dwFlags = 0;
    b.wType = DBTYPE_I2;
    b.bPrecision = bI.bPrecision = 9;
    b.bScale = bI.bScale = 0;

    wxString typeStr = wxT("DBTYPE_I2");
    bI.pwszDataSourceType = SysAllocString(typeStr.wc_str(*wxConvCurrent));
    if (!paramName.IsEmpty())
        bI.pwszName = SysAllocString(paramName.wc_str(*wxConvCurrent));
    else
        bI.pwszName = NULL;
}

void SQLCommand::WithParameters::AddParam(const wxString &paramName, long /*v*/, long offsetValue, ParamType paramType, long offsetStatus, long offsetLength) {
    auto &b = bindingList->emplace_back();
    auto &bI = bindInfoList->emplace_back();

    //			b.iOrdinal;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength >= 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    switch (paramType) {
        case Input:
            b.eParamIO = DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case Output:
            b.eParamIO = DBPARAMIO_OUTPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case IO:
            b.eParamIO = DBPARAMIO_OUTPUT | DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
    }
    b.cbMaxLen = bI.ulParamSize = sizeof(long);
    b.dwFlags = 0;
    b.wType = DBTYPE_I4;
    b.bPrecision = bI.bPrecision = 23;
    b.bScale = bI.bScale = 0;

    wxString typeStr = wxT("DBTYPE_I4");
    bI.pwszDataSourceType = SysAllocString(typeStr.wc_str(*wxConvCurrent));
    if (!paramName.IsEmpty())
        bI.pwszName = SysAllocString(paramName.wc_str(*wxConvCurrent));
    else
        bI.pwszName = NULL;

}

void SQLCommand::WithParameters::AddParam(const wxString &paramName, double /*v*/, long offsetValue, ParamType paramType, long offsetStatus, long offsetLength) {
    auto &b = bindingList->emplace_back();
    auto &bI = bindInfoList->emplace_back();

    //			b.iOrdinal;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength >= 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    switch (paramType) {
        case Input:
            b.eParamIO = DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case Output:
            b.eParamIO = DBPARAMIO_OUTPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case IO:
            b.eParamIO = DBPARAMIO_OUTPUT | DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
    }
    b.cbMaxLen = bI.ulParamSize = sizeof(double);
    b.dwFlags = 0;
    b.wType = DBTYPE_R8;
    b.bPrecision = bI.bPrecision = 23;
    b.bScale = bI.bScale = 5;

    wxString typeStr = wxT("DBTYPE_R8");
    bI.pwszDataSourceType = SysAllocString(typeStr.wc_str(*wxConvCurrent));
    if (!paramName.IsEmpty())
        bI.pwszName = SysAllocString(paramName.wc_str(*wxConvCurrent));
    else
        bI.pwszName = NULL;

}

void SQLCommand::WithParameters::AddParam(const wxString &paramName, float /*v*/, long offsetValue, ParamType paramType, long offsetStatus, long offsetLength) {
    DBBINDING b;
    DBPARAMBINDINFO bI;
    //			b.iOrdinal;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength >= 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    switch (paramType) {
        case Input:
            b.eParamIO = DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case Output:
            b.eParamIO = DBPARAMIO_OUTPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case IO:
            b.eParamIO = DBPARAMIO_OUTPUT | DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
    }
    b.cbMaxLen = bI.ulParamSize = sizeof(float);
    b.dwFlags = 0;
    b.wType = DBTYPE_R4;
    b.bPrecision = bI.bPrecision = 11;
    b.bScale = bI.bScale = 5;

    wxString typeStr = wxT("DBTYPE_R4");
    bI.pwszDataSourceType = SysAllocString(typeStr.wc_str(*wxConvCurrent));
    if (!paramName.IsEmpty())
        bI.pwszName = SysAllocString(paramName.wc_str(*wxConvCurrent));
    else
        bI.pwszName = NULL;
}

void SQLCommand::WithParameters::AddParam(const wxString &paramName, char * /*v*/, long offsetValue, long len, ParamType paramType, long offsetStatus, long offsetLength) {
    auto &b = bindingList->emplace_back();
    auto &bI = bindInfoList->emplace_back();
    //			b.iOrdinal;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength >= 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    switch (paramType) {
        case Input:
            b.eParamIO = DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case Output:
            b.eParamIO = DBPARAMIO_OUTPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case IO:
            b.eParamIO = DBPARAMIO_OUTPUT | DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
    }
    b.cbMaxLen = bI.ulParamSize = len;
    b.dwFlags = 0;
    b.wType = DBTYPE_STR;
    b.bPrecision = bI.bPrecision = 0;
    b.bScale = bI.bScale = 0;

    wxString typeStr = wxT("DBTYPE_STR");
    bI.pwszDataSourceType = SysAllocString(typeStr.wc_str(*wxConvCurrent));
    if (!paramName.IsEmpty())
        bI.pwszName = SysAllocString(paramName.wc_str(*wxConvCurrent));
    else
        bI.pwszName = NULL;

}

void SQLCommand::WithParameters::AddParam(const wxString &paramName, wchar_t * /*v*/, long offsetValue, long len, ParamType paramType, long offsetStatus, long offsetLength) {
    auto &b = bindingList->emplace_back();
    auto &bI = bindInfoList->emplace_back();
    //			b.iOrdinal;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength >= 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    switch (paramType) {
        case Input:
            b.eParamIO = DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case Output:
            b.eParamIO = DBPARAMIO_OUTPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case IO:
            b.eParamIO = DBPARAMIO_OUTPUT | DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
    }
    b.cbMaxLen = bI.ulParamSize = len;
    b.dwFlags = 0;
    b.wType = DBTYPE_WSTR;
    b.bPrecision = bI.bPrecision = 0;
    b.bScale = bI.bScale = 0;

    wxString typeStr = wxT("DBTYPE_WSTR");
    bI.pwszDataSourceType = SysAllocString(typeStr.wc_str(*wxConvCurrent));
    if (!paramName.IsEmpty())
        bI.pwszName = SysAllocString(paramName.wc_str(*wxConvCurrent));
    else
        bI.pwszName = NULL;
}

void SQLCommand::WithParameters::AddParam(const wxString &paramName, DBTIMESTAMP v, long offsetValue, ParamType paramType, long offsetStatus, long offsetLength) {
    auto &b = bindingList->emplace_back();
    auto &bI = bindInfoList->emplace_back();
    //			b.iOrdinal;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength >= 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    switch (paramType) {
        case Input:
            b.eParamIO = DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case Output:
            b.eParamIO = DBPARAMIO_OUTPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
        case IO:
            b.eParamIO = DBPARAMIO_OUTPUT | DBPARAMIO_INPUT;
            bI.dwFlags = DBPARAMFLAGS_ISOUTPUT | DBPARAMFLAGS_ISINPUT | DBPARAMFLAGS_ISNULLABLE;
            break;
    }
    b.cbMaxLen = bI.ulParamSize = sizeof(v);
    b.dwFlags = 0;
    b.wType = DBTYPE_DBTIMESTAMP;
    b.bPrecision = bI.bPrecision = 23;
    b.bScale = bI.bScale = 0;

    wxString typeStr = wxT("DBTYPE_DBTIMESTAMP");
    bI.pwszDataSourceType = SysAllocString(typeStr.wc_str(*wxConvCurrent));
    if (!paramName.IsEmpty())
        bI.pwszName = SysAllocString(paramName.wc_str(*wxConvCurrent));
    else
        bI.pwszName = NULL;
}

DBPARAMBINDINFO *SQLCommand::WithParameters::GetParamBindInfo() {
    DBPARAMBINDINFO *f = new DBPARAMBINDINFO[bindInfoList->size()];
    if (!f) {
        ClearMemory();
        throw SQLException::rException(wxT("cannot create DBPARAMBINDINFO"));
    }
    DBPARAMBINDINFO *p = f;
    std::vector<DBPARAMBINDINFO>::iterator iter = bindInfoList->begin();
    for (; iter != bindInfoList->end(); iter++, p++) {
        p->bPrecision = (*iter).bPrecision;
        p->bScale = (*iter).bScale;
        p->dwFlags = (*iter).dwFlags;
        p->pwszDataSourceType = (*iter).pwszDataSourceType;
        p->pwszName = (*iter).pwszName;
        p->ulParamSize = (*iter).ulParamSize;
    }
    return f;
}

DBBINDING *SQLCommand::WithParameters::GetBinding() {
    DBBINDING *f = new DBBINDING[bindingList->size()];
    if (!f) {
        ClearMemory();
        throw SQLException::rException(wxT("cannot create DBPARAMBINDINFO"));
    }
    DBBINDING *p = f;
    std::vector<DBBINDING>::iterator iter = bindingList->begin();
    ULONG i = 1;
    for (; iter != bindingList->end(); iter++, p++, i++) {
        p->iOrdinal = i;
        p->obValue = (*iter).obValue;
        p->obLength = (*iter).obLength;
        p->obStatus = (*iter).obStatus;
        p->pTypeInfo = (*iter).pTypeInfo;
        p->pObject = (*iter).pObject;
        p->pBindExt = (*iter).pBindExt;
        p->dwPart = (*iter).dwPart;
        p->dwMemOwner = (*iter).dwMemOwner;
        p->eParamIO = (*iter).eParamIO;
        p->cbMaxLen = (*iter).cbMaxLen;
        p->dwFlags = (*iter).dwFlags;
        p->wType = (*iter).wType;
        p->bPrecision = (*iter).bPrecision;
        p->bScale = (*iter).bScale;
    }
    return f;
}

long SQLCommand::WithParameters::GetBindingRowSize() {
    if (r_bindingRowSize > 0) return r_bindingRowSize;

    std::vector<DBBINDING>::iterator iter = bindingList->begin();
    long sz = 0;
    for (; iter != bindingList->end(); iter++) {
        sz += (*iter).cbMaxLen;
        DWORD dwPart = (*iter).dwPart;

        if ((dwPart & DBPART_STATUS) != 0)
            sz += sizeof(DBSTATUS);
        if ((dwPart & DBPART_LENGTH) != 0)
            sz += sizeof(DBLENGTH);
    }
    return sz;
}

long SQLCommand::WithParameters::GetNumParams() {
    return bindingList->size();
}

DB_UPARAMS *SQLCommand::WithParameters::GetParamOrdinals() {
    DB_UPARAMS *res = new DB_UPARAMS[bindingList->size()];
    if (!res) {
        ClearMemory();
        throw SQLException::rException(wxT("cannot allocate memory"));
    }
    std::vector<DBBINDING>::iterator iter = bindingList->begin();
    long sz = 1;
    DB_UPARAMS *r = res;
    for (; iter != bindingList->end(); iter++) {
        *r++ = sz++;
    }
    return res;
}

void SQLCommand::Execute(WorkUnit::Session &s, const wxString &sql) {
    SQLCommand::Simple cmd(s, sql);
    cmd.Prepare();
    cmd.Execute();
}

long SQLCommand::GetLong(WorkUnit::Session &s, const wxString &sql) {
    SQLCommand::Simple cmd(s, sql);
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

/*
void SQLCommand::Simple::RetrieveSingletonResult( wxString &p, ... ) {
	va_list marker;
	std::stringv = p;

	RowSet::Base *rowset = GetRowSet();
	bool hasResult = (rowset != NULL);
	if (hasResult) {
		if (rowset->Get() == NULL) hasResult=false;
		else {
			rowset->Prepare(1);
			long k=rowset->GetNextRows();
			if (k>0) rowset->GetData(0);
			else hasResult=false;
		}
	}
	va_start(marker, p);
	int col=0;
	while ( v != NULL ) {
		if (hasResult)
			*v = rowset->GetColumnValue(col);
		else
			*v = "";
		v = va_arg( marker, std::string *);
		col++;
	}
	va_end(marker);
}
*/

#endif
