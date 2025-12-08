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
#include "dbserver.h"
#include "sqlexception.h"
#include <atlbase.h>
#include <atlconv.h>
#include <iostream>
#include <time.h>
#include "words.h"

const wxString MSSQL_OLEDB = "SQLOLEDB";
const wxString MSSQL_2005_NATIVE_CLIENT = "SQLNCLI";  // native SQL 2005
const wxString ORACLE_OLEDB = "OraOLEDB.Oracle.1";
const wxString MSACCESS_OLEDB = "Microsoft.ACE.OLEDB.15.0";  // "Microsoft.Jet.OLEDB.4.0";
const wxString MSJET_OLEDB = "Microsoft.Jet.OLEDB.4.0";
const wxString MSODBC_OLEDB = "MSDASQL.1";
const wxString FOXPRO_OLEDB = "VFPOLEDB";

//CComPtr<IMalloc> DBServer::g_pIMalloc;
//std::ofstream DBServer::Base::fOut = NULL; //("server.log");

/*void DBServer::Base::TraceOut(const char *s) {
	if (fOut == NULL) return;

	time_t ltime;

	time( &ltime );
	char *buf = ctime( &ltime );
	buf[strlen(buf)-1] = '\0';
	fOut << buf << ": " << s << '\n';

}
*/

DBServer::MsSQL::MsSQL(const wxString &serverName, const wxString &dbName) : Base(MSSQL_OLEDB) {
    AddProperty((DBPROPID)DBPROP_INIT_DATASOURCE, serverName);
    AddProperty((DBPROPID)DBPROP_INIT_CATALOG, dbName);
}

void DBServer::MsSQL::Init() {
    Base::Init();
    AddProperty((DBPROPID)DBPROP_INIT_PROMPT, (short)DBPROMPT_NOPROMPT);
    AddProperty((DBPROPID)DBPROP_AUTH_ENCRYPT_PASSWORD, true);
    AddProperty((DBPROPID)DBPROP_AUTH_CACHE_AUTHINFO, true);
    AddProperty((DBPROPID)DBPROP_AUTH_PERSIST_SENSITIVE_AUTHINFO, true);
    AddProperty((DBPROPID)DBPROP_AUTH_PERSIST_ENCRYPTED, true);
}

void DBServer::MsSQL::UseIntegratedSecurity() {
    AddProperty((DBPROPID)DBPROP_AUTH_INTEGRATED, wxString(wxT("")));  // "SSPI" );
}

DBServer::Access::Access(const wxString &dbName, HWND hWnd) : Base(MSACCESS_OLEDB) {
    AddProperty((DBPROPID)DBPROP_INIT_HWND, reinterpret_cast<unsigned long>(hWnd));
    AddProperty((DBPROPID)DBPROP_INIT_DATASOURCE, dbName);
}
void DBServer::Access::Init() {
    Base::Init();
    AddProperty((DBPROPID)DBPROP_INIT_PROMPT, (short)DBPROMPT_NOPROMPT);
    AddProperty((DBPROPID)DBPROP_INIT_MODE, (long)DB_MODE_READ);
    AddProperty((DBPROPID)DBPROP_AUTH_USERID, wxT("Admin"));
}

DBServer::Jet::Jet(const wxString &dsName, HWND hWnd, const wxString &providerString) : Base(MSJET_OLEDB) {
    AddProperty((DBPROPID)DBPROP_INIT_HWND, reinterpret_cast<unsigned long>(hWnd));
    AddProperty((DBPROPID)DBPROP_INIT_DATASOURCE, dsName);
    AddProperty((DBPROPID)DBPROP_INIT_PROVIDERSTRING, providerString);
}
void DBServer::Jet::Init() {
    Base::Init();
    AddProperty((DBPROPID)DBPROP_INIT_PROMPT, (short)DBPROMPT_NOPROMPT);
    AddProperty((DBPROPID)DBPROP_INIT_MODE, (long)DB_MODE_READ);
}

DBServer::Foxpro::Foxpro(const wxString &dsName, HWND hWnd) : Base(FOXPRO_OLEDB) {
    AddProperty((DBPROPID)DBPROP_INIT_HWND, reinterpret_cast<unsigned long>(hWnd));
    AddProperty((DBPROPID)DBPROP_INIT_DATASOURCE, dsName);
    AddProperty((DBPROPID)DBPROP_INIT_PROVIDERSTRING, wxT("Dbase 5.0"));
}

void DBServer::Foxpro::Init() {
    Base::Init();
    AddProperty((DBPROPID)DBPROP_INIT_PROMPT, (short)DBPROMPT_NOPROMPT);
    AddProperty((DBPROPID)DBPROP_INIT_MODE, (long)DB_MODE_READ);
}

DBServer::Base::Base(const wxString &vString) {
    // Get the task memory allocator.
    //	USES_CONVERSION;

    if (FAILED(CoGetMalloc(MEMCTX_TASK, &pIMalloc))) {
        throw SQLException::rException(wxT("Cannot get task memory allocator"));
    }
    //	g_pIMalloc = pIMalloc;

    CLSID p;
    CLSIDFromProgID(vString.wc_str(*wxConvCurrent), &p);
    HRESULT hr = CoCreateInstance(p, NULL, CLSCTX_INPROC_SERVER, IID_IDBInitialize, (void **)&pIDBInitialize);
    if (hr != S_OK) {
        wxString v(wxT("Class "));
        v.Append(vString);
        if (hr == REGDB_E_CLASSNOTREG) {
            v.Append(wxT(" not registered!"));
            throw SQLException::rException(v);
        } else if (hr == CLASS_E_NOAGGREGATION) {
            v.Append(wxT(" not aggregated!"));
            throw SQLException::rException(v);
        } else if (hr == E_NOINTERFACE) {
            v.Append(wxT(" has no interface!"));
            throw SQLException::rException(v);
        } else {
            v.Append(wxT(" other error!"));
            throw SQLException::rException(v);
        }
    }
    propertyList = new std::vector<DBPROP>;
    if (!propertyList) throw SQLException::rException(wxT("cannot allocate memory"));
}

void DBServer::Base::Close(bool toThrow) {
    if (pIDBInitialize && FAILED(pIDBInitialize->Uninitialize())) {
        wxString err = SQLException::DumpErrorInfo(pIDBInitialize, IID_IDBInitialize);
        if (toThrow) throw SQLException::rException(wxString::Format(wxT("IDBInitialze->Uninitialize failed : %s"), err));
    }
    pIDBInitialize = NULL;
}

void DBServer::Base::Open() {
    Init();  // set all property lists.
    std::vector<DBPROP>::size_type nProps = propertyList->size();

    DBPROP *initProperties = new DBPROP[nProps];
    if (!initProperties) throw SQLException::rException(wxT("cannot allocate memory"));

    DBPROP *p = initProperties;

    std::vector<DBPROP>::iterator iter = propertyList->begin();
    for (; iter != propertyList->end(); iter++, p++) {
        p->colid = (*iter).colid;
        p->dwOptions = (*iter).dwOptions;
        p->dwPropertyID = (*iter).dwPropertyID;
        p->dwStatus = (*iter).dwStatus;
        p->vValue = (*iter).vValue;
    }

    DBPROPSET rgInitPropSet;

    rgInitPropSet.guidPropertySet = DBPROPSET_DBINIT;
    rgInitPropSet.cProperties = nProps;
    rgInitPropSet.rgProperties = initProperties;

    // Set initialization properties.

    CComPtr<IDBProperties> pIDBProperties;
    HRESULT hr;

    pIDBInitialize->QueryInterface(IID_IDBProperties, (void **)&pIDBProperties);
    hr = pIDBProperties->SetProperties(1, &rgInitPropSet);

    delete[] initProperties;
    initProperties = NULL;

    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pIDBProperties, IID_IDBProperties);
        pIDBInitialize = NULL;
        throw SQLException::rException(wxString::Format(wxT("Set properties failed! %s"), err));
    }

    if (FAILED(pIDBInitialize->Initialize())) {
        wxString err = SQLException::DumpErrorInfo(pIDBInitialize, IID_IDBInitialize);
        //		pIDBInitialize = NULL;
        throw SQLException::rException(wxString::Format(wxT("IDBInitialze->Initialize failed! %s"), err));
    }

    iter = propertyList->begin();

    for (; iter != propertyList->end(); iter++) {
        if ((*iter).vValue.vt == VT_BSTR)
            ::SysFreeString((*iter).vValue.bstrVal);
    }
    delete propertyList;
    propertyList = NULL;
}

void DBServer::Base::AddProperty(DBPROPID id, long i) {
    DBPROP &prop = propertyList->emplace_back();
    VariantInit(&prop.vValue);
    prop.dwPropertyID = id;
    prop.dwOptions = DBPROPOPTIONS_REQUIRED;
    prop.colid = DB_NULLID;
    prop.vValue.vt = VT_I4;
    prop.vValue.lVal = i;
}

void DBServer::Base::AddProperty(DBPROPID id, unsigned long i) {
    DBPROP &prop = propertyList->emplace_back();
    VariantInit(&prop.vValue);
    prop.dwPropertyID = id;
    prop.dwOptions = DBPROPOPTIONS_REQUIRED;
    prop.colid = DB_NULLID;
    prop.vValue.vt = VT_I4;
    prop.vValue.ulVal = i;
}

void DBServer::Base::AddProperty(DBPROPID id, short i) {
    DBPROP &prop = propertyList->emplace_back();
    VariantInit(&prop.vValue);
    prop.dwPropertyID = id;
    prop.dwOptions = DBPROPOPTIONS_REQUIRED;
    prop.colid = DB_NULLID;
    prop.vValue.vt = VT_I2;
    prop.vValue.iVal = i;
}

void DBServer::Base::AddProperty(DBPROPID id, const wxString &s) {
    DBPROP &prop = propertyList->emplace_back();
    VariantInit(&prop.vValue);
    prop.dwPropertyID = id;
    prop.dwOptions = DBPROPOPTIONS_REQUIRED;
    prop.colid = DB_NULLID;
    prop.vValue.vt = VT_BSTR;
    if (s.IsEmpty()) {
        prop.vValue.bstrVal = (BSTR)NULL;
    } else {
        prop.vValue.bstrVal = ::SysAllocString(s.wc_str(*wxConvCurrent));
    }
}

void DBServer::Base::AddProperty(DBPROPID id, bool i) {
    DBPROP &prop = propertyList->emplace_back();
    VariantInit(&prop.vValue);
    prop.dwPropertyID = id;
    prop.dwOptions = DBPROPOPTIONS_REQUIRED;
    prop.colid = DB_NULLID;
    prop.vValue.vt = VT_BOOL;
    prop.vValue.boolVal = i ? VARIANT_TRUE : VARIANT_FALSE;
}

DBServer::Base::~Base() {
    if (propertyList)
        delete propertyList;
    if (pIDBInitialize != NULL) {
        if (FAILED(pIDBInitialize->Uninitialize())) {
            wxString err = SQLException::DumpErrorInfo(pIDBInitialize, IID_IDBInitialize);
            //TODO-what to do here?
            //throw SQLException::rException(wxString::Format(wxT("IDBInitialze->Uninitialize failed! %s"), err));
        }
    }
}
#endif