#pragma once
//#define UNICODE
//#define _UNICODE

#if defined(_WIN32) && defined(__VISUALC__)
#define _WIN32_DCOM
#include <objbase.h>
#include <oledb.h>  // OLE DB include files
#include <oledberr.h>
#include <msdaguid.h>  // ODBC provider include files
#include <msdasql.h>
//#include <objbase.h>
#include <sqloledb.h>
#include <atlbase.h>
#include <stdlib.h>
#include <list>
#include <assert.h>
#include <fstream>

#include "wx/string.h"

namespace DBServer {
    //	extern CComPtr<IMalloc> g_pIMalloc;

    class Base {
        std::vector<DBPROP> *propertyList;

    public:
        CComPtr<IDBInitialize> pIDBInitialize;
        CComPtr<IMalloc> pIMalloc;

    protected:
        void AddProperty(DBPROPID id, long i);
        void AddProperty(DBPROPID id, unsigned long i);
        void AddProperty(DBPROPID id, short i);
        void AddProperty(DBPROPID id, const wxString &s);
        void AddProperty(DBPROPID id, bool i);

    public:
        Base(const wxString &vendorString);
        virtual ~Base();
        void Close(bool toThrow = true);
        void SetUser(const wxString &uid) { AddProperty((DBPROPID)DBPROP_AUTH_USERID, uid); }
        void SetUser(const wxString &uid, const wxString &pwd) {
            AddProperty((DBPROPID)DBPROP_AUTH_USERID, uid);
            AddProperty((DBPROPID)DBPROP_AUTH_PASSWORD, pwd);
        }
        void SetPassword(const wxString &pwd) { AddProperty((DBPROPID)DBPROP_AUTH_PASSWORD, pwd); }
        virtual void Init() {}
        void Open();
    };

    class MsSQL : public Base {
    public:
        MsSQL(const wxString &serverName, const wxString &dbName);
        void Init();
        void UseIntegratedSecurity();
    };

    class Access : public Base {
    public:
        Access(const wxString &dbName, HWND hWnd = 0);
        void Init();
    };

    class Jet : public Base {
    public:
        Jet(const wxString &dsName, HWND hWnd, const wxString &providerString = "Dbase 5.0");
        void Init();
    };

    class Excel : public Jet {
    public:
        Excel(const wxString &dsName, HWND hwnd) : Jet(dsName, hwnd, "Excel 8.0") {}
    };

    class Foxpro : public Base {
    public:
        Foxpro(const wxString &dsName, HWND hWnd);
        void Init();
    };
}
#endif
