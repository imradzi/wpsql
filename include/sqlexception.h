#pragma once

#if defined(_WIN32) && defined(__MSVC__)
#include <oledb.h>  // OLE DB include files
#include <oledberr.h>
#endif

#include <fstream>

class StringException : public std::exception {
    std::string s;

public:
    StringException(const std::string &e) : s(e) {}
    ~StringException() {}
    const char *what() const noexcept { return s.c_str(); }
    std::string GetMessage() { return s; }
};

namespace SQLException {

    extern bool showWindow;
    extern bool showMessage;
    extern bool showLog;

    //	extern std::ofstream *fOutPtr;

    std::string ShowError(const std::string &s);

#if defined(_WIN32) && defined(__MSVC__)
    std::string DumpErrorInfo(IUnknown *pObjectWithError, REFIID IID_InterfaceWithError, bool toReleaseObject = true);
    std::string GetStatusName(DBSTATUS st);
#endif

    class rException {
    public:
        std::string message;

    public:
        rException(const std::string &s) : message(s) {} 
    };
}
