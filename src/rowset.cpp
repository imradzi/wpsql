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

#include "RowSet.h"
#include "sqlexception.h"
#include <atlbase.h>
#include <atlconv.h>
#include <iostream>
#include <iomanip>
#include <strstream>
#include <stdio.h>

//long max(long i, long j);

void RowSet::Base::SetStatus(DBSTATUS stt) {
    for (auto const &it : bindingList) {
        if (it.obStatus > 0) {
            DBSTATUS *p = (DBSTATUS *)((char *)GetDataOffset() + it.obStatus);
            *p = stt;
        }
    }
}

void RowSet::Base::SetStatus(DBSTATUS *r, DBSTATUS s) {
    for (int i = 0; i < GetNumberOfColumns(); i++, r++)
        *r = s;
}

long RowSet::Base::GetColumnSize(ULONG i) {
    i++;
    for (ULONG k = 0; k < nCols; k++)
        if (pColumnsInfo[k].iOrdinal == i)
            return pColumnsInfo[k].ulColumnSize;
    return 0;
}

wxString RowSet::Base::GetColumnName(ULONG i) {
    //	USES_CONVERSION;

    i++;
    for (ULONG k = 0; k < nCols; k++)
        if (pColumnsInfo[k].iOrdinal == i)
            return pColumnsInfo[k].pwszName;
    return wxT("<undefined>");
}

#ifdef USE_UltimateGrid
int RowSet::Base::GetUGColumnType(int i, int *type) {
    switch (GetColumnType(i)) {
        case DBTYPE_UI1:
        case DBTYPE_I2:
        case DBTYPE_UI2:
        case DBTYPE_I4:
        case DBTYPE_UI4:
        case DBTYPE_R4:
        case DBTYPE_R8:
        case DBTYPE_NUMERIC: *type = UGCELLDATA_NUMBER; break;
        case DBTYPE_STR:
        case DBTYPE_WSTR: *type = UGCELLDATA_STRING; break;
        case DBTYPE_DBDATE:
        case DBTYPE_DATE:
        case DBTYPE_DBTIMESTAMP: *type = UGCELLDATA_TIME; break;
        default: return UG_NA;
    }
    return UG_SUCCESS;
}

int RowSet::Base::GetCell(int i, CUGCell *cell) {
    //	USES_CONVERSION;

    std::strstream str;
    DBSTATUS status = GetColumnStatus(i);
    if (status == DBSTATUS_S_ISNULL) {
        cell->SetText("<null>");
        return UG_SUCCESS;
    }

    switch (GetColumnType(i)) {
        case DBTYPE_UI1:
        case DBTYPE_I2: cell->SetNumber(*(SHORT *)GetColumnValuePtr(i)); break;
        case DBTYPE_UI2: cell->SetNumber(*(USHORT *)GetColumnValuePtr(i)); break;
        case DBTYPE_I4: cell->SetNumber(*(LONG *)GetColumnValuePtr(i)); break;
        case DBTYPE_UI4: cell->SetNumber(*(ULONG *)GetColumnValuePtr(i)); break;
        case DBTYPE_NUMERIC: cell->SetNumber(atol((char *)GetColumnValuePtr(i))); break;
        case DBTYPE_STR: cell->SetText((char *)GetColumnValuePtr(i)); break;
        case DBTYPE_WSTR: {
            BSTR bs = (BSTR)GetColumnValuePtr(i);
            cell->SetText(OLE2T(bs));
            break;
        }
        case DBTYPE_R4: cell->SetNumber(*(float *)GetColumnValuePtr(i)); break;
        case DBTYPE_R8: cell->SetNumber(*(double *)GetColumnValuePtr(i)); break;
        case DBTYPE_DBDATE:
        case DBTYPE_DATE:
        case DBTYPE_DBTIMESTAMP: {
            DBTIMESTAMP *p = (DBTIMESTAMP *)GetColumnValuePtr(i);
            cell->SetTime(p->second, p->minute, p->hour, p->day, p->month, p->year);
            break;
        }
        default:
            return UG_NA;
    }
    return UG_SUCCESS;
}
#endif

wxString RowSet::Base::GetColumnValue(int i, bool isShort) {
    //	USES_CONVERSION;

    wxString buf;
    DBSTATUS status = GetColumnStatus(i);
    if (status == DBSTATUS_S_ISNULL) return wxT("<null>");

    switch (GetColumnType(i)) {
        case DBTYPE_UI1:
        case DBTYPE_I2: buf.Printf(wxT("%ld"), *(long *)GetColumnValuePtr(i)); break;
        case DBTYPE_UI2: buf.Printf(wxT("%ld"), *(unsigned long *)GetColumnValuePtr(i)); break;
        case DBTYPE_I4: buf.Printf(wxT("%ld"), *(long *)GetColumnValuePtr(i)); break;
        case DBTYPE_UI4: buf.Printf(wxT("%ld"), *(long *)GetColumnValuePtr(i)); break;
        case DBTYPE_NUMERIC:
        case DBTYPE_STR: buf = wxString((char *)GetColumnValuePtr(i), *wxConvCurrent); break;
        case DBTYPE_BSTR:
        case DBTYPE_WSTR: {
            buf = (BSTR)GetColumnValuePtr(i);
            break;
        }
        case DBTYPE_R4: buf.Printf(wxT("%.4f"), *(float *)GetColumnValuePtr(i)); break;
        case DBTYPE_R8: buf.Printf(wxT("%.4lf"), *(double *)GetColumnValuePtr(i)); break;
        case DBTYPE_DBDATE:
        case DBTYPE_DATE:
        case DBTYPE_DBTIMESTAMP: {
            DBTIMESTAMP *p = (DBTIMESTAMP *)GetColumnValuePtr(i);
            if (isShort)
                buf.Printf(wxT("%02d-%02d-%04d"), p->day, p->month, p->year);
            else
                buf.Printf(wxT("%02d-%02d-%04d %02d:%02d:%04d"), p->day, p->month, p->year, p->hour, p->minute, p->second);
            break;
        }
    }
    return buf;
}

long RowSet::Base::GetColumnLongValue(int i) {
    //	USES_CONVERSION;

    DBSTATUS status = GetColumnStatus(i);
    if (status == DBSTATUS_S_ISNULL) return 0;

    wxString str;

    switch (GetColumnType(i)) {
        case DBTYPE_UI1:
        case DBTYPE_I2: return *(SHORT *)GetColumnValuePtr(i);
        case DBTYPE_UI2: return *(USHORT *)GetColumnValuePtr(i);
        case DBTYPE_I4: return *(LONG *)GetColumnValuePtr(i);
        case DBTYPE_UI4: return *(ULONG *)GetColumnValuePtr(i);
        case DBTYPE_NUMERIC:
        case DBTYPE_STR: str = wxString((char *)GetColumnValuePtr(i), *wxConvCurrent); break;
        case DBTYPE_R4: str.Printf(wxT("%.4lf"), *(float *)GetColumnValuePtr(i)); break;
        case DBTYPE_R8: str.Printf(wxT("%.4lf"), *(double *)GetColumnValuePtr(i)); break;
        case DBTYPE_BSTR:
        case DBTYPE_WSTR: {
            str = (BSTR)GetColumnValuePtr(i);
            break;
        }
    }
    long rVal;
    if (!str.ToLong(&rVal))
        return 0;
    else
        return rVal;
}

double RowSet::Base::GetColumnFloatValue(int i) {
    //	USES_CONVERSION;

    DBSTATUS status = GetColumnStatus(i);
    if (status == DBSTATUS_S_ISNULL) return 0.0;

    wxString str;

    switch (GetColumnType(i)) {
        case DBTYPE_UI1:
        case DBTYPE_I2: return double(*(SHORT *)GetColumnValuePtr(i));
        case DBTYPE_UI2: return double(*(USHORT *)GetColumnValuePtr(i));
        case DBTYPE_I4: return double(*(LONG *)GetColumnValuePtr(i));
        case DBTYPE_UI4: return double(*(ULONG *)GetColumnValuePtr(i));
        case DBTYPE_NUMERIC:
        case DBTYPE_STR: str = wxString((char *)GetColumnValuePtr(i), *wxConvCurrent); break;
        case DBTYPE_R4: return *(float *)GetColumnValuePtr(i);
        case DBTYPE_R8: return *(double *)GetColumnValuePtr(i);
        case DBTYPE_BSTR:
        case DBTYPE_WSTR: {
            str = (BSTR)GetColumnValuePtr(i);
            break;
        }
    }
    double rVal;
    if (!str.ToDouble(&rVal))
        return 0.0;
    else
        return rVal;
}

RowSet::Base::Base() : pIRowset(NULL) {
    binding = NULL;
    nCols = 0;
    pColumnsInfo = NULL;
    pColumnStrings = NULL;
    rowHandles = NULL;
    nRowsObtained = 0;
    hAccessor = NULL;
    m_DataBuffer = NULL;
    pIRowsetChange = NULL;
    pIRowsetUpdate = NULL;
    pIAccessor = NULL;
}

RowSet::Base::Base(IRowset *p) : pIRowset(p) {
    toReleaseRowset = false;
    binding = NULL;
    pColumnsInfo = NULL;
    pColumnStrings = NULL;
    rowHandles = NULL;
    hAccessor = NULL;
    pIRowsetChange = NULL;
    pIRowsetUpdate = NULL;
    pIAccessor = NULL;

    IColumnsInfo *pIColumnsInfo;
    nCols = 0;

    if (FAILED(pIRowset->QueryInterface(IID_IColumnsInfo, (void **)&pIColumnsInfo))) {
        wxString err = SQLException::DumpErrorInfo(pIRowset, IID_IRowset);
        throw SQLException::rException(err);
    }

    HRESULT hr = pIColumnsInfo->GetColumnInfo(&nCols, &pColumnsInfo, &pColumnStrings);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pIColumnsInfo, IID_IColumnsInfo);
        throw SQLException::rException(wxString::Format(wxT("GetColumnInfo failed: %s"), err));
    }
    if (FAILED(CoGetMalloc(MEMCTX_TASK, &pIMalloc))) throw SQLException::rException(wxT("Cannot get task memory allocator"));
    pIColumnsInfo->Release();
    nRowsObtained = 0;
}

void RowSet::Base::Set(IRowset *p) {
    if (nRowsObtained != 0) {
        pIRowset->ReleaseRows(nRowsObtained, rowHandles, NULL, NULL, NULL);
    }

    if (rowHandles != NULL) {
        delete[] rowHandles;
        rowHandles = NULL;
    }
    if (pIAccessor != NULL && hAccessor != NULL) {
        pIAccessor->ReleaseAccessor(hAccessor, NULL);
        pIAccessor->Release();
    }
    if (binding != NULL) {
        delete[] binding;
        binding = NULL;
    }

    if (pColumnsInfo != NULL) {
        pIMalloc->Free(pColumnsInfo);
        pColumnsInfo = NULL;
    }
    if (pColumnStrings != NULL) {
        pIMalloc->Free(pColumnStrings);
        pColumnStrings = NULL;
    }
    if (toReleaseRowset && pIRowset != NULL) {
        pIRowset->Release();
        pIRowset = NULL;
    }

    toReleaseRowset = true;
    nRowsObtained = 0;
    pIRowset = p;
    nCols = 0;

    if (p == NULL) return;

    IColumnsInfo *pIColumnsInfo;

    if (FAILED(pIRowset->QueryInterface(IID_IColumnsInfo, (void **)&pIColumnsInfo))) {
        wxString err = SQLException::DumpErrorInfo(pIRowset, IID_IRowset);
        throw SQLException::rException(err);
    }

    HRESULT hr = pIColumnsInfo->GetColumnInfo(&nCols, &pColumnsInfo, &pColumnStrings);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pIColumnsInfo, IID_IColumnsInfo);
        throw SQLException::rException(wxString::Format(wxT("GetColumnInfo failed. %s"), err));
    }
    if (FAILED(CoGetMalloc(MEMCTX_TASK, &pIMalloc))) throw SQLException::rException(wxT("Cannot get task memory allocator"));
    pIColumnsInfo->Release();
}

RowSet::Base::~Base() {
    if (nRowsObtained != 0) {
        pIRowset->ReleaseRows(nRowsObtained, rowHandles, NULL, NULL, NULL);
    }
    if (pIRowsetChange != NULL) pIRowsetChange->Release();
    if (pIRowsetUpdate != NULL) pIRowsetUpdate->Release();

    if (rowHandles != NULL) {
        delete[] rowHandles;
        rowHandles = NULL;
    }
    if (pIAccessor != NULL && hAccessor != NULL) {
        pIAccessor->ReleaseAccessor(hAccessor, NULL);
        pIAccessor->Release();
    }

    if (binding != NULL) {
        delete[] binding;
        binding = NULL;
    }
    if (toReleaseRowset && pIRowset != NULL) pIRowset->Release();

    if (pColumnsInfo != NULL) pIMalloc->Free(pColumnsInfo);
    if (pColumnStrings != NULL) pIMalloc->Free(pColumnStrings);
}

void RowSet::Base::Prepare(long rowNo) {
    maxRow = 0;
    currentRowOffset = -1;

    Init();

    HRESULT hr = pIRowset->QueryInterface(IID_IAccessor, (void **)&pIAccessor);
    if (FAILED(hr)) {
        wxString err = SQLException::DumpErrorInfo(pIRowset, IID_IRowset);
        throw SQLException::rException(wxString::Format(wxT("Getting IAccessor:%s"), err));
    }

    pIRowsetChange = NULL;
    hr = pIRowset->QueryInterface(IID_IRowsetChange, (void **)&pIRowsetChange);
    if (FAILED(hr)) pIRowsetChange = NULL;

    pIRowsetUpdate = NULL;
    hr = pIRowset->QueryInterface(IID_IRowsetUpdate, (void **)&pIRowsetUpdate);
    if (FAILED(hr)) pIRowsetUpdate = NULL;

    long nColumns = GetNumberOfColumns();
    binding = GetBinding();
    DBBINDSTATUS *bindingStatus = new DBBINDSTATUS[nColumns];
    if (!bindingStatus) throw SQLException::rException(wxT("cannot allocate memory"));
    nRowChunk = rowNo;
    hr = pIAccessor->CreateAccessor(
        DBACCESSOR_ROWDATA,  // Accessor will be used to retrieve row of data
        nColumns,            // Number of columns being bound
        binding,             // Structure containing bind info
        0,                   // Not used for row accessors
        &hAccessor,          // Returned accessor handle
        bindingStatus        // Information about binding validity
    );

    if (FAILED(hr)) {
        wxString msg;
        for (int iB = 0; iB < GetNumberOfColumns(); iB++) {
            if (bindingStatus[iB] == DBBINDSTATUS_OK) {
            } else if (bindingStatus[iB] == DBBINDSTATUS_BADORDINAL) {
                msg.Append(wxT("column "));
                msg.Append(wxString::Format(wxT("%ld"), (iB + 1)));
                msg.Append(wxT(" -> BAD ORDINAL\n"));
            } else if (bindingStatus[iB] == DBBINDSTATUS_UNSUPPORTEDCONVERSION) {
                msg.Append(wxT("column "));
                msg.Append(wxString::Format(wxT("%ld"), (iB + 1)));
                msg.Append(wxT(" -> UNSUPPORTED CONVERSION\n"));
            } else if (bindingStatus[iB] == DBBINDSTATUS_BADBINDINFO) {
                msg.Append(wxT("column "));
                msg.Append(wxString::Format(wxT("%ld"), (iB + 1)));
                msg.Append(wxT(" -> DBBINDSTATUS_BADBINDINFO\n"));
            } else if (bindingStatus[iB] == DBBINDSTATUS_BADSTORAGEFLAGS) {
                msg.Append(wxT("column "));
                msg.Append(wxString::Format(wxT("%ld"), (iB + 1)));
                msg.Append(wxT(" -> DBBINDSTATUS_BADSTORAGEFLAGS\n"));
            } else if (bindingStatus[iB] == DBBINDSTATUS_NOINTERFACE) {
                msg.Append(wxT("column "));
                msg.Append(wxString::Format(wxT("%ld"), (iB + 1)));
                msg.Append(wxT(" -> DBBINDSTATUS_NOINTERFACE\n"));
            }
        }
        delete[] bindingStatus;
        wxString err = SQLException::DumpErrorInfo(pIAccessor, IID_IAccessor);
        throw SQLException::rException(wxString::Format(wxT("Creating Accessor: %s - %s"), err, msg));
    }

    delete[] bindingStatus;

    if (rowNo > 0) {
        rowHandles = new HROW[rowNo];
        if (!rowHandles) throw SQLException::rException(wxT("row handle is NULL!"));
    } else
        rowHandles = NULL;
}

long RowSet::Base::GetNextRows(long nRowsToSkip) {
    HRESULT hr;
    if (nRowsObtained != 0) {
        hr = pIRowset->ReleaseRows(nRowsObtained, rowHandles, NULL, NULL, NULL);
    }

    nRowsObtained = 0;

    if (rowHandles != NULL) {
        hr = pIRowset->GetNextRows(
            0,               // Reserved
            nRowsToSkip,     // cRowsToSkip
            nRowChunk,       // cRowsDesired
            &nRowsObtained,  // cRowsObtained
            &rowHandles      // Filled in with row handles.
        );

        //		TRACE("pIRowset->GetNextRows(0,skip=%ld,chunk=%ld,obtained=%ld) ",nRowsToSkip, nRowChunk, nRowsObtained);

        if (hr == DB_E_BADSTARTPOSITION || hr == DB_S_ENDOFROWSET) {
            sawEOF = true;
            //			TRACE(" EOF found!\n");
            return nRowsObtained;
        } else if (FAILED(hr)) {
            //			TRACE(" FAILED!\n");
            wxString err = SQLException::DumpErrorInfo(pIRowset, IID_IRowset);
            throw SQLException::rException(wxString::Format(wxT("Getting Rows :%s"), err));
        } else {
            sawEOF = (nRowsObtained < nRowChunk);
            //			TRACE(" saw eof = %d !\n", sawEOF);
        }
    }
    return nRowsObtained;
}

void RowSet::Base::GetData(long rowNo) {
    pIRowset->GetData(rowHandles[rowNo], hAccessor, GetDataOffset());
}

void RowSet::Base::SetData(long rowNo) {
    if (pIRowsetChange != NULL) {
        HRESULT hr = pIRowsetChange->SetData(rowHandles[rowNo], hAccessor, GetDataOffset());
        if (FAILED(hr)) {
            wxString err = SQLException::DumpErrorInfo(pIRowsetChange, IID_IRowsetChange);
            throw SQLException::rException(wxString::Format(wxT("SetData: %s"), err));
        }
    }
}

void RowSet::Base::Delete(long rowNo) {
    if (pIRowsetChange != NULL) {
        HRESULT hr = pIRowsetChange->DeleteRows(DB_NULL_HCHAPTER, 1, rowHandles + rowNo, NULL);
        if (FAILED(hr)) {
            wxString err = SQLException::DumpErrorInfo(pIRowsetChange, IID_IRowsetChange);
            throw SQLException::rException(wxString::Format(wxT("Deleting Row: %s"), err));
        }
    }
}

void RowSet::Base::Insert(void *buffer) {
    //	HROW insertedRow;
    //	HRESULT hr = pIRowsetChange->InsertRow( DB_NULL_HCHAPTER, hAccessor, GetDataOffset(), &insertedRow);
    //	if need to read the inserted row the last param can be used in GetData() to retrieve inserted record);

    if (pIRowsetChange != NULL) {
        if (buffer == NULL) buffer = GetDataOffset();
        HRESULT hr = pIRowsetChange->InsertRow(DB_NULL_HCHAPTER, hAccessor, buffer, NULL);
        if (FAILED(hr)) {
            wxString err = SQLException::DumpErrorInfo(pIRowsetChange, IID_IRowsetChange);
            throw SQLException::rException(wxString::Format(wxT("InsertRows: %s"), err));
        }
    }
}

void RowSet::Base::Update() {
    if (pIRowsetUpdate != NULL) {
        DBCOUNTITEM nRows;
        HROW *hRow;
        DBROWSTATUS *rowStatus;
        HRESULT hr = pIRowsetUpdate->Update(DB_NULL_HCHAPTER, nRowsObtained, rowHandles, &nRows, &hRow, &rowStatus);
        if (FAILED(hr)) {
            wxString err = SQLException::DumpErrorInfo(pIRowsetUpdate, IID_IRowsetUpdate);
            throw SQLException::rException(wxString::Format(wxT("Updating Rows:%s"), err));
        }
        if (hRow != NULL) pIMalloc->Free(hRow);
        if (rowStatus != NULL) pIMalloc->Free(rowStatus);
    }
}

//long max(long i, long j) {
//	if (j > i) return j;
//	return i;
//}

bool RowSet::Base::GetDataAt(ULONG rowNo) {
    long i = rowNo % nRowChunk;
    long ofs = rowNo / nRowChunk * nRowChunk;  // get the remainder....
    long rowsToSkip = ofs - currentRowOffset;

    //	TRACE("GetDataAt(%ld/%ld):: MaxRow = %ld, CurrentRowOffset = %ld, i=%ld, ofs=%ld, rowsToSkip=%ld\n", rowNo, nRowChunk, maxRow, currentRowOffset, i, ofs, rowsToSkip);

    if (currentRowOffset < 0) {
        currentRowOffset = 0;
        GetNextRows(0);
        //		TRACE("max(maxrows=%ld, new=%ld)->", maxRow, ofs+nRowsObtained);
        maxRow = max(maxRow, ofs + nRowsObtained);
        //		TRACE("%ld  <---- first run.\n", maxRow);
    }

    if (long(rowNo) >= currentRowOffset && rowNo < currentRowOffset + nRowChunk) {
        GetData(i);
        return true;
    }

    if (rowsToSkip == 0) throw SQLException::rException(wxT("row to skip is ZERO!"));

    if (GetNextRows(rowsToSkip) > 0) {
        GetData(i);
        currentRowOffset = ofs;
        //		TRACE("max(maxrows=%ld, new=%ld)->", maxRow, ofs+nRowsObtained);
        maxRow = max(maxRow, ofs + nRowsObtained);
        //		TRACE("%ld\n", maxRow);
    } else
        return false;
    return true;
}

DBBINDING *RowSet::Base::GetBinding() {
    DBBINDING *f = new DBBINDING[bindingList.size()];
    if (!f) throw SQLException::rException(wxT("cannot create DBBINDING"));
    DBBINDING *p = f;
    std::vector<DBBINDING>::iterator iter = bindingList.begin();
    ULONG i = 1;
    for (; iter != bindingList.end(); iter++, p++, i++) {
        p->iOrdinal = (*iter).iOrdinal;
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

void RowSet::Base::AddColumn(int iOrd, double v, long offsetValue, long offsetStatus, long offsetLength) {
    //	USES_CONVERSION;
    auto &b = bindingList.emplace_back();
    b.iOrdinal = iOrd;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength > 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    b.eParamIO = DBPARAMIO_NOTPARAM;
    b.cbMaxLen = sizeof(v);
    b.dwFlags = 0;
    b.wType = DBTYPE_R8;
    b.bPrecision = 23;
    b.bScale = 5;
}

void RowSet::Base::AddColumn(int iOrd, float v, long offsetValue, long offsetStatus, long offsetLength) {
    //	USES_CONVERSION;
    auto &b = bindingList.emplace_back();
    b.iOrdinal = iOrd;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength > 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    b.eParamIO = DBPARAMIO_NOTPARAM;
    b.cbMaxLen = sizeof(v);
    b.dwFlags = 0;
    b.wType = DBTYPE_R4;
    b.bPrecision = 15;
    b.bScale = 4;
}

void RowSet::Base::AddColumn(int iOrd, int v, long offsetValue, long offsetStatus, long offsetLength) {
    //	USES_CONVERSION;
    auto &b = bindingList.emplace_back();
    b.iOrdinal = iOrd;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength > 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    b.eParamIO = DBPARAMIO_NOTPARAM;
    b.cbMaxLen = sizeof(v);
    b.dwFlags = 0;
    b.wType = DBTYPE_I2;
    b.bPrecision = 9;
    b.bScale = 0;
}

void RowSet::Base::AddColumn(int iOrd, long v, long offsetValue, long offsetStatus, long offsetLength) {
    //	USES_CONVERSION;
    auto &b = bindingList.emplace_back();
    b.iOrdinal = iOrd;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength > 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    b.eParamIO = DBPARAMIO_NOTPARAM;
    b.cbMaxLen = sizeof(v);
    b.dwFlags = 0;
    b.wType = DBTYPE_I4;
    b.bPrecision = 20;
    b.bScale = 0;
}

void RowSet::Base::AddColumn(int iOrd, DBTIMESTAMP v, long offsetValue, long offsetStatus, long offsetLength) {
    //	USES_CONVERSION;
    auto &b = bindingList.emplace_back();
    b.iOrdinal = iOrd;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength > 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    b.eParamIO = DBPARAMIO_NOTPARAM;
    b.cbMaxLen = sizeof(v);
    b.dwFlags = 0;
    b.wType = DBTYPE_DBTIMESTAMP;
    b.bPrecision = 20;
    b.bScale = 0;
}

void RowSet::Base::AddColumn(int iOrd, char * /*v*/, long offsetValue, long len, long offsetStatus, long offsetLength) {
    //	USES_CONVERSION;
    auto &b = bindingList.emplace_back();
    b.iOrdinal = iOrd;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength > 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    b.eParamIO = DBPARAMIO_NOTPARAM;
    b.cbMaxLen = len;
    b.dwFlags = 0;
    b.wType = DBTYPE_STR;
    b.bPrecision = len;
    b.bScale = 0;
}

void RowSet::Base::AddColumn(int iOrd, wchar_t * /*v*/, long offsetValue, long len, long offsetStatus, long offsetLength) {
    //	USES_CONVERSION;
    auto &b = bindingList.emplace_back();
    b.iOrdinal = iOrd;
    b.obValue = offsetValue;
    b.obLength = offsetLength;
    b.obStatus = offsetStatus;
    b.pTypeInfo = NULL;
    b.pObject = NULL;
    b.pBindExt = NULL;
    b.dwPart = DBPART_VALUE;
    if (offsetStatus >= 0) b.dwPart |= DBPART_STATUS;
    if (offsetLength > 0) b.dwPart |= DBPART_LENGTH;
    b.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    b.eParamIO = DBPARAMIO_NOTPARAM;
    b.cbMaxLen = len;
    b.dwFlags = 0;
    b.wType = DBTYPE_WSTR;
    b.bPrecision = len;
    b.bScale = 0;
}

void RowSet::All::Init() {
    long lVal = 0;
    long offset = 0;
    double dVal = 0;
    float fVal = 0;

    DBTIMESTAMP dbTime;
    memset(&dbTime, 0, sizeof(dbTime));
    wchar_t wStr[10];
    char s[10];
    wxString str;

    wxString v;

    for (ULONG nCol = 0; nCol < nCols; nCol++) {
        switch (pColumnsInfo[nCol].wType) {
            case DBTYPE_BOOL:
            case DBTYPE_I1:
            case DBTYPE_UI1:
            case DBTYPE_UI2:
            case DBTYPE_I2:
                AddColumn(nCol + 1, lVal, offset, offset + sizeof(lVal));
                offset += sizeof(lVal) + sizeof(DBSTATUS);
                break;
            case DBTYPE_R8:
            case DBTYPE_CY:
                AddColumn(nCol + 1, dVal, offset, offset + sizeof(dVal));
                offset += sizeof(dVal) + sizeof(DBSTATUS);
                break;
            case DBTYPE_R4:
                AddColumn(nCol + 1, fVal, offset, offset + sizeof(fVal));
                offset += sizeof(fVal) + sizeof(DBSTATUS);
                break;
            case DBTYPE_VARNUMERIC:
            case DBTYPE_NUMERIC:
            case DBTYPE_I4:
            case DBTYPE_UI4:
                AddColumn(nCol + 1, lVal, offset, offset + sizeof(lVal));
                offset += sizeof(lVal) + sizeof(DBSTATUS);
                break;
            case DBTYPE_DBDATE:
            case DBTYPE_DATE:
            case DBTYPE_DBTIME:
            case DBTYPE_DBTIMESTAMP:
                AddColumn(nCol + 1, dbTime, offset, offset + sizeof(dbTime));
                offset += sizeof(dbTime) + sizeof(DBSTATUS);
                break;
            case DBTYPE_WSTR: {
                int len = pColumnsInfo[nCol].bPrecision + 1;
                AddColumn(nCol + 1, wStr, offset, len, offset + len);
                offset += len + sizeof(DBSTATUS);
                break;
            }
            case DBTYPE_STR: {
                int len = pColumnsInfo[nCol].ulColumnSize + 1;
                AddColumn(nCol + 1, s, offset, len, offset + len);
                offset += len + sizeof(DBSTATUS);
                break;
            }
            default: {
                str.Printf(wxT("%ld "), pColumnsInfo[nCol].wType);
                v += str;
            }
        }
    }
    if (!v.empty()) {
        //		wxMessageBox(v, wxT("Unhandled column type"), wxICON_ERROR);
        throw SQLException::rException(wxT("Bug to fix"));
    }
    data = new char[offset + 1];
    if (!data) throw SQLException::rException(wxT("cannot allocate memory for rowset data"));
}

RowSet::All::~All() {
    if (data) {
        delete[] data;
        data = NULL;
    }
}

/*
	DBTYPE_EMPTY = 0,
	DBTYPE_NULL = 1,
	DBTYPE_I2 = 2,
	DBTYPE_UI1 = 17,
	DBTYPE_I1 = 16,
	DBTYPE_UI2 = 18,

	DBTYPE_I4 = 3,
	DBTYPE_UI4 = 19,
	DBTYPE_R4 = 4,
	DBTYPE_R8 = 5,
	DBTYPE_CY = 6,
	DBTYPE_DATE = 7,
	DBTYPE_BSTR = 8,
	DBTYPE_IDISPATCH = 9,
	DBTYPE_ERROR = 10,
	DBTYPE_BOOL = 11,
	DBTYPE_VARIANT = 12,
	DBTYPE_IUNKNOWN = 13,
	DBTYPE_DECIMAL = 14,
	DBTYPE_ARRAY = 0x2000,
	DBTYPE_BYREF = 0x4000,

	// The following values exactly match VARENUM
	// in Automation but cannot be used in VARIANT.
	DBTYPE_I8 = 20,
	DBTYPE_UI8 = 21,
	DBTYPE_GUID = 72,
	DBTYPE_VECTOR = 0x1000,
	DBTYPE_FILETIME = 64,
	DBTYPE_RESERVED = 0x8000,

	// The following values are not in VARENUM in OLE.
	DBTYPE_BYTES = 128,
	DBTYPE_STR = 129,
	DBTYPE_WSTR = 130,
	DBTYPE_NUMERIC = 131,
	DBTYPE_UDT = 132,
	DBTYPE_DBDATE = 133,
	DBTYPE_DBTIME = 134,
	DBTYPE_DBTIMESTAMP = 135,
	DBTYPE_HCHAPTER = 136,
	DBTYPE_PROPVARIANT = 138,
	DBTYPE_VARNUMERIC = 139,
*/

#endif