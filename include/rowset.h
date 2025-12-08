#pragma once
#include "dbserver.h"
#ifdef USE_UltimateGrid
#include "ugctrl.h"
#endif

#if defined(_WIN32) && defined(__VISUALC__)
namespace RowSet {
    class Base {
        CComPtr<IMalloc> pIMalloc;

    protected:
        DBBINDING *binding;
        IRowset *pIRowset;
        bool toReleaseRowset;
        DBCOLUMNINFO *pColumnsInfo;
        OLECHAR *pColumnStrings;
        IAccessor *pIAccessor;
        IRowsetChange *pIRowsetChange;
        IRowsetUpdate *pIRowsetUpdate;
        HACCESSOR hAccessor;
        HROW *rowHandles;
        DBCOUNTITEM nRowsObtained;
        DBROWCOUNT nRowChunk;

        std::vector<DBBINDING> bindingList;

        bool sawEOF;
        long currentRowOffset;
        long maxRow;

        void *m_DataBuffer;

    protected:
        DBORDINAL nCols;
        virtual void Init() {}

        HROW GetRowHandle(long rowNo) { return rowHandles[rowNo]; }
        void *GetColumnValuePtr(int i) { return (char *)GetDataOffset() + binding[i].obValue; }

    public:
        Base();
        Base(IRowset *p);
        virtual ~Base();
        void Set(IRowset *p);
        virtual IRowset *Get() { return pIRowset; }
        virtual void Prepare(long noOfRows = 1);

        virtual long GetNextRows(long nRowsToSkip = 0);

        virtual void GetData(long rowNo);
        virtual void SetData(long rowNo);
        virtual void Delete(long rowNo);
        virtual void Insert(void *buffer = NULL);
        virtual void Update();

        virtual bool GetDataAt(ULONG rowNo);
        virtual bool SeenEOF() { return sawEOF; }
        virtual long GetMaxRow() { return maxRow; }

        wxString GetColumnName(ULONG i);
        virtual DBSTATUS GetColumnStatus(int i) { return *(DBSTATUS *)((char *)GetDataOffset() + binding[i].obStatus); }
        virtual DBTYPE GetColumnType(int i) { return binding[i].wType; }
#ifdef USE_UltimateGrid
        virtual int GetUGColumnType(int i, int *type);
        virtual int GetCell(int col, CUGCell *cell);
#endif
        virtual long GetColumnSize(ULONG i);
        virtual wxString GetColumnValue(int i, bool isShort = true);
        virtual long GetColumnLongValue(int i);
        virtual double GetColumnFloatValue(int i);
        virtual int GetNumberOfColumns() { return int(bindingList.size()); }

        virtual void SetStatus(DBSTATUS stt);
        virtual void SetStatus(DBSTATUS *r, DBSTATUS s);

        void SetDataOffset(void *p) { m_DataBuffer = p; }
        void AddColumn(int iCol, int v, long offsetValue, long offsetStatus = -1, long offsetLength = -1);
        void AddColumn(int iCol, long v, long offsetValue, long offsetStatus = -1, long offsetLength = -1);
        void AddColumn(int iCol, double v, long offsetValue, long offsetStatus = -1, long offsetLength = -1);
        void AddColumn(int iCol, float v, long offsetValue, long offsetStatus = -1, long offsetLength = -1);
        void AddColumn(int iCol, char *v, long offsetValue, long len, long offsetStatus = -1, long offsetLength = -1);
        void AddColumn(int iCol, DBTIMESTAMP v, long offsetValue, long offsetStatus = -1, long offsetLength = -1);
        void AddColumn(int iCol, wchar_t *v, long offsetValue, long len, long offsetStatus = -1, long offsetLength = -1);

    protected:
        DBBINDING *GetBinding();
        virtual void *GetDataOffset() { return m_DataBuffer; }
    };

    class All : public Base {
        char *data;
        void Init();
        virtual void *GetDataOffset() { return data; }

    public:
        All() : Base(),
                data(NULL) {}
        All(IRowset *p) : Base(p),
                          data(NULL) {}
        virtual ~All();
    };

    class Long : public Base {
    protected:
        void Init() { RowSet::Base::AddColumn(1, number.number, offsetof(Param, number), offsetof(Param, stt)); }
        virtual void *GetDataOffset() { return &number; }

    public:
        struct Param {
            long number;
            DBSTATUS stt;

        public:
            long &operator()() {
                if (!IsValid()) number = 0;
                return number;
            }
            bool IsValid() { return stt == DBSTATUS_S_OK; }
        } number;

    public:
        Long() : Base() {}
        Long(IRowset *p) : Base(p) {}
    };

    class Double : public Base {
    protected:
        void Init() { RowSet::Base::AddColumn(1, number.number, offsetof(Param, number), offsetof(Param, stt)); }
        virtual void *GetDataOffset() { return &number; }

    public:
        struct Param {
            double number;
            DBSTATUS stt;

        public:
            double &operator()() {
                if (!IsValid()) number = 0;
                return number;
            }
            bool IsValid() { return stt == DBSTATUS_S_OK; }
        } number;

    public:
        Double() : Base() {}
        Double(IRowset *p) : Base(p) {}
    };
    class String : public Base {
    protected:
        void Init() { RowSet::Base::AddColumn(1, str.str, offsetof(Param, str), sizeof(str.str), offsetof(Param, stt)); }
        virtual void *GetDataOffset() { return &str; }

    public:
        struct Param {
            wxChar str[1024];
            DBSTATUS stt;

        public:
            wxString operator()() {
                if (!IsValid()) *str = '\0';
                return str;
            }
            bool IsValid() { return stt == DBSTATUS_S_OK; }
        } str;

    public:
        String() : Base() {}
        String(IRowset *p) : Base(p) {}
    };
}
#endif