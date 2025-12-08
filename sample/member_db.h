#pragma once
#include "include/rDb.h"

class MemberDb : public DB::SQLiteBase {
protected:
    std::vector<DB::DBObjects> objectList() const override;

public:
    MemberDb() : DB::SQLiteBase("members.db") {}
    virtual ~MemberDb() { Close(); }
    void CheckStructure() override;
};
