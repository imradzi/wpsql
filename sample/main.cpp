#include "member_db.h"

int main() {
    MemberDb db;
    DB::Logger::initialize("MemberService", "debug");
    db.Open(true); // open DB, create if not exist
    return 0;
}