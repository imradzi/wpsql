#include "member_db.h"

int main() {
    MemberDb db;
    db.CheckSchemaAndRestructure(); // restructure table is schema changed. 
    return 0;
}