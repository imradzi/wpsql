#include "member_db.h"

std::vector<DB::DBObjects> MemberDb::objectList() const {
  std::vector<DB::DBObjects> list{
      //
      {"ul_keys",
       DB::Table,
       {
           "create table <TABLENAME>("
           "  id integer primary key, "
           "  key text, "
           "  value text, "
           "  description text, "
           "  isDeleted integer, "
           "  unique(key)"
           ")",
           "create index idx_<TABLENAME>_key on <TABLENAME>(key)",
       }},

      {"UL_LocalKeys",
       DB::Table,
       {
           "create table <TABLENAME>("
           "  id integer primary key, "
           "  key text, "
           "  value text, "
           "  description text, "
           "  isDeleted integer, "
           "  unique(key) "
           ")",
           "create index idx_<TABLENAME>_key on <TABLENAME>(key)",
       }},

      {"Types",
       DB::Table,
       {
           "create table <TABLENAME>("
           "  id integer primary key, "
           "  parentID integer, "
           "  code text, "
           "  name text, "
           "  limitvalue text, "
           "  defaultvalue text, "
           "  isDeleted integer, "
           "  foreign key(parentID) references types(id))",
       }},

      {"MemberFTS",
       DB::Table,
       {
           "create virtual table <TABLENAME> using FTS5("
           "   name, "
           "   IC, "
           "   telNo, "
           "   email)",
       }},

      {"Members",
       DB::Table,
       {
           "create table <TABLENAME> ("
           "  id blob primary key, " // ulid
           "  dob integer, "
           "  noOfTrans integer, "
           "  timeCreated integer "
           ")",
       }},
      {"MemberTransactions",
       DB::Table,
       {
           "create table <TABLENAME> ("
           "  id blob primary key, " // ulid
           "  amount integer, " // in cents
           "  timeCreated integer "
           ")",
       }}

  };
  auto ol = DB::SQLiteBase::objectList();
  ol.insert(ol.end(), list.begin(), list.end());
  return ol;
}

void MemberDb::CheckStructure() { DB::SQLiteBase::CheckStructure(); }
