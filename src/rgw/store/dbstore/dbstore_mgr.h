// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <string>

#include "common/dbstore.h"

using namespace rgw::store;
using DB = rgw::store::DB;

/* XXX: Should be a dbstore config option */
const static std::string default_tenant = "default_ns";

class DBStoreManager {
private:
  std::map<std::string, DB*> db_store_handles_;
  CephContext *cct_ = nullptr;
  DB *default_db_ = nullptr;

public:
  DBStoreManager(CephContext *cct);
  DBStoreManager(CephContext *cct, std::string logfile, int loglevel);
  ~DBStoreManager();

  /* XXX: TBD based on testing
   * 1)  Lock to protect DBStoreHandles map.
   * 2) Refcount of each DBStore to protect from
   * being deleted while using it.
   */
  DB* getDB () { return default_db_; };
  DB* getDB (std::string tenant, bool create);
  DB* createDB();
  DB* createDB (std::string tenant);
  void deleteDB (std::string tenant);
  void deleteDB (DB* db);
  void destroyAllHandles();

private:
  std::string getDBFullPath() const;
  std::string getDBBasePath() const;
};
