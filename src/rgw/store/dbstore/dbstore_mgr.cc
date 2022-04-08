// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <filesystem>

#include "dbstore_mgr.h"
#include "common/dbstore_log.h"
#include "sqlite/sqliteDB.h"

using namespace std;
DBStoreManager::DBStoreManager(CephContext *cct)
  : cct_(cct)
  , default_db_(createDB()) {
}

DBStoreManager::DBStoreManager(CephContext *cct, std::string logfile, int loglevel)
  : cct_(cct) 
  , default_db_(createDB()) {
  /* No ceph context. Create one with log args provided */
  cct_->_log->set_log_file(logfile);
  cct_->_log->reopen_log_file();
  cct_->_conf->subsys.set_log_level(ceph_subsys_rgw, loglevel);
}

DBStoreManager::~DBStoreManager() { 
  destroyAllHandles(); 
}

/* Given a tenant, find and return the DBStore handle.
 * If not found and 'create' set to true, create one
 * and return
 */
DB *DBStoreManager::getDB (string tenant, bool create)
{
  if (tenant.empty())
    return default_db_;

  auto iter = db_store_handles_.find(tenant);
  if (iter == db_store_handles_.end()) {
    if (!create) {
      return nullptr;
    }
    return createDB(tenant);
  }
  return iter->second;
}

DB* DBStoreManager::createDB() {
  const auto& db_path = getDBFullPath();
  ldout(cct_, 0) << "Creating DB with full path: (" << db_path <<")" << dendl;
  return createDB(db_path);
}

/* Create DBStore instance */
DB *DBStoreManager::createDB(string tenant) {
  DB *dbs = nullptr;
  /* Create the handle */
#ifdef SQLITE_ENABLED
  dbs = new SQLiteDB(tenant, cct_);
#else
  dbs = new DB(tenant, cct_);
#endif

  /* API is DB::Initialize(string logfile, int loglevel);
   * If none provided, by default write in to dbstore.log file
   * created in current working directory with loglevel L_EVENT.
   * XXX: need to align these logs to ceph location
   */
  if (dbs->Initialize("", -1) < 0) {
    ldout(cct_, 0) << "DB initialization failed for tenant("<<tenant<<")" << dendl;

    delete dbs;
    return nullptr;
  }

  /* XXX: Do we need lock to protect this map?
  */
  auto ret = db_store_handles_.insert(pair<string, DB*>(tenant, dbs));

  /*
   * Its safe to check for already existing entry (just
   * incase other thread raced and created the entry)
   */
  if (ret.second == false) {
    /* Entry already created by another thread */
    delete dbs;

    dbs = ret.first->second;
  }

  return dbs;
}

void DBStoreManager::deleteDB(string tenant) {
  if (tenant.empty() || db_store_handles_.empty())
    return;

  /* XXX: Check if we need to perform this operation under a lock */
  auto iter = db_store_handles_.find(tenant);

  if (iter == db_store_handles_.end())
    return;

  DB *dbs = iter->second;

  db_store_handles_.erase(iter);
  dbs->Destroy(dbs->get_def_dpp());
  delete dbs;
}

void DBStoreManager::deleteDB(DB *dbs) {
  if (!dbs)
    return;

  deleteDB(dbs->getDBname());
}


void DBStoreManager::destroyAllHandles(){
  if (db_store_handles_.empty())
    return;

  for (auto& [key, dbs]: db_store_handles_) {
    dbs->Destroy(dbs->get_def_dpp());
    delete dbs;
  }
  db_store_handles_.clear();
}

std::string DBStoreManager::getDBFullPath() const {
  auto rgw_data = cct_->_conf.get_val<std::string>("rgw_data");
  auto db_path = std::filesystem::path(rgw_data) / default_tenant;
  return db_path.string();
}

std::string DBStoreManager::getDBBasePath() const {
  return cct_->_conf.get_val<std::string>("rgw_data");
}
