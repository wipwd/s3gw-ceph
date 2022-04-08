// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "rgw/store/dbstore/dbstore_mgr.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <memory>

using namespace rgw;
namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_dbstore_tests";

class TestDBStoreManager : public ::testing::Test {
protected:
  void SetUp() override {
    fs::current_path(fs::temp_directory_path());
    fs::create_directory(TEST_DIR);
  }

  void TearDown() override {
    fs::current_path(fs::temp_directory_path());
    fs::remove_all(TEST_DIR);
  }

  std::string getTestDir() const {
    auto test_dir = fs::temp_directory_path() / TEST_DIR;
    return test_dir.string();
  }

  fs::path getDBFullPath(const std::string & base_dir) const {
    auto db_full_name = default_tenant + ".db";
    auto db_full_path = fs::path(base_dir) /  db_full_name;
    return db_full_path;
  }

  std::string getDBTenant(const std::string & base_dir) const {
    auto db_full_path = fs::path(base_dir) /  default_tenant;
    return db_full_path.string();
  }

  std::string getDBTenant() const {
    return getDBTenant(getTestDir());
  }

  fs::path getDBFullPath() const {
    return getDBFullPath(getTestDir());
  }

  fs::path getLogFilePath(const std::string & log_file) {
    return fs::temp_directory_path() / log_file;
  }
};

TEST_F(TestDBStoreManager, BasicInstantiate) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_data", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto dbstore_mgr = std::make_shared<DBStoreManager>(ceph_context.get());
  EXPECT_TRUE(fs::exists(getDBFullPath()));
}

TEST_F(TestDBStoreManager, BasicInstantiateSecondConstructor) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_data", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto dbstore_mgr = std::make_shared<DBStoreManager>(ceph_context.get(), getLogFilePath("test.log").string(), 10);
  EXPECT_TRUE(fs::exists(getDBFullPath()));
}

TEST_F(TestDBStoreManager, TestDBName) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_data", getTestDir());

  auto dbstore_mgr = std::make_shared<DBStoreManager>(ceph_context.get());
  auto db = dbstore_mgr->getDB(getDBTenant(), false);
  ASSERT_NE(nullptr, db);
  EXPECT_EQ(getDBTenant(), db->getDBname());
}

TEST_F(TestDBStoreManager, TestDBNameDefaultDB) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_data", getTestDir());

  auto dbstore_mgr = std::make_shared<DBStoreManager>(ceph_context.get());
  // passing an empty tenant should return the default_db
  auto db = dbstore_mgr->getDB("", false);
  ASSERT_NE(nullptr, db);
  EXPECT_EQ(getDBTenant(), db->getDBname());
}

TEST_F(TestDBStoreManager, TestGetNewDB) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_data", getTestDir());

  auto dbstore_mgr = std::make_shared<DBStoreManager>(ceph_context.get());

  auto new_tenant_path = fs::path(getTestDir()) / "new_tenant";
  auto db = dbstore_mgr->getDB(new_tenant_path.string(), true);
  ASSERT_NE(nullptr, db);
  EXPECT_EQ(new_tenant_path.string(), db->getDBname());
}

TEST_F(TestDBStoreManager, TestDBNameDefaultDBNoTenant) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_data", getTestDir());

  auto dbstore_mgr = std::make_shared<DBStoreManager>(ceph_context.get());
  auto db = dbstore_mgr->getDB();
  ASSERT_NE(nullptr, db);
  EXPECT_EQ(getDBTenant(), db->getDBname());
}

TEST_F(TestDBStoreManager, TestDelete) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_data", getTestDir());

  auto dbstore_mgr = std::make_shared<DBStoreManager>(ceph_context.get());
  dbstore_mgr->deleteDB(getDBTenant());
  auto db = dbstore_mgr->getDB(getDBTenant(), false);
  ASSERT_EQ(nullptr, db);
}


