// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * Simple filesystem SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#include <filesystem>

#include "rgw_sal_simplefile.h"
#include "driver/simplefile/user.h"
#include "driver/simplefile/bucket.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

int SimpleFileUser::read_attrs(const DoutPrefixProvider *dpp,
                               optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  /** Read the User attributes from the backing Store */
  return -ENOTSUP;
}

int SimpleFileUser::merge_and_store_attrs(const DoutPrefixProvider *dpp,
                                          Attrs &new_attrs, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  /** Set the attributes in attrs, leaving any other existing attrs set, and
   * write them to the backing store; a merge operation */
  return -ENOTSUP;
}

int SimpleFileUser::read_stats(const DoutPrefixProvider *dpp, optional_yield y,
                               RGWStorageStats *stats,
                               ceph::real_time *last_stats_sync,
                               ceph::real_time *last_stats_update) {
  /** Read the User stats from the backing Store, synchronous */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::read_stats_async(const DoutPrefixProvider *dpp,
                                     RGWGetUserStats_CB *cb) {
  /** Read the User stats from the backing Store, asynchronous */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::complete_flush_stats(const DoutPrefixProvider *dpp,
                                         optional_yield y) {
  /** Flush accumulated stat changes for this User to the backing store */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::read_usage(
    const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool *is_truncated, RGWUsageIter &usage_iter,
    std::map<rgw_user_bucket, rgw_usage_log_entry> &usage) {
  /** Read detailed usage stats for this User from the backing store */
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::trim_usage(const DoutPrefixProvider *dpp,
                               uint64_t start_epoch, uint64_t end_epoch) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::load_user(const DoutPrefixProvider *dpp, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO (0)" << dendl;
  return 0;
}

int SimpleFileUser::store_user(const DoutPrefixProvider *dpp, optional_yield y,
                               bool exclusive, RGWUserInfo *old_info) {
  /** Store this User to the backing store */ ldpp_dout(dpp, 10)
      << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileUser::remove_user(const DoutPrefixProvider *dpp,
                                optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

static void populate_buckets_from_path(const SimpleFileStore& store, const DoutPrefixProvider *dpp, std::filesystem::path path, BucketList& buckets) {
  for (auto const &dir_entry : std::filesystem::directory_iterator{path}) {
    auto bucket =
        std::unique_ptr<Bucket>(new SimpleFileBucket{dir_entry.path(), store});
    bucket->load_bucket(dpp, null_yield);
    buckets.add(std::move(bucket));
  }
}

int SimpleFileUser::list_buckets(const DoutPrefixProvider *dpp,
                                 const std::string &marker,
                                 const std::string &end_marker, uint64_t max,
                                 bool need_stats, BucketList &buckets,
                                 optional_yield y) {
  // TODO this should list buckets assigned to a user. for now we just get every
  // bucket
  populate_buckets_from_path(store, dpp, store.buckets_path(), buckets);
  ldpp_dout(dpp, 10) << __func__ << ": TODO " << marker << ", " << end_marker
                     << ", "
                     << "max=" << max << ", "
                     << "buckets=" << buckets.get_buckets() << dendl;
  return 0;
}

int SimpleFileUser::create_bucket(
    const DoutPrefixProvider *dpp, const rgw_bucket &b,
    const std::string &zonegroup_id, rgw_placement_rule &placement_rule,
    std::string &swift_ver_location, const RGWQuotaInfo *pquota_info,
    const RGWAccessControlPolicy &policy, Attrs &attrs, RGWBucketInfo &info,
    obj_version &ep_objv, bool exclusive, bool obj_lock_enabled, bool *existed,
    req_info &req_info, std::unique_ptr<Bucket> *bucket, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileUser::verify_mfa(
    const std::string &mfa_str, bool *verified,
    const DoutPrefixProvider *dpp, optional_yield y
) {
  return 0;
}

} // ns rgw::sal