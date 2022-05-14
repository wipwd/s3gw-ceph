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
#ifndef RGW_STORE_SIMPLEFILE_ZONE_H
#define RGW_STORE_SIMPLEFILE_ZONE_H

#include "rgw_sal.h"

namespace rgw::sal {

class SimpleFileStore;

class SimpleFileZone : public Zone {
 protected:
  SimpleFileStore *store;
  RGWRealm *realm{nullptr};
  RGWZoneGroup *zonegroup{nullptr};
  RGWZone *zone_public_config{nullptr};
  RGWZoneParams *zone_params{nullptr};
  RGWPeriod *current_period{nullptr};
  rgw_zone_id cur_zone_id;

 public:
  SimpleFileZone(const SimpleFileZone&) = delete;
  SimpleFileZone& operator= (const SimpleFileZone&) = delete;
  SimpleFileZone(SimpleFileStore *_store);
  ~SimpleFileZone() = default;

  virtual const RGWZoneGroup &get_zonegroup() override;
  virtual int get_zonegroup(const std::string &id,
                            RGWZoneGroup &zonegroup) override;
  virtual const RGWZoneParams &get_params() override;
  virtual const rgw_zone_id &get_id() override;
  virtual const RGWRealm &get_realm() override;
  virtual const std::string &get_name() const override;
  virtual bool is_writeable() override;
  virtual bool get_redirect_endpoint(std::string *endpoint) override;
  virtual bool has_zonegroup_api(const std::string &api) const override;
  virtual const std::string &get_current_period_id() override;
};

} // ns rgw::sal

#endif // RGW_STORE_SIMPLEFILE_ZONE_H
