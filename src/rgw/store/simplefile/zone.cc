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
#include "rgw_sal_simplefile.h"
#include "store/simplefile/zone.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

const RGWZoneGroup &SimpleFileZone::get_zonegroup() { return *zonegroup; }

int SimpleFileZone::get_zonegroup(const std::string &id, RGWZoneGroup &zg) {
  zg = *zonegroup;
  return 0;
}

const RGWZoneParams &SimpleFileZone::get_params() { return *zone_params; }

const rgw_zone_id &SimpleFileZone::get_id() { return cur_zone_id; }

const RGWRealm &SimpleFileZone::get_realm() { return *realm; }

const std::string &SimpleFileZone::get_name() const {
  return zone_params->get_name();
}

bool SimpleFileZone::is_writeable() { return true; }

bool SimpleFileZone::get_redirect_endpoint(std::string *endpoint) {
  return false;
}

bool SimpleFileZone::has_zonegroup_api(const std::string &api) const {
  return false;
}

const std::string &SimpleFileZone::get_current_period_id() {
  return current_period->get_id();
}

SimpleFileZone::SimpleFileZone(SimpleFileStore *_store) : store(_store) {
  realm = new RGWRealm();
  zonegroup = new RGWZoneGroup();
  zone_public_config = new RGWZone();
  zone_params = new RGWZoneParams();
  current_period = new RGWPeriod();
  cur_zone_id = rgw_zone_id(zone_params->get_id());
  RGWZonePlacementInfo info;
  RGWZoneStorageClasses sc;
  sc.set_storage_class("STANDARD", nullptr, nullptr);
  info.storage_classes = sc;
  zone_params->placement_pools["default"] = info;
}

} // ns rgw::sal