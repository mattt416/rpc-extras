#!/usr/bin/env python

# Copyright 2015, Rackspace US, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
import maas_common
import subprocess


STATUSES = {'HEALTH_OK': 2, 'HEALTH_WARN': 1, 'HEALTH_ERR': 0}

def check_command(command):
    output = subprocess.check_output(command, stderr=subprocess.STDOUT)
    lines = output.strip().split('\n')
    return json.loads(lines[-1])


def get_ceph_report(client, keyring, fmt='json'):
    return check_command(('ceph', '--format', fmt, '--name', client, '--keyring', keyring, 'report'))


def get_mon_statistics(report=None):
    mon = [m for m in report['health']['health']['health_services'][0]['mons'] if m['name'] == args.host][0]
    maas_common.metric('health', 'uint32', STATUSES[mon['health']])
    for key, m_type in (("kb_total", "uint64"), ("kb_used", "uint64"),
                        ("kb_avail", "uint64"), ("avail_percent", "uint32")):
        maas_common.metric(key, m_type, mon[key])


def get_osd_statistics(report=None, osd_id=None):
    osd_ref = 'osd.%s' % osd_id
    for _osd in report['osdmap']['osds']:
        if _osd['osd'] == osd_id:
            osd = _osd
            break
    for key in ('up', 'in'):
        name = '_'.join((osd_ref, key))
        maas_common.metric_bool(name, osd[key])

    for _osd in report['pgmap']['osd_stats']:
        if _osd['osd'] == osd_id:
            osd = _osd
            break
    for key in ('kb', 'kb_used', 'kb_avail'):
        name = '_'.join((osd_ref, key))
        maas_common.metric(name, 'uint64', osd[key])


def get_cluster_statistics(report=None):
    metrics = []
    osds = {'total': 0, 'up': 0, 'in': 0}
    osds_stats = ('kb', 'kb_avail', 'kb_used')
    pgs = {'total': 0, 'active_clean': 0}
    for osd in report['osdmap']['osds']:
        osds['total'] += 1
        if osd['up'] == 1:
            osds['up'] += 1
        if osd['in'] == 1:
            osds['in'] += 1
    for pg in report['pgmap']['pg_stats']:
        pgs['total'] += 1
        if pg['state'] == 'active+clean':
            pgs['active_clean'] += 1
    metrics.append({'name': 'cluster_health', 'type': 'uint32', 'value': STATUSES[report['health']['overall_status']]})
    for k in osds:
        metrics.append({'name': 'osds_%s' % k, 'type': 'uint32', 'value': osds[k]})
    for k in report['pgmap']['osd_stats_sum']:
        if k in osds_stats:
            metrics.append({'name': 'osds_%s' % k, 'type': 'uint64', 'value': report['pgmap']['osd_stats_sum'][k]})
    for k in pgs:
        metrics.append({'name': 'pgs_%s' % k, 'type': 'uint32', 'value': pgs[k]})
    for m in metrics:
        maas_common.metric(m['name'], m['type'], m['value'])


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', choices=['cluster', 'mon', 'osd'], help='the type of data to return')
    parser.add_argument('--osd_id', type=int, help='the osd id')
    parser.add_argument('--host', help='hostname')
    parser.add_argument('--name', required=True, help='Ceph client name')
    parser.add_argument('--keyring', required=True, help='Ceph client keyring')
    return parser.parse_args()


def main(args):
    get_statistics = {'cluster': get_cluster_statistics, 'mon': get_mon_statistics, 'osd': get_osd_statistics}
    report = get_ceph_report(client=args.name, keyring=args.keyring)
    kwargs = {'report': report}
    if args.osd_id is not None:
        kwargs['osd_id'] = args.osd_id
    get_statistics[args.type](**kwargs)
    maas_common.status_ok()


if __name__ == '__main__':
    with maas_common.print_output():
        args = get_args()
        main(args)
