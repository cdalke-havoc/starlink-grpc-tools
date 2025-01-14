#!/usr/bin/env python3
"""Publish Starlink user terminal data over InfluxDB line protocol.

This script pulls the current status info and/or metrics computed from the
history data and publishes them over UDP in InfluxDB line protocol.

Each data point will follow the following syntax:
https://docs.influxdata.com/influxdb/v1/write_protocols/line_protocol_tutorial/

weather,location=us-midwest temperature=82 1465839830100400200
  |    -------------------- --------------  |
  |             |             |             |
  |             |             |             |
+-----------+--------+-+---------+-+---------+
|measurement|,tag_set| |field_set| |timestamp|
+-----------+--------+-+---------+-+---------+
"""

import json
import logging
import math
import os
import signal
import sys
import time
import socket

try:
    import ssl
    ssl_ok = True
except ImportError:
    ssl_ok = False

import dish_common

HOST_DEFAULT = "127.0.0.1"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
target_udp_host = "127.0.0.1"
target_udp_port = 8094

class Terminated(Exception):
    pass


def handle_sigterm(signum, frame):
    # Turn SIGTERM into an exception so main loop can clean up
    raise Terminated


def parse_args():
    parser = dish_common.create_arg_parser(output_description="publish data to InfluxDB Line protocol (UDP)",
                                           bulk_history=False)

    opts = dish_common.run_arg_parser(parser, need_id=True, no_stdout_errors=True)
    return opts

def loop_body(opts, gstate):
    msgs = []
    data = {}

    def cb_add_item(key, val, category):
        if not "dish_{0}".format(category) in data:
            data["dish_{0}".format(category)] = {}

        # Skip NaN values that occur on startup because they can upset Javascript JSON parsers
        if not (isinstance(val, float) and math.isnan(val)):
            data["dish_{0}".format(category)].update({key: val})

    def cb_add_sequence(key, val, category, _):
        if not "dish_{0}".format(category) in data:
            data["dish_{0}".format(category)] = {}

        data["dish_{0}".format(category)].update({key: list(val)})

    # Get dish data
    rc = dish_common.get_data(opts, gstate, cb_add_item, cb_add_sequence)[0]
    if (rc == 1):
        data["online"] = False
        # Error
        return rc
    else:
        data["online"] = True

    # Construct messages from dish JSON that are more conveniently grouped:
    # - dish_status: Basic status data
    # - dish_alerts: Alert data

    #del data["wedges_fraction_obstructed"]
    #del data["raw_wedges_fraction_obstructed"]

    if data["online"]:
        data = data["dish_status"]

        raw_dish_status_msg = "starlink_dish_status "
        raw_dish_alerts_msg = "starlink_dish_alerts "
        for key in data.keys():
            value = data[key]
            value_type = type(value).__name__

            if (value is None):
                continue

            if (value_type == "bool"):
               value = "true" if value else "false"
            if (value_type == "int"):
                value = "{}i".format(value)
            if (value_type == "list"):
                continue
            if (value_type == "str"):
                value = "\"{}\"".format(value)
            data[key] = value

            if key.startswith("alert_"):
                raw_dish_alerts_msg += "{}={},".format(key, data[key])
            else:
                raw_dish_status_msg += "{}={},".format(key, data[key])
        raw_dish_status_msg = raw_dish_status_msg[:-1] + " "
        raw_dish_alerts_msg = raw_dish_alerts_msg[:-1] + " "

        raw_dish_status_msg += str(int(time.time() * 1000.0 * 1000.0))
        raw_dish_alerts_msg += str(int(time.time() * 1000.0 * 1000.0))
        #print(raw_dish_status_msg)
        #print(raw_dish_alerts_msg)
        sock.sendto((raw_dish_status_msg).encode(), (target_udp_host, target_udp_port))
        sock.sendto((raw_dish_alerts_msg).encode(), (target_udp_host, target_udp_port))

    else:
        pass

    # print(json.dumps(data))
    return rc


def main():#
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s")

    gstate = dish_common.GlobalState(target=opts.target)

    signal.signal(signal.SIGTERM, handle_sigterm)

    rc = 0
    try:
        next_loop = time.monotonic()
        while True:
            rc = loop_body(opts, gstate)
            if opts.loop_interval > 0.0:
                now = time.monotonic()
                next_loop = max(next_loop + opts.loop_interval, now)
                time.sleep(next_loop - now)
            else:
                break
    except (KeyboardInterrupt, Terminated):
        pass
    finally:
        gstate.shutdown()

    sys.exit(rc)


if __name__ == "__main__":
    main()
