#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use


class HarnessCli():
    """Client for Scylla pytest Cluster Harness"""

    def __init__(self, subnet, server_port=10000, server_subnet_addr="254"):
        self.subnet = subnet   # e.g. "127.1.1"
        self.server_port = server_port
        self.server_subnet_addr = server_subnet_addr
        self.server_addr = f"{subnet}.{self.server_subnet_addr}" # XXX
