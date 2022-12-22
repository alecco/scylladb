
#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test repro of failure to store mutation with schema change and a server down
"""
from test.topology.test_mutation_schema_change import test_mutation_schema_change, \
        test_mutation_schema_change_restart
