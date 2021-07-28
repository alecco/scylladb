/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

// Test Raft library with many candidates
//
// Using slower but precise clock

#include "replication.hh"

#if 0
SEASTAR_THREAD_TEST_CASE(test_many_100) {
    replication_test<steady_clock_type>(
        {.nodes = 100, .total_values = 10,
         .updates = {entries{1},
                     isolate{0},    // drop leader, free election
                     entries{2},
                     }}
    , true, 100ms,
    rpc_config{ .network_delay = 20ms, .local_delay = 1ms });
}

SEASTAR_THREAD_TEST_CASE(test_many_400) {
    replication_test<steady_clock_type>(
        {.nodes = 400, .total_values = 10,
         .updates = {entries{1},
                     isolate{0},    // drop leader, free election
                     entries{2},
                     }}
    , true, 100ms,
    rpc_config{ .network_delay = 20ms, .local_delay = 1ms });
}
#endif

SEASTAR_THREAD_TEST_CASE(test_many_700) {
    replication_test<steady_clock_type>(
        {.nodes = 700, .total_values = 10,
         .updates = {entries{1},
                     isolate{0},    // drop leader, free election
                     entries{2},
                     }}
    , true, 100ms,
    rpc_config{ .network_delay = 20ms, .local_delay = 1ms });
}
