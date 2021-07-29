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

SEASTAR_THREAD_TEST_CASE(test_many_2500) {
    replication_test<steady_clock_type>(
        {.nodes = 2500, .total_values = 10,
         .updates = {entries{1},
                     isolate{0},              // drop leader, free election
                     entries{2},
                     new_leader{1},
                     set_config{1,2,3,4,5},   // Set configuration to smaller size
                     }}
    , true, 100ms,
    rpc_config{ .network_delay = 20ms, .local_delay = 1ms });
}
