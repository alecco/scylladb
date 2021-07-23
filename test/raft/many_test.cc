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

using update = std::variant<entries, new_leader, partition, disconnect1, disconnect2,
      stop, reset, wait_log, set_config, check_rpc_config, check_rpc_added,
      check_rpc_removed, rpc_reset_counters, tick>;

SEASTAR_THREAD_TEST_CASE(test_many_100) {
fmt::print("test_many 100\n"); // XXX
    replication_test<steady_clock_type>(
        {.nodes = 100, .total_values = 10,
         .updates = {entries{1},
                     disconnect1{0},    // drop leader, free election
                     entries{2},
                     }}
    , true, false, 100ms,
    delays{ .network_delay = 20ms, .local_delay = 1ms });
}

#if 1
SEASTAR_THREAD_TEST_CASE(test_many_300) {
fmt::print("test_many 300\n"); // XXX
    replication_test<steady_clock_type>(
        {.nodes = 300, .total_values = 10,
         .updates = {entries{1},
                     disconnect1{0},    // drop leader, free election
                     entries{2},
                     }}
    , true, false, 100ms,
    delays{ .network_delay = 20ms, .local_delay = 1ms });
}

SEASTAR_THREAD_TEST_CASE(test_many_600) {
fmt::print("test_many 600\n"); // XXX
    replication_test<steady_clock_type>(
        {.nodes = 600, .total_values = 10,
         .updates = {entries{1},
                     disconnect1{0},    // drop leader, free election
                     entries{2},
                     }}
    , true, false, 100ms,
    delays{ .network_delay = 20ms, .local_delay = 1ms });
}

SEASTAR_THREAD_TEST_CASE(test_many_700) {
fmt::print("test_many 700\n"); // XXX
    replication_test<steady_clock_type>(
        {.nodes = 700, .total_values = 10,
         .updates = {entries{1},
                     disconnect1{0},    // drop leader, free election
                     entries{2},
                     }}
    , true, false, 100ms,
    delays{ .network_delay = 20ms, .local_delay = 1ms });
}

SEASTAR_THREAD_TEST_CASE(test_many_800) {
fmt::print("test_many 800\n"); // XXX
    replication_test<steady_clock_type>(
        {.nodes = 800, .total_values = 10,
         .updates = {entries{1},
                     disconnect1{0},    // drop leader, free election
                     entries{2},
                     }}
    , true, false, 100ms,
    delays{ .network_delay = 20ms, .local_delay = 1ms });
}
#endif
