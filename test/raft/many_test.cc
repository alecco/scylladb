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

#include "replication.hh"

// Using slower but precise clock
size_t tick_delta_n =    100;
seastar::steady_clock_type::duration tick_delta = tick_delta_n * 1ms; // 100ms
auto network_delay =      30ms;           //  1/3rd  of tick
auto local_delay =         1ms;           // same host latency
auto extra_delay_max_n =    500;          // extra randomized per rpc delay (us)
uint64_t local_mask = ~((1l<<32) - 1);    // prefix mask for nodes (shards) per server


using update = std::variant<entries, new_leader, partition, disconnect1, disconnect2,
      stop, reset, wait_log, set_config, check_rpc_config, check_rpc_added,
      check_rpc_removed, rpc_reset_counters, tick>;

SEASTAR_THREAD_TEST_CASE(test_take_snapshot_and_stream_prevote) {
    replication_test<steady_clock_type>(
        {                      .nodes = 600, .total_values = 10,
         .updates = {entries{1},
                     disconnect1{0},             // drop leader, free election
                     entries{2},
                     }}
    , true, false, tick_delta);
}

