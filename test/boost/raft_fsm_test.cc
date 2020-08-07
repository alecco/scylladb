/*
 * Copyright (c) 2020, Arm Limited and affiliates. All rights reserved.
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

#define BOOST_TEST_MODULE raft

#include <boost/test/unit_test.hpp>

#include "raft/fsm.hh"

using raft::term_t, raft::index_t, raft::server_id;

BOOST_AUTO_TEST_CASE(test_election_timeout) {

    raft::fsm fsm(server_id{utils::make_random_uuid()}, term_t{}, server_id{}, raft::log{});

    BOOST_CHECK(fsm.is_follower());

    for (int i = 0; i < 2 * fsm.ELECTION_TIMEOUT; i++) {
        fsm.tick();
    }
    // Immediately converts from leader to follower if quorum=1
    BOOST_CHECK(fsm.is_leader());
}
