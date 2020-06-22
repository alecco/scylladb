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
#include "test/lib/log.hh"

#include "raft/fsm.hh"

using raft::term_t, raft::index_t, raft::server_id;


void election_timeout(raft::fsm& fsm) {
    for (int i = 0; i < 2 * fsm.ELECTION_TIMEOUT; i++) {
        fsm.tick();
    }
}

BOOST_AUTO_TEST_CASE(test_election_single_node) {

    raft::fsm fsm(server_id{utils::make_random_uuid()}, term_t{}, server_id{}, raft::log{});

    BOOST_CHECK(fsm.is_follower());

    election_timeout(fsm);

    // Immediately converts from leader to follower if quorum=1
    BOOST_CHECK(fsm.is_leader());

    auto output = fsm.get_output();

    BOOST_CHECK(output.term);
    BOOST_CHECK(output.vote);
    BOOST_CHECK(output.messages.empty());
    BOOST_CHECK(output.log_entries.empty());
    BOOST_CHECK(output.committed.empty());
    // The leader does not become candidate simply because
    // a timeout has elapsed, i.e. there are no spurious
    // elections.
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    BOOST_CHECK(!output.term);
    BOOST_CHECK(!output.vote);
    BOOST_CHECK(output.messages.empty());
    BOOST_CHECK(output.log_entries.empty());
    BOOST_CHECK(output.committed.empty());
}

// Test that adding an entry to a single-node cluster
// does not lead to RPC
BOOST_AUTO_TEST_CASE(test_single_node_is_quiet) {

    raft::fsm fsm(server_id{utils::make_random_uuid()}, term_t{}, server_id{}, raft::log{});

    election_timeout(fsm);

    // Immediately converts from leader to follower if quorum=1
    BOOST_CHECK(fsm.is_leader());

    (void) fsm.get_output();

    fsm.add_entry(raft::command{});

    BOOST_CHECK(fsm.get_output().messages.empty());
}

BOOST_AUTO_TEST_CASE(test_election_two_nodes) {

    server_id id1{utils::make_random_uuid()}, id2{utils::make_random_uuid()};

    raft::fsm fsm(id1, term_t{}, server_id{}, raft::log{});

    raft::configuration cfg;
    cfg.servers.push_back(raft::server_address{id1});
    cfg.servers.push_back(raft::server_address{id2});
    fsm.set_configuration(cfg);
    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // After election timeout, a follower becomes a candidate
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    // If nothing happens, the candidate stays this way
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    auto output = fsm.get_output();
    // After a favourable reply, we become a leader (quorum is 2)
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());
    // Out of order response to the previous election is ignored
    fsm.step(id2, raft::vote_reply{output.term - term_t{1}, false});
    assert(fsm.is_leader());
    // Any message with a newer term -> immediately convert to
    // follower
    fsm.step(id2, raft::vote_request{output.term + term_t{1}});
    BOOST_CHECK(fsm.is_follower());

    // Check that the candidate converts to a follower as well
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    fsm.step(id2, raft::vote_request{output.term + term_t{1}});
    BOOST_CHECK(fsm.is_follower());

    // Test that a node doesn't cast a vote if it has voted for
    // self already
    (void) fsm.get_output();
    while (fsm.is_follower()) {
        fsm.tick();
    }
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    auto msg = std::get<raft::vote_request>(output.messages.back().second);
    fsm.step(id2, std::move(msg));
    // We could figure out this round is going to a nowhere, but
    // we're not that smart and simply wait for a vote_reply.
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    auto reply = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(!reply.vote_granted);
}

BOOST_AUTO_TEST_CASE(test_election_four_nodes) {

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()},
              id4{utils::make_random_uuid()};

    raft::fsm fsm(id1, term_t{}, server_id{}, raft::log{});

    raft::configuration cfg;
    cfg.servers.push_back(raft::server_address{id1});
    cfg.servers.push_back(raft::server_address{id2});
    cfg.servers.push_back(raft::server_address{id3});
    cfg.servers.push_back(raft::server_address{id4});

    fsm.set_configuration(cfg);
    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Inform FSM about a new leader at a new term
    fsm.step(id4, raft::append_request_recv{term_t{1}, id4, index_t{1}, term_t{1}});

    (void) fsm.get_output();

    // Request a vote during the same term. Even though
    // we haven't voted, we should deny a vote because we
    // know about a leader for this term.
    fsm.step(id3, raft::vote_request{term_t{1}, index_t{1}, term_t{1}});

    auto output = fsm.get_output();
    auto reply = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(!reply.vote_granted);

    // Run out of steam for this term. Start a new one.
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    output = fsm.get_output();
    // Add a favourable reply, not enough for quorum
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_candidate());

    // Add another one, this adds up to quorum
    fsm.step(id3, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());
}
