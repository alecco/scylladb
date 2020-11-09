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

// Port of etcd Raft implementation unit tests

#define BOOST_TEST_MODULE raft

#include <boost/test/unit_test.hpp>
#include "test/lib/log.hh"
#include "serializer.hh"
#include <limits>

#include "raft/fsm.hh"

using raft::term_t, raft::index_t, raft::server_id;

void election_threshold(raft::fsm& fsm) {
    for (int i = 0; i <= raft::ELECTION_TIMEOUT.count(); i++) {
        fsm.tick();
    }
}

void election_timeout(raft::fsm& fsm) {
    for (int i = 0; i <= 2 * raft::ELECTION_TIMEOUT.count(); i++) {
        fsm.tick();
    }
}

struct failure_detector: public raft::failure_detector {
    bool alive = true;
    bool is_alive(raft::server_id from) override {
        return alive;
    }
};

template <typename T>
raft::command create_command(T val) {
    raft::command command;
    ser::serialize(command, val);

    return std::move(command);
}

raft::fsm_config fsm_cfg{.append_request_threshold = 1};

class fsm_debug : public raft::fsm {
public:
    using raft::fsm::fsm;
    raft::follower_progress& get_progress(server_id id) {
        raft::follower_progress& progress = _tracker->find(id);
        return progress;
    }
};


#if 0
// TestProgressLeader
BOOST_AUTO_TEST_CASE(test_progress_leader) {

    failure_detector fd;

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};

    raft::configuration cfg({id1, id2});                 // 2 nodes
    raft::log log{raft::snapshot{.config = cfg}};

    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    auto output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    for (int i = 0; i < 30; ++i) {
        raft::command cmd = create_command(i + 1);
        raft::log_entry le = fsm.add_entry(std::move(cmd));

        do {
            output = fsm.get_output();
        } while (output.messages.size() == 0);  // Should only loop twice

        auto msg = std::get<raft::append_request_send>(output.messages.back().second);
        auto idx = msg.entries.back().get().idx;
        BOOST_CHECK(idx == i + 1);
        fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    }
}

// TestProgressResumeByHeartbeatResp
// TBD once we have failure_detector.is_alive() heartbeat

// Similar but with append reply
BOOST_AUTO_TEST_CASE(test_progress_resume_by_append_resp) {

    failure_detector fd;

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};

    raft::configuration cfg({id1, id2});                 // 2 nodes
    raft::log log{raft::snapshot{.config = cfg}};

    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    election_timeout(fsm);
    auto output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    raft::follower_progress& fprogress = fsm.get_progress(id2);
    BOOST_CHECK(fprogress.state == raft::follower_progress::state::PROBE);

    fprogress = fsm.get_progress(id2);
    BOOST_CHECK(!fprogress.probe_sent);
    raft::command cmd = create_command(1);
    raft::log_entry le = fsm.add_entry(std::move(cmd));
    do {
        output = fsm.get_output();
    } while (output.messages.size() == 0);

    BOOST_CHECK(fprogress.probe_sent);
}

// TestProgressPaused
BOOST_AUTO_TEST_CASE(test_progress_paused) {

    failure_detector fd;
    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};
    raft::configuration cfg({id1, id2});                 // 2 nodes
    raft::log log{raft::snapshot{.config = cfg}};

    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    election_timeout(fsm);
    auto output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    fsm.step(id2, raft::vote_reply{output.term, true});

    fsm.add_entry(create_command(1));
    fsm.add_entry(create_command(2));
    fsm.add_entry(create_command(3));
    fsm.tick();
    do {
        output = fsm.get_output();
    } while (output.messages.size() == 0);
    BOOST_CHECK(output.messages.size() == 1);
}
#endif

// TestProgressFlowControl
BOOST_AUTO_TEST_CASE(test_progress_flow_control) {

    failure_detector fd;
    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};
    raft::configuration cfg({id1, id2});                 // 2 nodes
    raft::log log{raft::snapshot{.config = cfg}};

    // Fit 2 ints
    raft::fsm_config fsm_cfg_8{.append_request_threshold = 2};
    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg_8);

    election_timeout(fsm);
    auto output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    fsm.step(id2, raft::vote_reply{output.term, true});
	// Throw away all the messages relating to the initial election.
    output = fsm.get_output();
    raft::follower_progress& fprogress = fsm.get_progress(id2);
    BOOST_CHECK(fprogress.state == raft::follower_progress::state::PROBE);

	// While node 2 is in probe state, propose a bunch of entries.
    // TODO: 1000 long string

    for (auto i = 0; i < 30; ++i) {
        fsm.add_entry(create_command(i));
    }
    fsm.tick();
    do {
        output = fsm.get_output();
    } while (output.messages.size() == 0);
    BOOST_CHECK(output.messages.size() == 1);
    raft::append_request_send msg;
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request_send>(output.messages.back().second));
	// the first proposal (only one proposal gets sent because follower is in probe state)
    BOOST_CHECK(msg.entries.size() == 1);
    const raft::log_entry& le = msg.entries.back().get();
    size_t first_idx = 1;         // Current log index, starts 1
    BOOST_CHECK(le.idx == first_idx);
    raft::command cmd;
    BOOST_REQUIRE_NO_THROW(cmd = std::get<raft::command>(le.data));
    BOOST_CHECK(cmd.size() == sizeof(int));   // TODO: 1000

	// When this append is acked, we change to replicate state and can
	// send multiple messages at once.
    fsm.step(id2, raft::append_reply{msg.current_term, le.idx, raft::append_reply::accepted{le.idx}});
    do {
        output = fsm.get_output();
    } while (output.messages.size() == 0);
    BOOST_CHECK(output.messages.size() == raft::follower_progress::max_in_flight);

    for (size_t i = 0; i < output.messages.size(); ++i) {
        raft::append_request_send msg;
        BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request_send>(output.messages[i].second));
        // XXX why etcd 2 entries?
        // XXX XXX XXX   depends on configuration  append request threshold (default zero?)
fmt::print("XXX msg entries size {}\n", msg.entries.size());
        const raft::log_entry& le = msg.entries.back().get();
        BOOST_CHECK(le.idx == first_idx + i + 1);
        raft::command cmd;
        BOOST_REQUIRE_NO_THROW(cmd = std::get<raft::command>(le.data));
        BOOST_CHECK(cmd.size() == sizeof(int));   // TODO: 1000
    }

	// Ack all those messages together and get the last two
	// messages (containing three entries).
    auto ack_idx = index_t{first_idx + raft::follower_progress::max_in_flight};
    fsm.step(id2, raft::append_reply{msg.current_term, ack_idx, raft::append_reply::accepted{ack_idx}});
    do {
        output = fsm.get_output();
    } while (output.messages.size() == 0);

fmt::print("XXX msg size {}\n", output.messages.size());
    BOOST_CHECK(output.messages.size() == raft::follower_progress::max_in_flight);

}

