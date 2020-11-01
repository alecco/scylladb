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
// XXX #include "serializer_impl.hh"

#include "raft/fsm.hh"

using raft::term_t, raft::index_t, raft::server_id;

class fsm_debug : public raft::fsm {
public:
    using raft::fsm::fsm;
    raft::follower_progress& get_progress(server_id id) {
        raft::follower_progress& progress = _tracker->find(id);
        return progress;
    }
};

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

    // First iteration doesn't output message
    int follower_idx = 1;     // Next index to send to follower
    for (int i = 0; i < 30; ++i) {
        raft::command cmd = create_command(i);
        raft::log_entry le = fsm.add_entry(std::move(cmd));
        BOOST_CHECK(le.term == 1);
        BOOST_CHECK(le.idx == i + 1);

        auto output = fsm.get_output();
        BOOST_CHECK(output.log_entries.size() == 1);
        BOOST_CHECK(output.log_entries[0]->term == 1);
        BOOST_CHECK(output.log_entries[0]->idx == i + 1);

        if (output.messages.size() == 1) {
            auto msg = std::get<raft::append_request_send>(output.messages.back().second);
            auto idx = msg.entries.back().get().idx;
            BOOST_CHECK(idx == follower_idx);
            fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
            follower_idx++;
        }
    }
}

// TestProgressResumeByHeartbeatResp
BOOST_AUTO_TEST_CASE(test_progress_resume_by_hearbeat_resp) {

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

    raft::follower_progress& fprogress = fsm.get_progress(id2);
    BOOST_CHECK(fprogress.state == raft::follower_progress::state::PROBE);
    fsm.add_entry(create_command(1));   // move probe_sent with add, not tick
    output = fsm.get_output();          // Advances stable idx
    fprogress = fsm.get_progress(id2);
    BOOST_CHECK(fprogress.probe_sent);
    fsm.add_entry(create_command(2));   // why second needed to send...
    output = fsm.get_output();
    BOOST_CHECK(output.messages.size() == 1);
    auto msg = std::get<raft::append_request_send>(output.messages.back().second);
    auto idx = msg.entries.back().get().idx;
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    fsm.tick();    // beat, make it send heartbeat
    BOOST_CHECK(!fprogress.probe_sent);  // hmm
}

