/*
 * Copyright (C) 2020 ScyllaDB
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

#pragma once

#include <variant>
#include "raft/raft.hh"
#include "raft/fsm.hh"
#include "serializer.hh"
#include "serializer_impl.hh"

using raft::term_t, raft::index_t, raft::server_id;

// Simplified log entry without log index (for input)
struct log_input {
    int term;
    int value;
};

//
// Expected log_entry from fsm output
//
struct cmd {
    int value;
};

struct configuration {
    std::vector<unsigned> previous;
    std::vector<unsigned> current;
};

struct dummy {};

struct log_entry {
    unsigned term;
    unsigned idx;
    std::variant<cmd, configuration, dummy> data;
};

struct initial_snapshot {
    raft::snapshot snap;
};

// Become receptive to candidates
struct candidate {
    unsigned id;
};

// Run manual full election cycle and elect specific fsm
// TODO: handle uncommitted log entries
struct elect {
    unsigned id;
};

// Become receptive to candidates
struct receptive {
    unsigned id;
};

// Disconnects nodes
struct disconnect {
    std::vector<unsigned> ids;
    disconnect(std::initializer_list<unsigned> lst) {
        ids = lst;
    }
};

// Reconnects nodes previously disconnected
struct reconnect {
    std::vector<unsigned> ids;
    reconnect(std::initializer_list<unsigned> lst) {
        ids = lst;
    }
};

// Global actions with no arguments
enum simple_action {
    receptive_all,
};

struct server {
    unsigned id;
};

// Append entries to a leader fsm
struct entries {
    struct server server;
    unsigned n;
};

struct snapshot {
    unsigned idx;
    unsigned term;
    unsigned id;
};

struct append_request {
    unsigned term;
    unsigned leader_id;
    unsigned prev_log_id;
    unsigned prev_log_idx;
    unsigned prev_log_term;
    unsigned leader_commit_idx;
    std::vector<log_input> entries;
};

struct append_reply_accepted {
    unsigned last_idx;
    unsigned commit_idx;
    unsigned last_new_idx;
};

struct append_reply_rejected {
    unsigned last_idx;
    unsigned commit_idx;
    unsigned current_term;
    unsigned non_matching_idx;
};

struct vote_request {
    unsigned current_term;
    unsigned last_log_idx;
    unsigned last_log_term;
};

struct vote_reply {
    unsigned current_term;
    bool vote_granted;
};

struct install_snapshot {
    unsigned current_term;
    snapshot snp;
};

struct snapshot_reply {
    bool success;
};

struct message {
    unsigned dst;
    std::variant<append_request, append_reply_accepted, append_reply_rejected,
            vote_request, vote_reply, install_snapshot, snapshot_reply> rpc_message;
};

// Get output out of specific fsm
struct expect {
    unsigned server;
    bool follower;
    bool candidate;
    bool leader;
    unsigned term;
    unsigned vote;
    std::vector<log_entry> entries;
    std::vector<message> messages;
    std::vector<log_entry> committed;
    std::optional<snapshot> snp;
};

struct step {
    std::vector<std::variant<simple_action, candidate, receptive, elect,
            disconnect, reconnect, entries>> actions;
    std::vector<struct expect> expect;
};

struct test_case {
    std::string name;
    unsigned nodes;
    unsigned fsms = 1;
    unsigned initial_term = 1;
    std::optional<unsigned> initial_leader;
    std::vector<std::vector<log_input>> initial_logs;
    // std::vector<struct initial_snapshot> initial_snapshots;
    // std::vector<raft::server::configuration> config;
    std::vector<step> steps;
};


raft::log create_log(std::vector<log_entry> list, unsigned start_idx);


class Tester {
protected:
    std::vector<test_case> _tests;
    void _test(test_case test);
public:
    Tester(std::initializer_list<test_case> tests) : _tests(tests) {
        for (auto t: _tests) {
            _test(std::move(t));
        }
    };
};
