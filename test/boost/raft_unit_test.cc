/*
 * Copyright (C) 2020, ScyllaDB
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

#include "raft/raft.hh"
#include "raft/log.hh"

using raft::term_t, raft::index_t, raft::server_id;

BOOST_AUTO_TEST_CASE(test_log_noop) {

    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.config = cfg}};

    BOOST_CHECK(log.empty());

}

BOOST_AUTO_TEST_CASE(test_log_is_up_to_date) {

    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.config = cfg}};

    BOOST_CHECK(log.is_up_to_date(index_t{1}, term_t{0}));    // Vote request with same term/idx
    log.emplace_back(raft::log_entry{term_t{0}, index_t{1}}); // Add one entry
    log.emplace_back(raft::log_entry{term_t{0}, index_t{2}}); // Vote request with higher term/idx
    BOOST_CHECK(!log.is_up_to_date(index_t{1}, term_t{0}));   // Vote request with lower idx
    BOOST_CHECK(log.is_up_to_date(index_t{1}, term_t{1}));    // Vote request with higher term
    log.emplace_back(raft::log_entry{term_t{1}, index_t{2}}); // Term increment
    BOOST_CHECK(!log.is_up_to_date(index_t{2}, term_t{0}));   // Vote request with lower term
}

bool operator==(const raft::snapshot& l, const raft::snapshot& r) {
    if (l.idx == r.idx && l.term == r.term) {
        return true;
    }
    return false;
}

BOOST_AUTO_TEST_CASE(test_log_snapshot) {

    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.config = cfg}};

    auto snp_z = raft::snapshot{index_t{0}, term_t{0}};
    BOOST_CHECK(log.get_snapshot() == snp_z);

    raft::snapshot snp_1{index_t{1}, term_t{1}};

    for (size_t i = 1; i <= 10; ++i) {
        log.emplace_back(raft::log_entry{term_t{0}, index_t{i}});
    }

    BOOST_CHECK(log.non_snapshoted_length() == 10);

    auto& snp2 = log.get_snapshot();
    // Snapshot on 10 but with 5 trailing
    log.apply_snapshot(raft::snapshot{index_t{10}, term_t{0}}, 5);
    BOOST_CHECK(log.non_snapshoted_length() == 5);
    auto snp_10 = raft::snapshot{index_t{10}, term_t{0}};
    BOOST_CHECK(log.get_snapshot() == snp_10);
    auto& snp3 = log.get_snapshot();
}

BOOST_AUTO_TEST_CASE(test_log_stable) {

    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.config = cfg}};
    for (size_t i = 1; i <= 10; ++i) {
        log.emplace_back(raft::log_entry{term_t{0}, index_t{i}});
    }
    log.stable_to(raft::index_t{5});
    BOOST_CHECK(log.stable_idx() == raft::index_t{5});
}

BOOST_AUTO_TEST_CASE(test_log_match_term_no_snapshot) {

    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.config = cfg}};

    for (size_t i = 1; i <= 10; ++i) {
        log.emplace_back(raft::log_entry{term_t{0}, index_t{i}});
    }

    // Empty leader special case
    auto [match_e, term_e] = log.match_term(raft::index_t{0}, raft::term_t{0});
    BOOST_CHECK(match_e);

    // Check all entries with same term 0 match
    for (size_t i = 1; i <= 10; ++i) {
        auto [match_i, term_i] = log.match_term(raft::index_t{i}, raft::term_t{0});
        BOOST_CHECK(match_i);
    }
    // Past doesn't match
    auto [match_n, term_n] = log.match_term(raft::index_t{11}, raft::term_t{0});
    BOOST_CHECK(!match_n);

    // Check entry with higher term 1 doesn't match
    auto [match_ht, term_ht] = log.match_term(raft::index_t{1}, raft::term_t{1});
    BOOST_CHECK(!match_ht);
    BOOST_CHECK(term_ht == 0);

}

BOOST_AUTO_TEST_CASE(test_log_match_term_with_snapshot) {

    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.idx = raft::index_t{10}, .term = raft::term_t{1}, .config = cfg}};

    for (size_t i = 11; i <= 20; ++i) {
        log.emplace_back(raft::log_entry{term_t{1}, index_t{i}});
    }

    // Empty leader special case
    auto [match_e, term_e] = log.match_term(raft::index_t{0}, raft::term_t{0});
    BOOST_CHECK(match_e);

    // Check all entries with same term 1 match
    for (size_t i = 10; i <= 20; ++i) {
        auto [match_i, term_i] = log.match_term(raft::index_t{i}, raft::term_t{1});
        BOOST_CHECK(match_i);
    }
    // Past doesn't match
    auto [match_n, term_n] = log.match_term(raft::index_t{21}, raft::term_t{1});
    BOOST_CHECK(!match_n);

    // Check entry with higher term 1 doesn't match
    auto [match_ht, term_ht] = log.match_term(raft::index_t{10}, raft::term_t{2});
    BOOST_CHECK(!match_ht);
    BOOST_CHECK(term_ht == 1);

}

BOOST_AUTO_TEST_CASE(test_log_maybe_append_entries) {

    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.idx = raft::index_t{10}, .term = raft::term_t{1}, .config = cfg}};

    for (size_t i = 11; i <= 20; ++i) {
        log.emplace_back(raft::log_entry{term_t{1}, index_t{i}});
    }

    // Entries before start should be ignored, no matter what term
    log.maybe_append({raft::log_entry{term_t{0}, index_t{3}}});
    BOOST_CHECK(log.last_idx() == 20);
    BOOST_CHECK(log.non_snapshoted_length() == 10);

    // Existing matching entries are confirmed (including snap)
    std::vector<raft::log_entry> matching_entries;
    for (size_t i = 1; i <= 20; ++i) {
        matching_entries.emplace_back(raft::log_entry{term_t{1}, index_t{i}});
    }
    log.maybe_append(std::move(matching_entries));
    BOOST_CHECK(log.last_idx() == 20);
    BOOST_CHECK(log.non_snapshoted_length() == 10);

    // First non-matching truncates from there
    // so it should drop and replace 16-20, then add 21-25
    std::vector<raft::log_entry> non_matching_entries;
    for (size_t i = 10; i <= 15; ++i) {
        non_matching_entries.emplace_back(raft::log_entry{term_t{1}, index_t{i}});
    }
    for (size_t i = 16; i <= 25; ++i) {
        non_matching_entries.emplace_back(raft::log_entry{term_t{2}, index_t{i}});
    }
    log.maybe_append(std::move(non_matching_entries));
    BOOST_CHECK(log.last_idx() == 25);
    BOOST_CHECK(log.non_snapshoted_length() == 15);
}
