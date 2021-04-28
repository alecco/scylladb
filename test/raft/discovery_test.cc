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
#define BOOST_TEST_MODULE raft
#include "test/raft/helpers.hh"
#include "service/raft/discovery.hh"

using namespace raft;

using discovery_network = std::unordered_map<server_id, service::discovery*>;

using service::discovery;

void
run_discovery_impl(discovery_network& network) {
    while (true) {
        for (auto e : network) {
            discovery& from = *e.second;
            auto output = from.get_output();
            if (std::holds_alternative<discovery::i_am_leader>(output)) {
                return;
            } else if (std::holds_alternative<discovery::pause>(output)) {
                continue;
            }
            auto& msgs = std::get<discovery::request_list>(output);
            for (auto&& m : msgs) {
                auto it = network.find(m.first.id);
                if (it == network.end()) {
                    // The node is not available, drop the message
                    continue;
                }
                discovery& to = *(it->second);
                from.response(m.first, to.request(m.second));
            }
        }
    }
}

template <typename... Args>
void run_discovery(Args&&... args) {
    discovery_network network;
    auto add_node = [&network](discovery& node) -> void {
        network.emplace(node.id(), &node);
    };
    (add_node(args), ...);
    run_discovery_impl(network);
}

BOOST_AUTO_TEST_CASE(test_basic) {

    server_address addr1 = {.id = id()};

    // Must supply an Internet address for self
    BOOST_CHECK_THROW(discovery(addr1, {}), std::logic_error);
    server_address addr2 = {.id = id(), .info = "192.168.1.2"};
    BOOST_CHECK_NO_THROW(discovery(addr2, {}));
    // Must supply an Internet address for each peer
    BOOST_CHECK_THROW(discovery(addr2, {addr1}), std::logic_error);
    // OK to include self into peers
    BOOST_CHECK_NO_THROW(discovery(addr2, {addr2}));
    // With a single peer, discovery immediately finds a leader
    discovery d(addr2, {});
    BOOST_CHECK(d.is_leader());
    d = discovery(addr2, {addr2});
    BOOST_CHECK(d.is_leader());
}


BOOST_AUTO_TEST_CASE(test_discovery) {

    server_address addr1 = {.id = id(), .info = "192.168.1.1"};
    server_address addr2 = {.id = id(), .info = "192.168.1.2"};

    discovery d1(addr1, {addr2});
    discovery d2(addr2, {addr1});
    run_discovery(d1, d2);

    BOOST_CHECK(d1.is_leader() ^ d2.is_leader());
}

BOOST_AUTO_TEST_CASE(test_discovery_fullmesh) {

    server_address addr1 = {.id = id(), .info = "127.0.0.13"};
    server_address addr2 = {.id = id(), .info = "127.0.0.19"};
    server_address addr3 = {.id = id(), .info = "127.0.0.21"};

    auto seeds = std::vector<server_address>({addr1, addr2, addr3});

    discovery d1(addr1, seeds);
    discovery d2(addr2, seeds);
    discovery d3(addr3, seeds);
    run_discovery(d1, d2, d3);

    BOOST_CHECK(d1.is_leader() ^ d2.is_leader() ^ d3.is_leader());
}
