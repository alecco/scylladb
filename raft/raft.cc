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
#include "raft.hh"

namespace raft {

seastar::logger logger("raft");

future<seastar::semaphore_units<>> log::lock() {
    return seastar::get_units(*_log_lock, 1);
}

log_entry& log::operator[](size_t i) {
    assert(index_t(i) >= _log_starting_index);
    return _log[i - _log_starting_index];
}

// reserve n additional entries
void log::ensure_capacity(size_t n) {
     // there is not reserver for std::deque!
     //_log.reserve(_log.size() + n);
}

void log::emplace_back(log_entry&& e) {
    _log.emplace_back(std::move(e));
}

bool log::empty() const {
    return _log.empty();
}

index_t log::last_idx() const {
    return index_t(_log.size()) + _log_starting_index - index_t(1);
}

index_t log::next_idx() const {
    return last_idx() + index_t(1);
}

void log::truncate_head(size_t i) {
    auto it = _log.begin() + (i - _log_starting_index);
    _log.erase(it, _log.end());
}

index_t log::start_index() const {
    return _log_starting_index;
}

} // end of namespace raft
