/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
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


#include <inttypes.h>
#include <regex>

#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/adjacent_find.hpp>
#include <seastar/core/coroutine.hh>

#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"

#include "auth/resource.hh"
#include "auth/service.hh"
#include "schema_builder.hh"
#include "db/extensions.hh"
#include "database.hh"
#include "types/user.hh"
#include "gms/feature_service.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "service/raft/raft_group_registry.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "utils/UUID_gen.hh"
#include "db/schema_tables.hh"

namespace cql3 {

namespace statements {

static logging::logger mylogger("create_table");

create_table_statement::create_table_statement(cf_name name,
                                               ::shared_ptr<cf_prop_defs> properties,
                                               bool if_not_exists,
                                               column_set_type static_columns,
                                               const std::optional<utils::UUID>& id)
    : schema_altering_statement{name}
    , _use_compact_storage(false)
    , _static_columns{static_columns}
    , _properties{properties}
    , _if_not_exists{if_not_exists}
    , _id(id)
{
}

future<> create_table_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const {
    return state.has_keyspace_access(keyspace(), auth::permission::CREATE);
}

void create_table_statement::validate(service::storage_proxy&, const service::client_state& state) const {
    // validated in announceMigration()
}

// Column definitions
std::vector<column_definition> create_table_statement::get_columns() const
{
    std::vector<column_definition> column_defs;
    for (auto&& col : _columns) {
        column_kind kind = column_kind::regular_column;
        if (_static_columns.contains(col.first)) {
            kind = column_kind::static_column;
        }
        column_defs.emplace_back(col.first->name(), col.second, kind);
    }
    return column_defs;
}

mutation make_scylla_tables_mutation_timeuuid(schema_ptr table, api::timestamp_type& timestamp,
        utils::UUID& tuuid) {

    auto dv_ts = data_value{tuuid}.serialize_nonnull();

    schema_ptr s = db::schema_tables::scylla_tables();
    auto pkey = partition_key::from_singular(*s, "system");
    mutation m(db::schema_tables::scylla_tables(), pkey);

    auto& column_def_cur = *s->get_column_definition("current_timeuuid");
    m.set_static_cell(column_def_cur, atomic_cell::make_live(*column_def_cur.type, timestamp, dv_ts));

    // list of previous
    collection_mutation_description list_values;
    auto tuuid_raw = timeuuid_type->decompose(tuuid);
    list_values.cells.emplace_back(
            bytes(reinterpret_cast<const int8_t*>(tuuid_raw.data()), tuuid_raw.size()),
            atomic_cell::make_live(*timeuuid_type, timestamp, dv_ts));
    auto& column_def_prev = *s->get_column_definition("previous_timeuuid");
    auto timeuuid_list_type = list_type_impl::get_instance(timeuuid_type, false);
    m.set_static_cell(column_def_prev, list_values.serialize(*timeuuid_list_type));

    return m;
}

future<mutation> create_table_statement::create_schema_timeuuid(const schema_ptr& schema, query_processor& qp) const {
    using namespace std::chrono_literals;

    api::timestamp_type timestamp = api::new_timestamp();
    auto timestamp_us = std::chrono::microseconds{timestamp};
    utils::UUID new_tuuid = utils::UUID_gen::get_random_time_UUID_from_micros(timestamp_us);

    static const auto load_timeuuid_cql = format("SELECT current_timeuuid, previous_timeuuid "
            "FROM system_schema.{} WHERE keyspace_name = ?", db::schema_tables::SCYLLA_TABLES);
    ::shared_ptr<untyped_result_set> prev_timeuuid_rs = co_await qp.execute_internal(load_timeuuid_cql,
            {"system"});

fmt::print("\n\n XXX 1 {}\n\n", new_tuuid);
    utils::UUID cur_tuuid;
list_type_impl::native_type arg_types;
// std::vector<timeuuid_native_type> vt;
    if (!prev_timeuuid_rs->empty() && prev_timeuuid_rs->one().has("current_timeuuid")) {
        // There should be only one row since timeuuid columns are static
        const auto& row = prev_timeuuid_rs->one();
        cur_tuuid = row.get_as<utils::UUID>("current_timeuuid");
        if (timestamp <= cur_tuuid.timestamp()) {
            mylogger.info("schema mutated within same microsecond {}.{} {}", schema->ks_name(),
                    schema->cf_name(), timestamp);
            timestamp += 1;
        }

fmt::print("\n XXX prev timeuuid list? {} ----------------------------\n", row.has("previous_timeuuid"));
        if (row.has("previous_timeuuid")) {
            auto prev_tuuids = row.get_list<timeuuid_native_type>("previous_timeuuid");
fmt::print("\n XXX prev timeuuids... \n");
            for (const auto& tu: prev_tuuids) {
fmt::print("  {}\n", tu.uuid);
// XXX                 arg_types.emplace_back(data_value{tu}.serialize_nonnull());
                // XXX should this be timeuuid
                arg_types.emplace_back(tu);
            }
// fmt::print("\n XXX prev timeuuid {} ----------------------------\n", prev_tuuids);
        }
    }

//     prev_tuuids.push_back(cur_tuuid);
    arg_types.emplace_back(new_tuuid); // XXX
auto timeuuid_list_type = list_type_impl::get_instance(timeuuid_type, false);
    // auto x = make_list_value(timeuuid_list_type, list_type_impl::native_type(prev_tuuids));
    // auto x = make_list_value(timeuuid_list_type, {cur_tuuid});
auto x = make_list_value(timeuuid_list_type, std::move(arg_types));
// auto x = make_list_value(timeuuid_list_type, std::move(vt));

    // Store new schema timestamp
    static const auto store_timeuuid_cql = format("UPDATE system_schema.{} "
            "SET current_timeuuid = ?, previous_timeuuid = ? "
            "WHERE keyspace_name = ?", db::schema_tables::SCYLLA_TABLES);
    mylogger.trace("Updating schema timeuuid to {}", timestamp);

// XXX buil list of UUIDs

    // XXX: should we protect this from exceptions and check result?
    ::shared_ptr<untyped_result_set> store_timeuuid_rs = co_await qp.execute_internal(store_timeuuid_cql,
            {new_tuuid,
#if 0
            data_value::make_null(timeuuid_list_type),
#else
            x,
#endif
            "system"});
fmt::print("\n YYY 8 \n");

    // XXX send previous!   
    co_return make_scylla_tables_mutation_timeuuid(schema, timestamp, new_tuuid);
}

future<shared_ptr<cql_transport::event::schema_change>> create_table_statement::announce_migration(query_processor& qp) const {
    auto schema = get_cf_meta_data(qp.db());  // XXX schema for the table being created (cf)
    auto& mm = qp.get_migration_manager();
    auto& raft_gr = qp.raft();
    try {
        if (raft_gr.local().is_enabled())  {
            co_await raft_gr.invoke_on(0, [this, &qp, &mm = mm, schema = std::move(schema)]
                    (service::raft_group_registry& raft_gr) -> future<> {
                raft::server& group0 = raft_gr.group0();

                co_await group0.read_barrier();

                // XXX before because it needs schema and it's moved
                auto m_schema_uuid = co_await create_schema_timeuuid(schema, qp);

                std::vector<mutation> m = co_await mm.prepare_new_column_family_announcement(std::move(schema));
                m.push_back(m_schema_uuid);

                // todo: add schema version into command, to apply
                // only on condition the version is the same.
                // qqq: what happens if there is a command in between?
                // there is a new schema version, apply skipped, but
                // we don't get a proper error.
                auto cmd = mm.adjust_for_distribution(std::move(m));
                co_await group0.add_entry(cmd, raft::wait_type::applied);
                // TODO: loop and retry if apply is a no-op - check
                // new schema version
                co_return;
            });
        } else {

            co_await mm.announce_new_column_family(std::move(schema));
        }
        using namespace cql_transport;
        co_return ::make_shared<event::schema_change>(
            event::schema_change::change_type::CREATED,
            event::schema_change::target_type::TABLE,
            this->keyspace(),
            this->column_family());
    } catch (const exceptions::already_exists_exception& e) {
        if (_if_not_exists) {
            co_return ::shared_ptr<cql_transport::event::schema_change>();
        }
        throw e;
    }
}

/**
 * Returns a CFMetaData instance based on the parameters parsed from this
 * <code>CREATE</code> statement, or defaults where applicable.
 *
 * @return a CFMetaData instance corresponding to the values parsed from this statement
 * @throws InvalidRequestException on failure to validate parsed parameters
 */
schema_ptr create_table_statement::get_cf_meta_data(const database& db) const {
    schema_builder builder{keyspace(), column_family(), _id};
    apply_properties_to(builder, db);
    return builder.build(_use_compact_storage ? schema_builder::compact_storage::yes : schema_builder::compact_storage::no);
}

void create_table_statement::apply_properties_to(schema_builder& builder, const database& db) const {
    auto&& columns = get_columns();
    for (auto&& column : columns) {
        builder.with_column_ordered(column);
    }
#if 0
    cfmd.defaultValidator(defaultValidator)
        .addAllColumnDefinitions(getColumns(cfmd))
#endif
    add_column_metadata_from_aliases(builder, _key_aliases, _partition_key_types, column_kind::partition_key);
    add_column_metadata_from_aliases(builder, _column_aliases, _clustering_key_types, column_kind::clustering_key);
#if 0
    if (valueAlias != null)
        addColumnMetadataFromAliases(cfmd, Collections.singletonList(valueAlias), defaultValidator, ColumnDefinition.Kind.COMPACT_VALUE);
#endif

    _properties->apply_to_builder(builder, _properties->make_schema_extensions(db.extensions()));
}

void create_table_statement::add_column_metadata_from_aliases(schema_builder& builder, std::vector<bytes> aliases, const std::vector<data_type>& types, column_kind kind) const
{
    assert(aliases.size() == types.size());
    for (size_t i = 0; i < aliases.size(); i++) {
        if (!aliases[i].empty()) {
            builder.with_column(aliases[i], types[i], kind);
        }
    }
}

std::unique_ptr<prepared_statement>
create_table_statement::prepare(database& db, cql_stats& stats) {
    // Cannot happen; create_table_statement is never instantiated as a raw statement
    // (instead we instantiate create_table_statement::raw_statement)
    abort();
}

future<> create_table_statement::grant_permissions_to_creator(const service::client_state& cs) const {
    return do_with(auth::make_data_resource(keyspace(), column_family()), [&cs](const auth::resource& r) {
        return auth::grant_applicable_permissions(
                *cs.get_auth_service(),
                *cs.user(),
                r).handle_exception_type([](const auth::unsupported_authorization_operation&) {
            // Nothing.
        });
    });
}

create_table_statement::raw_statement::raw_statement(cf_name name, bool if_not_exists)
    : cf_statement{std::move(name)}
    , _if_not_exists{if_not_exists}
{ }

std::unique_ptr<prepared_statement> create_table_statement::raw_statement::prepare(database& db, cql_stats& stats) {
    // Column family name
    const sstring& cf_name = _cf_name->get_column_family();
    std::regex name_regex("\\w+");
    if (!std::regex_match(std::string(cf_name), name_regex)) {
        throw exceptions::invalid_request_exception(format("\"{}\" is not a valid table name (must be alphanumeric character only: [0-9A-Za-z]+)", cf_name.c_str()));
    }
    if (cf_name.size() > size_t(schema::NAME_LENGTH)) {
        throw exceptions::invalid_request_exception(format("Table names shouldn't be more than {:d} characters long (got \"{}\")", schema::NAME_LENGTH, cf_name.c_str()));
    }

    // Check for duplicate column names
    auto i = boost::range::adjacent_find(_defined_names, [] (auto&& e1, auto&& e2) {
        return e1->text() == e2->text();
    });
    if (i != _defined_names.end()) {
        throw exceptions::invalid_request_exception(format("Multiple definition of identifier {}", (*i)->text()));
    }

    _properties.validate(db, _properties.properties()->make_schema_extensions(db.extensions()));
    const bool has_default_ttl = _properties.properties()->get_default_time_to_live() > 0;

    auto stmt = ::make_shared<create_table_statement>(*_cf_name, _properties.properties(), _if_not_exists, _static_columns, _properties.properties()->get_id());

    std::optional<std::map<bytes, data_type>> defined_multi_cell_columns;
    for (auto&& entry : _definitions) {
        ::shared_ptr<column_identifier> id = entry.first;
        cql3_type pt = entry.second->prepare(db, keyspace());

        if (has_default_ttl && pt.is_counter()) {
            throw exceptions::invalid_request_exception("Cannot set default_time_to_live on a table with counters");
        }

        if (pt.get_type()->is_multi_cell()) {
            if (pt.get_type()->is_user_type()) {
                // check for multi-cell types (non-frozen UDTs or collections) inside a non-frozen UDT
                auto type = static_cast<const user_type_impl*>(pt.get_type().get());
                for (auto&& inner: type->all_types()) {
                    if (inner->is_multi_cell()) {
                        // a nested non-frozen UDT should have already been rejected when defining the type
                        assert(inner->is_collection());
                        throw exceptions::invalid_request_exception("Non-frozen UDTs with nested non-frozen collections are not supported");
                    }
                }

                if (!db.features().cluster_supports_nonfrozen_udts()) {
                    throw exceptions::invalid_request_exception("Non-frozen UDT support is not enabled");
                }
            }

            if (!defined_multi_cell_columns) {
                defined_multi_cell_columns = std::map<bytes, data_type>{};
            }
            defined_multi_cell_columns->emplace(id->name(), pt.get_type());
        }
        stmt->_columns.emplace(id, pt.get_type()); // we'll remove what is not a column below
    }
    if (_key_aliases.empty()) {
        throw exceptions::invalid_request_exception("No PRIMARY KEY specified (exactly one required)");
    } else if (_key_aliases.size() > 1) {
        throw exceptions::invalid_request_exception("Multiple PRIMARY KEYs specified (exactly one required)");
    }

    stmt->_use_compact_storage = _properties.use_compact_storage();

    auto& key_aliases = _key_aliases[0];
    std::vector<data_type> key_types;
    for (auto&& alias : key_aliases) {
        stmt->_key_aliases.emplace_back(alias->name());
        auto t = get_type_and_remove(stmt->_columns, alias);
        if (t->is_counter()) {
            throw exceptions::invalid_request_exception(format("counter type is not supported for PRIMARY KEY part {}", alias->text()));
        }
        if (t->references_duration()) {
            throw exceptions::invalid_request_exception(format("duration type is not supported for PRIMARY KEY part {}", alias->text()));
        }
        if (_static_columns.contains(alias)) {
            throw exceptions::invalid_request_exception(format("Static column {} cannot be part of the PRIMARY KEY", alias->text()));
        }
        key_types.emplace_back(t);
    }
    stmt->_partition_key_types = key_types;

    // Handle column aliases
    if (_column_aliases.empty()) {
        if (_properties.use_compact_storage()) {
            // There should remain some column definition since it is a non-composite "static" CF
            if (stmt->_columns.empty()) {
                throw exceptions::invalid_request_exception("No definition found that is not part of the PRIMARY KEY");
            }
            if (defined_multi_cell_columns) {
                throw exceptions::invalid_request_exception("Non-frozen collections and UDTs are not supported with COMPACT STORAGE");
            }
        }
        stmt->_clustering_key_types = std::vector<data_type>{};
    } else {
        // If we use compact storage and have only one alias, it is a
        // standard "dynamic" CF, otherwise it's a composite
        if (_properties.use_compact_storage() && _column_aliases.size() == 1) {
            if (defined_multi_cell_columns) {
                throw exceptions::invalid_request_exception("Non-frozen collections and UDTs are not supported with COMPACT STORAGE");
            }
            auto alias = _column_aliases[0];
            if (_static_columns.contains(alias)) {
                throw exceptions::invalid_request_exception(format("Static column {} cannot be part of the PRIMARY KEY", alias->text()));
            }
            stmt->_column_aliases.emplace_back(alias->name());
            auto at = get_type_and_remove(stmt->_columns, alias);
            if (at->is_counter()) {
                throw exceptions::invalid_request_exception(format("counter type is not supported for PRIMARY KEY part {}", stmt->_column_aliases[0]));
            }
            if (at->references_duration()) {
                throw exceptions::invalid_request_exception(format("duration type is not supported for PRIMARY KEY part {}", stmt->_column_aliases[0]));
            }
            stmt->_clustering_key_types.emplace_back(at);
        } else {
            std::vector<data_type> types;
            for (auto&& t : _column_aliases) {
                stmt->_column_aliases.emplace_back(t->name());
                auto type = get_type_and_remove(stmt->_columns, t);
                if (type->is_counter()) {
                    throw exceptions::invalid_request_exception(format("counter type is not supported for PRIMARY KEY part {}", t->text()));
                }
                if (type->references_duration()) {
                    throw exceptions::invalid_request_exception(format("duration type is not supported for PRIMARY KEY part {}", t->text()));
                }
                if (_static_columns.contains(t)) {
                    throw exceptions::invalid_request_exception(format("Static column {} cannot be part of the PRIMARY KEY", t->text()));
                }
                types.emplace_back(type);
            }

            if (_properties.use_compact_storage()) {
                if (defined_multi_cell_columns) {
                    throw exceptions::invalid_request_exception("Non-frozen collections and UDTs are not supported with COMPACT STORAGE");
                }
                stmt->_clustering_key_types = types;
            } else {
                stmt->_clustering_key_types = types;
            }
        }
    }

    if (!_static_columns.empty()) {
        // Only CQL3 tables can have static columns
        if (_properties.use_compact_storage()) {
            throw exceptions::invalid_request_exception("Static columns are not supported in COMPACT STORAGE tables");
        }
        // Static columns only make sense if we have at least one clustering column. Otherwise everything is static anyway
        if (_column_aliases.empty()) {
            throw exceptions::invalid_request_exception("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
        }
    }

    if (_properties.use_compact_storage() && !stmt->_column_aliases.empty()) {
        if (stmt->_columns.empty()) {
#if 0
            // The only value we'll insert will be the empty one, so the default validator don't matter
            stmt.defaultValidator = BytesType.instance;
            // We need to distinguish between
            //   * I'm upgrading from thrift so the valueAlias is null
            //   * I've defined my table with only a PK (and the column value will be empty)
            // So, we use an empty valueAlias (rather than null) for the second case
            stmt.valueAlias = ByteBufferUtil.EMPTY_BYTE_BUFFER;
#endif
        } else {
            if (stmt->_columns.size() > 1) {
                throw exceptions::invalid_request_exception(format("COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: {})",
                    ::join( ", ", stmt->_columns | boost::adaptors::map_keys)));
            }
#if 0
            Map.Entry<ColumnIdentifier, AbstractType> lastEntry = stmt.columns.entrySet().iterator().next();
            stmt.defaultValidator = lastEntry.getValue();
            stmt.valueAlias = lastEntry.getKey().bytes;
            stmt.columns.remove(lastEntry.getKey());
#endif
        }
    } else {
        // For compact, we are in the "static" case, so we need at least one column defined. For non-compact however, having
        // just the PK is fine since we have CQL3 row marker.
        if (_properties.use_compact_storage() && stmt->_columns.empty()) {
            throw exceptions::invalid_request_exception("COMPACT STORAGE with non-composite PRIMARY KEY require one column not part of the PRIMARY KEY, none given");
        }
#if 0
        // There is no way to insert/access a column that is not defined for non-compact storage, so
        // the actual validator don't matter much (except that we want to recognize counter CF as limitation apply to them).
        stmt.defaultValidator = !stmt.columns.isEmpty() && (stmt.columns.values().iterator().next() instanceof CounterColumnType)
            ? CounterColumnType.instance
            : BytesType.instance;
#endif
    }

    // If we give a clustering order, we must explicitly do so for all aliases and in the order of the PK
    if (!_properties.defined_ordering().empty()) {
        if (_properties.defined_ordering().size() > _column_aliases.size()) {
            throw exceptions::invalid_request_exception("Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }

        int i = 0;
        for (auto& pair: _properties.defined_ordering()){
            auto& id = pair.first;
            auto& c = _column_aliases.at(i);

            if (!(*id == *c)) {
                if (_properties.find_ordering_info(*c)) {
                    throw exceptions::invalid_request_exception(format("The order of columns in the CLUSTERING ORDER directive must be the one of the clustering key ({} must appear before {})", c, id));
                } else {
                    throw exceptions::invalid_request_exception(format("Missing CLUSTERING ORDER for column {}", c));
                }
            }
            ++i;
        }
    }

    return std::make_unique<prepared_statement>(stmt);
}

data_type create_table_statement::raw_statement::get_type_and_remove(column_map_type& columns, ::shared_ptr<column_identifier> t)
{
    auto it = columns.find(t);
    if (it == columns.end()) {
        throw exceptions::invalid_request_exception(format("Unknown definition {} referenced in PRIMARY KEY", t->text()));
    }
    auto type = it->second;
    if (type->is_multi_cell()) {
        if (type->is_collection()) {
            throw exceptions::invalid_request_exception(format("Invalid non-frozen collection type for PRIMARY KEY component {}", t->text()));
        } else {
            throw exceptions::invalid_request_exception(format("Invalid non-frozen user-defined type for PRIMARY KEY component {}", t->text()));
        }
    }
    columns.erase(t);

    return _properties.get_reversable_type(*t, type);
}

void create_table_statement::raw_statement::add_definition(::shared_ptr<column_identifier> def, ::shared_ptr<cql3_type::raw> type, bool is_static) {
    _defined_names.emplace(def);
    _definitions.emplace(def, type);
    if (is_static) {
        _static_columns.emplace(def);
    }
}

void create_table_statement::raw_statement::add_key_aliases(const std::vector<::shared_ptr<column_identifier>> aliases) {
    _key_aliases.emplace_back(aliases);
}

void create_table_statement::raw_statement::add_column_alias(::shared_ptr<column_identifier> alias) {
    _column_aliases.emplace_back(alias);
}

// Check for choices of table properties (e.g., the choice of compaction
// strategy) which are restricted configuration options.
// This check can throw a configuration_exception immediately if an option
// is forbidden by the configuration, or return a warning string if the
// relevant restriction was set to "warn".
// This function is only supposed to check for options which are usually
// legal but restricted by the configuration. Checks for other of errors
// in the table's options are done elsewhere.
std::optional<sstring> check_restricted_table_properties(
    service::storage_proxy& proxy,
    const sstring& keyspace, const sstring& table,
    const cf_prop_defs& cfprops)
{
    // Note: In the current implementation, CREATE TABLE calls this function
    // after cfprops.validate() was called, but ALTER TABLE calls this
    // function before cfprops.validate() (there, validate() is only called
    // in announce_migration(), in the middle of execute).
    auto strategy = cfprops.get_compaction_strategy_class();
    if (strategy && *strategy == sstables::compaction_strategy_type::date_tiered) {
        switch(proxy.local_db().get_config().restrict_dtcs()) {
        case db::tri_mode_restriction_t::mode::TRUE:
            throw exceptions::configuration_exception(
                "DateTieredCompactionStrategy is deprecated, and "
                "forbidden by the current configuration. Please use "
                "TimeWindowCompactionStrategy instead. You may also override this "
                "restriction by setting the restrict_dtcs configuration option "
                "to false.");
        case db::tri_mode_restriction_t::mode::WARN:
            return format("DateTieredCompactionStrategy is deprecated, "
                "but was used for table {}.{}. The restrict_dtcs "
                "configuration option can be changed to silence this warning "
                " or make it into an error.", keyspace, table);
        case db::tri_mode_restriction_t::mode::FALSE:
            break;
        }
    }
    return std::nullopt;
}

future<::shared_ptr<messages::result_message>>
create_table_statement::execute(query_processor& qp, service::query_state& state, const query_options& options) const {
    std::optional<sstring> warning = check_restricted_table_properties(qp.proxy(), keyspace(), column_family(), *_properties);
    return schema_altering_statement::execute(qp, state, options).then([this, warning = std::move(warning)] (::shared_ptr<messages::result_message> msg) {
        if (warning) {
            msg->add_warning(*warning);
            mylogger.warn("{}", *warning);
        }
        return msg;
    });
}

}

}
