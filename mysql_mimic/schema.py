from __future__ import annotations

import re
from collections import defaultdict
from itertools import chain
from dataclasses import dataclass
from typing import Any, Optional, List, Dict, Iterable

from sqlglot.executor import Table, execute
from sqlglot import expressions as exp

from mysql_mimic.results import AllowedResult
from mysql_mimic.errors import MysqlError, ErrorCode
from mysql_mimic.packets import ComFieldList
from mysql_mimic.utils import dict_depth


@dataclass
class Column:
    name: str
    type: str
    table: str
    is_nullable: bool = True
    default: Optional[str] = None
    comment: Optional[str] = None
    schema: Optional[str] = None
    catalog: Optional[str] = "def"


INFO_SCHEMA = {
    "information_schema": {
        "character_sets": {
            "character_set_name": "TEXT",
            "default_collate_name": "TEXT",
            "description": "TEXT",
            "maxlen": "INT",
        },
        "collations": {
            "collation_name": "TEXT",
            "character_set_name": "TEXT",
            "id": "TEXT",
            "is_default": "TEXT",
            "is_compiled": "TEXT",
            "sortlen": "INT",
        },
        "columns": {
            "table_catalog": "TEXT",
            "table_schema": "TEXT",
            "table_name": "TEXT",
            "column_name": "TEXT",
            "ordinal_position": "INT",
            "column_default": "TEXT",
            "is_nullable": "TEXT",
            "data_type": "TEXT",
            "character_maximum_length": "INT",
            "character_octet_length": "INT",
            "numeric_precision": "INT",
            "numeric_scale": "INT",
            "datetime_precision": "INT",
            "character_set_name": "TEXT",
            "collation_name": "TEXT",
            "column_type": "TEXT",
            "column_key": "TEXT",
            "extra": "TEXT",
            "privileges": "TEXT",
            "column_comment": "TEXT",
            "generation_expression": "TEXT",
            "srs_id": "TEXT",
        },
        "column_privileges": {
            "grantee": "TEXT",
            "table_catalog": "TEXT",
            "table_schema": "TEXT",
            "table_name": "TEXT",
            "column_name": "TEXT",
            "privilege_type": "TEXT",
            "is_grantable": "TEXT",
        },
        "events": {
            "event_catalog": "TEXT",
            "event_schema": "TEXT",
            "event_name": "TEXT",
            "definer": "TEXT",
            "time_zone": "TEXT",
            "event_body": "TEXT",
            "event_definition": "TEXT",
            "event_type": "TEXT",
            "execute_at": "TEXT",
            "interval_value": "INT",
            "interval_field": "TEXT",
            "sql_mode": "TEXT",
            "starts": "TEXT",
            "ends": "TEXT",
            "status": "TEXT",
            "on_completion": "TEXT",
            "created": "TEXT",
            "last_altered": "TEXT",
            "last_executed": "TEXT",
            "event_comment": "TEXT",
            "originator": "INT",
            "character_set_client": "TEXT",
            "collation_connection": "TEXT",
            "database_collation": "TEXT",
        },
        "key_column_usage": {
            "constraint_catalog": "TEXT",
            "constraint_schema": "TEXT",
            "constraint_name": "TEXT",
            "table_catalog": "TEXT",
            "table_schema": "TEXT",
            "table_name": "TEXT",
            "column_name": "TEXT",
            "ordinal_position": "INT",
            "position_in_unique_constraint": "INT",
            "referenced_table_schema": "TEXT",
            "referenced_table_name": "TEXT",
            "referenced_column_name": "TEXT",
        },
        "parameters": {
            "specific_catalog": "TEXT",
            "specific_schema": "TEXT",
            "specific_name": "TEXT",
            "ordinal_position": "INT",
            "parameter_mode": "TEXT",
            "parameter_name": "TEXT",
            "data_type": "TEXT",
            "character_maximum_length": "INT",
            "character_octet_length": "INT",
            "numeric_precision": "INT",
            "numeric_scale": "INT",
            "datetime_precision": "INT",
            "character_set_name": "TEXT",
            "collation_name": "TEXT",
            "dtd_identifier": "TEXT",
            "routine_type": "TEXT",
        },
        "partitions": {
            "table_catalog": "TEXT",
            "table_schema": "TEXT",
            "table_name": "TEXT",
            "partition_name": "TEXT",
            "subpartition_name": "TEXT",
            "partition_ordinal_position": "INT",
            "subpartition_ordinal_position": "INT",
            "partition_method": "TEXT",
            "subpartition_method": "TEXT",
            "partition_expression": "TEXT",
            "subpartition_expression": "TEXT",
            "partition_description": "TEXT",
            "table_rows": "INT",
            "avg_row_length": "INT",
            "data_length": "INT",
            "max_data_length": "INT",
            "index_length": "INT",
            "data_free": "INT",
            "create_time": "TEXT",
            "update_time": "TEXT",
            "check_time": "TEXT",
            "checksum": "TEXT",
            "partition_comment": "TEXT",
            "nodegroup": "TEXT",
            "tablespace_name": "TEXT",
        },
        "referential_constraints": {
            "constraint_catalog": "TEXT",
            "constraint_schema": "TEXT",
            "constraint_name": "TEXT",
            "unique_constraint_catalog": "TEXT",
            "unique_constraint_schema": "TEXT",
            "unique_constraint_name": "TEXT",
            "match_option": "TEXT",
            "update_rule": "TEXT",
            "delete_rule": "TEXT",
            "table_name": "TEXT",
            "referenced_table_name": "TEXT",
        },
        "routines": {
            "specific_name": "TEXT",
            "routine_catalog": "TEXT",
            "routine_schema": "TEXT",
            "routine_name": "TEXT",
            "routine_type": "TEXT",
            "data_type": "TEXT",
            "character_maximum_length": "INT",
            "character_octet_length": "INT",
            "numeric_precision": "INT",
            "numeric_scale": "INT",
            "datetime_precision": "INT",
            "character_set_name": "TEXT",
            "collation_name": "TEXT",
            "dtd_identifier": "TEXT",
            "routine_body": "TEXT",
            "routine_definition": "TEXT",
            "external_name": "TEXT",
            "external_language": "TEXT",
            "parameter_style": "TEXT",
            "is_deterministic": "TEXT",
            "sql_data_access": "TEXT",
            "sql_path": "TEXT",
            "security_type": "TEXT",
            "created": "TEXT",
            "last_altered": "TEXT",
            "sql_mode": "TEXT",
            "routine_comment": "TEXT",
            "definer": "TEXT",
            "character_set_client": "TEXT",
            "collation_connection": "TEXT",
            "database_collation": "TEXT",
        },
        "schema_privileges": {
            "grantee": "TEXT",
            "table_catalog": "TEXT",
            "table_schema": "TEXT",
            "privilege_type": "TEXT",
            "is_grantable": "TEXT",
        },
        "schemata": {
            "catalog_name": "TEXT",
            "schema_name": "TEXT",
            "default_character_set_name": "TEXT",
            "default_collation_name": "TEXT",
            "sql_path": "TEXT",
        },
        "statistics": {
            "table_catalog": "TEXT",
            "table_schema": "TEXT",
            "table_name": "TEXT",
            "non_unique": "INT",
            "index_schema": "TEXT",
            "index_name": "TEXT",
            "seq_in_index": "INT",
            "column_name": "TEXT",
            "collation": "TEXT",
            "cardinality": "INT",
            "sub_part": "TEXT",
            "packed": "TEXT",
            "nullable": "TEXT",
            "index_type": "TEXT",
            "comment": "TEXT",
            "index_comment": "TEXT",
            "is_visible": "TEXT",
            "expression": "TEXT",
        },
        "tables": {
            "table_catalog": "TEXT",
            "table_schema": "TEXT",
            "table_name": "TEXT",
            "table_type": "TEXT",
            "engine": "TEXT",
            "version": "TEXT",
            "row_format": "TEXT",
            "table_rows": "INT",
            "avg_row_length": "INT",
            "data_length": "INT",
            "max_data_length": "INT",
            "index_length": "INT",
            "data_free": "INT",
            "auto_increment": "INT",
            "create_time": "TEXT",
            "update_time": "TEXT",
            "check_time": "TEXT",
            "table_collation": "TEXT",
            "checksum": "TEXT",
            "create_options": "TEXT",
            "table_comment": "TEXT",
        },
        "table_constraints": {
            "constraint_name": "TEXT",
            "constraint_catalog": "TEXT",
            "constraint_schema": "TEXT",
            "table_schema": "TEXT",
            "table_name": "TEXT",
            "constraint_type": "TEXT",
            "enforced": "TEXT",
        },
        "table_privileges": {
            "grantee": "TEXT",
            "table_catalog": "TEXT",
            "table_schema": "TEXT",
            "table_name": "TEXT",
            "privilege_type": "TEXT",
            "is_grantable": "TEXT",
        },
        "triggers": {
            "trigger_catalog": "TEXT",
            "trigger_schema": "TEXT",
            "trigger_name": "TEXT",
            "event_manipulation": "TEXT",
            "event_object_catalog": "TEXT",
            "event_object_schema": "TEXT",
            "event_object_table": "TEXT",
            "action_order": "INT",
            "action_condition": "TEXT",
            "action_statement": "TEXT",
            "action_orientation": "TEXT",
            "action_timing": "TEXT",
            "action_reference_old_table": "TEXT",
            "action_reference_new_table": "TEXT",
            "action_reference_old_row": "TEXT",
            "action_reference_new_row": "TEXT",
            "created": "TEXT",
            "sql_mode": "TEXT",
            "definer": "TEXT",
            "character_set_client": "TEXT",
            "collation_connection": "TEXT",
            "database_collation": "TEXT",
        },
        "user_privileges": {
            "grantee": "TEXT",
            "table_catalog": "TEXT",
            "privilege_type": "TEXT",
            "is_grantable": "TEXT",
        },
        "views": {
            "table_catalog": "TEXT",
            "table_schema": "TEXT",
            "table_name": "TEXT",
            "view_definition": "TEXT",
            "check_option": "TEXT",
            "is_updatable": "TEXT",
            "definer": "TEXT",
            "security_type": "TEXT",
            "character_set_client": "TEXT",
            "collation_connection": "TEXT",
        },
    },
    "mysql": {
        "procs_priv": {
            "host": "TEXT",
            "db": "TEXT",
            "user": "TEXT",
            "routine_name": "TEXT",
            "routine_type": "TEXT",
            "proc_priv": "TEXT",
            "timestamp": "TEXT",
            "grantor": "TEXT",
        },
        "role_edges": {
            "from_host": "TEXT",
            "from_user": "TEXT",
            "to_host": "TEXT",
            "to_user": "TEXT",
            "with_admin_option": "TEXT",
        },
        "user": {
            "host": "TEXT",
            "user": "TEXT",
            "select_priv": "TEXT",
            "insert_priv": "TEXT",
            "update_priv": "TEXT",
            "delete_priv": "TEXT",
            "index_priv": "TEXT",
            "alter_priv": "TEXT",
            "create_priv": "TEXT",
            "drop_priv": "TEXT",
            "grant_priv": "TEXT",
            "create_view_priv": "TEXT",
            "show_view_priv": "TEXT",
            "create_routine_priv": "TEXT",
            "alter_routine_priv": "TEXT",
            "execute_priv": "TEXT",
            "trigger_priv": "TEXT",
            "event_priv": "TEXT",
            "create_tmp_table_priv": "TEXT",
            "lock_tables_priv": "TEXT",
            "references_priv": "TEXT",
            "reload_priv": "TEXT",
            "shutdown_priv": "TEXT",
            "process_priv": "TEXT",
            "file_priv": "TEXT",
            "show_db_priv": "TEXT",
            "super_priv": "TEXT",
            "repl_slave_priv": "TEXT",
            "repl_client_priv": "TEXT",
            "create_user_priv": "TEXT",
            "create_tablespace_priv": "TEXT",
            "create_role_priv": "TEXT",
            "drop_role_priv": "TEXT",
            "ssl_type": "TEXT",
            "ssl_cipher": "TEXT",
            "x509_issuer": "TEXT",
            "x509_subject": "TEXT",
            "plugin": "TEXT",
            "authentication_string": "TEXT",
            "password_expired": "TEXT",
            "password_last_changed": "TEXT",
            "password_lifetime": "TEXT",
            "account_locked": "TEXT",
            "password_reuse_history": "TEXT",
            "password_reuse_time": "TEXT",
            "password_require_current": "TEXT",
            "user_attributes": "TEXT",
            "max_questions": "INT",
            "max_updates": "INT",
            "max_connections": "INT",
            "max_user_connections": "INT",
        },
    },
}


def mapping_to_columns(schema: dict) -> List[Column]:
    """Convert a schema mapping into a list of Column instances"""
    depth = dict_depth(schema)
    if depth < 2:
        return []
    if depth == 2:
        # {table: {col: type}}
        schema = {"": schema}
        depth += 1
    if depth == 3:
        # {db: {table: {col: type}}}
        schema = {"def": schema}  # def is the default MySQL catalog
        depth += 1
    if depth != 4:
        raise MysqlError("Invalid schema mapping")

    result = []
    for catalog, dbs in schema.items():
        for db, tables in dbs.items():
            for table, cols in tables.items():
                for column, coltype in cols.items():
                    result.append(
                        Column(
                            name=column,
                            type=coltype,
                            table=table,
                            schema=db,
                            catalog=catalog,
                        )
                    )

    return result


def info_schema_tables(columns: Iterable[Column]) -> Dict[str, Dict[str, Table]]:
    """
    Convert a list of Column instances into a mapping of SQLGlot Tables.

    These Tables are used by SQLGlot to execute INFORMATION_SCHEMA queries.
    """
    ordinal_positions: dict[Any, int] = defaultdict(lambda: 0)

    data = {
        db: {k: Table(tuple(v.keys())) for k, v in tables.items()}
        for db, tables in INFO_SCHEMA.items()
    }

    tables = set()
    dbs = set()
    catalogs = set()

    info_schema_cols = mapping_to_columns(INFO_SCHEMA)

    for column in chain(columns, info_schema_cols):
        tables.add((column.catalog, column.schema, column.table))
        dbs.add((column.catalog, column.schema))
        catalogs.add(column.catalog)
        key = (column.catalog, column.schema, column.table)
        ordinal_position = ordinal_positions[key]
        ordinal_positions[key] += 1
        data["information_schema"]["columns"].append(
            (
                column.catalog,  # table_catalog
                column.schema,  # table_schema
                column.table,  # table_name
                column.name,  # column_name
                ordinal_position,  # ordinal_position
                column.default,  # column_default
                "YES" if column.is_nullable else "NO",  # is_nullable
                column.type,  # data_type
                None,  # character_maximum_length
                None,  # character_octet_length
                None,  # numeric_precision
                None,  # numeric_scale
                None,  # datetime_precision
                "NULL",  # character_set_name
                "NULL",  # collation_name
                column.type,  # column_type
                None,  # column_key
                None,  # extra
                None,  # privileges
                column.comment,  # column_comment
                None,  # generation_expression
                None,  # srs_id
            )
        )

    for catalog, db, table in sorted(tables):
        data["information_schema"]["tables"].append(
            (
                catalog,  # table_catalog
                db,  # table_schema
                table,  # table_name
                "SYSTEM TABLE" if db in INFO_SCHEMA else "BASE TABLE",  # table_type
                "MinervaSQL",  # engine
                "1.0",  # version
                None,  # row_format
                None,  # table_rows
                None,  # avg_row_length
                None,  # data_length
                None,  # max_data_length
                None,  # index_length
                None,  # data_free
                None,  # auto_increment
                None,  # create_time
                None,  # update_time
                None,  # check_time
                "utf8mb4_general_ci ",  # table_collation
                None,  # checksum
                None,  # create_options
                None,  # table_comment
            )
        )

    for catalog, db in sorted(dbs):
        data["information_schema"]["schemata"].append(
            (
                catalog,  # catalog_name
                db,  # schema_name
                "utf8mb4",  # default_character_set_name
                "utf8mb4_general_ci",  # default_collation_name
                None,  # sql_path
            )
        )

    return data


def show_statement_to_info_schema_query(show: exp.Show) -> exp.Select:
    kind = show.name.upper()
    if kind == "COLUMNS":
        outputs = [
            "column_name AS Field",
            "data_type AS Type",
            "is_nullable AS Null",
            "column_key AS Key",
            "column_default AS Default",
            "extra AS Extra",
        ]
        if show.args.get("full"):
            outputs.extend(
                [
                    "collation_name AS Collation",
                    "privileges AS Privileges",
                    "column_comment AS Comment",
                ]
            )
        table = show.text("target")
        select = (
            exp.select(*outputs)
            .from_("information_schema.columns")
            .where(f"table_name = '{table}'")
        )
        db = show.text("db")
        if db:
            select = select.where(f"table_schema = '{db}'")
        like = show.text("like")
        if like:
            select = select.where(f"column_name LIKE '{like}'")
    elif kind == "TABLES":
        outputs = ["table_name AS Table_name"]
        if show.args.get("full"):
            outputs.extend(["table_type AS Table_type"])

        select = exp.select(*outputs).from_("information_schema.tables")
        db = show.text("db")
        if db:
            select = select.where(f"table_schema = '{db}'")
        like = show.text("like")
        if like:
            select = select.where(f"table_name LIKE '{like}'")
    elif kind == "DATABASES":
        select = exp.select("schema_name AS Database").from_(
            "information_schema.schemata"
        )
        like = show.text("like")
        if like:
            select = select.where(f"schema_name LIKE '{like}'")
    elif kind == "INDEX":
        outputs = [
            "table_name AS Table",
            "non_unique AS Non_unique",
            "index_name AS Key_name",
            "seq_in_index AS Seq_in_index",
            "column_name AS Column_name",
            "collation AS Collation",
            "cardinality AS Cardinality",
            "sub_part AS Sub_part",
            "packed AS Packed",
            "nullable AS Null",
            "index_type AS Index_type",
            "comment AS Comment",
            "index_comment AS Index_comment",
            "is_visible AS Visible",
            "expression AS Expression",
        ]
        table = show.text("target")
        select = (
            exp.select(*outputs)
            .from_("information_schema.statistics")
            .where(f"table_name = '{table}'")
        )
        db = show.text("db")
        if db:
            select = select.where(f"table_schema = '{db}'")
    else:
        raise MysqlError(
            f"Unsupported SHOW command: {kind}", code=ErrorCode.NOT_SUPPORTED_YET
        )

    return select


def com_field_list_to_show_statement(com_field_list: ComFieldList) -> str:
    show = exp.Show(
        this="COLUMNS",
        target=exp.to_identifier(com_field_list.table),
    )
    if com_field_list.wildcard:
        show.set("like", exp.Literal.string(com_field_list.wildcard))
    return show.sql(dialect="mysql")


def like_to_regex(like: str) -> re.Pattern:
    like = like.replace("%", ".*?")
    like = like.replace("_", ".")
    return re.compile(like)


class BaseInfoSchema:
    """
    Base InfoSchema interface used by the `Session` class.
    """

    async def query(self, expression: exp.Expression) -> AllowedResult:
        ...


class InfoSchema(BaseInfoSchema):
    """
    InfoSchema implementation that uses SQLGlot to execute queries.
    """

    def __init__(self, tables: Dict[str, Dict[str, Table]]):
        self.tables = tables

    async def query(self, expression: exp.Expression) -> AllowedResult:
        expression = self._preprocess(expression)
        result = execute(expression, schema=INFO_SCHEMA, tables=self.tables)
        return result.rows, result.columns

    @classmethod
    def from_mapping(cls, mapping: dict) -> InfoSchema:
        columns = mapping_to_columns(mapping)
        return cls(info_schema_tables(columns))

    def _preprocess(self, expression: exp.Expression) -> exp.Expression:
        return expression.transform(_remove_collate)


def ensure_info_schema(schema: dict | BaseInfoSchema) -> BaseInfoSchema:
    if isinstance(schema, BaseInfoSchema):
        return schema
    return InfoSchema.from_mapping(schema)


def _remove_collate(node: exp.Expression) -> exp.Expression:
    """
    SQLGlot's executor doesn't support collation specifiers.

    Just remove them for now.
    """
    if isinstance(node, exp.Collate):
        return node.left
    return node
