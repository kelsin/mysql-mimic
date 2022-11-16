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
        "schemata": {
            "catalog_name": "TEXT",
            "schema_name": "TEXT",
            "default_character_set_name": "TEXT",
            "default_collation_name": "TEXT",
            "sql_path": "TEXT",
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
        "character_sets": {
            "character_set_name": "TEXT",
            "default_collate_name": "TEXT",
            "description": "TEXT",
            "maxlen": "INT",
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
    }
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
        "information_schema": {
            k: Table(tuple(v.keys()))
            for k, v in INFO_SCHEMA["information_schema"].items()
        }
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
                "SYSTEM TABLE"
                if db == "information_schema"
                else "BASE TABLE",  # table_type
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
        result = execute(expression, schema=INFO_SCHEMA, tables=self.tables)
        return result.rows, result.columns

    @classmethod
    def from_mapping(cls, mapping: dict) -> InfoSchema:
        columns = mapping_to_columns(mapping)
        return cls(info_schema_tables(columns))


def ensure_info_schema(schema: dict | BaseInfoSchema) -> BaseInfoSchema:
    if isinstance(schema, BaseInfoSchema):
        return schema
    return InfoSchema.from_mapping(schema)
