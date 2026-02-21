import datetime
import decimal
import uuid
from dataclasses import dataclass


@dataclass(frozen=True)
class PgType:
    name: str
    python_type: type


PG_TYPES: dict[int, PgType] = {
    # scalars
    16: PgType("bool", bool),
    17: PgType("bytea", bytes),
    18: PgType("char", str),
    19: PgType("name", str),
    20: PgType("int8", int),
    21: PgType("int2", int),
    23: PgType("int4", int),
    25: PgType("text", str),
    26: PgType("oid", int),
    114: PgType("json", object),
    700: PgType("float4", float),
    701: PgType("float8", float),
    790: PgType("money", str),
    1042: PgType("bpchar", str),
    1043: PgType("varchar", str),
    1082: PgType("date", datetime.date),
    1083: PgType("time", datetime.time),
    1114: PgType("timestamp", datetime.datetime),
    1184: PgType("timestamptz", datetime.datetime),
    1186: PgType("interval", datetime.timedelta),
    1700: PgType("numeric", decimal.Decimal),
    2950: PgType("uuid", uuid.UUID),
    3802: PgType("jsonb", object),
    # arrays
    199: PgType("_json", list[object]),
    791: PgType("_money", list[str]),
    1000: PgType("_bool", list[bool]),
    1001: PgType("_bytea", list[bytes]),
    1002: PgType("_char", list[str]),
    1003: PgType("_name", list[str]),
    1005: PgType("_int2", list[int]),
    1007: PgType("_int4", list[int]),
    1009: PgType("_text", list[str]),
    1014: PgType("_bpchar", list[str]),
    1015: PgType("_varchar", list[str]),
    1016: PgType("_int8", list[int]),
    1021: PgType("_float4", list[float]),
    1022: PgType("_float8", list[float]),
    1028: PgType("_oid", list[int]),
    1115: PgType("_timestamp", list[datetime.datetime]),
    1182: PgType("_date", list[datetime.date]),
    1183: PgType("_time", list[datetime.time]),
    1185: PgType("_timestamptz", list[datetime.datetime]),
    1187: PgType("_interval", list[datetime.timedelta]),
    1231: PgType("_numeric", list[decimal.Decimal]),
    2951: PgType("_uuid", list[uuid.UUID]),
    3807: PgType("_jsonb", list[object]),
}
