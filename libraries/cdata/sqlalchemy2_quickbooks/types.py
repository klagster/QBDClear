from sqlalchemy import types as sqlTypes

CDATA_DATA_TYPE_NOT_SPECIFIED = -99
CDATA_DATA_TYPE_BINARY = 1
CDATA_DATA_TYPE_VARBINARY = 1
CDATA_DATA_TYPE_LONGVARBINARY = 1
CDATA_DATA_TYPE_BLOB = 1
CDATA_DATA_TYPE_BOOLEAN = 3
CDATA_DATA_TYPE_DATE = 5
CDATA_DATA_TYPE_TIMESTAMP = 6
CDATA_DATA_TYPE_DECIMAL = 7
CDATA_DATA_TYPE_DOUBLE = 8
CDATA_DATA_TYPE_TINYINT= 2
CDATA_DATA_TYPE_SMALLINT = 10
CDATA_DATA_TYPE_INTEGER = 11
CDATA_DATA_TYPE_BIGINT = 12
CDATA_DATA_TYPE_FLOAT = 15
CDATA_DATA_TYPE_VARCHAR = 16
CDATA_DATA_TYPE_TIME = 17
CDATA_DATA_TYPE_OBJECT = 13
CDATA_DATA_TYPE_NUMERIC = 21
CDATA_DATA_TYPE_UUID = 9


class BINARY(sqlTypes.BINARY):

  __visit_name__ = "BINARY"


class BOOLEAN(sqlTypes.BOOLEAN):

  __visit_name__ = "BOOLEAN"


class DATE(sqlTypes.DATE):

  __visit_name__ = "DATE"


class TIMESTAMP(sqlTypes.TIMESTAMP):

  __visit_name__ = "TIMESTAMP"


class DECIMAL(sqlTypes.DECIMAL):

  __visit_name__ = "DECIMAL"


class DOUBLE(sqlTypes.Float):

  __visit_name__ = "DOUBLE"


class TINYINT(sqlTypes.TypeEngine):

  __visit_name__ = "TINYINT"


class SMALLINT(sqlTypes.SMALLINT):

  __visit_name__ = "SMALLINT"


class INTEGER(sqlTypes.INTEGER):

  __visit_name__ = "INTEGER"


class BIGINT(sqlTypes.BIGINT):

  __visit_name__ = "BIGINT"


class FLOAT(sqlTypes.FLOAT):

  __visit_name__ = "FLOAT"


class VARCHAR(sqlTypes.VARCHAR):

  __visit_name__ = "VARCHAR"


class TIME(sqlTypes.TIME):

  __visit_name__ = "TIME"


OBJECT = VARCHAR


NUMERIC = FLOAT


UUID = VARCHAR


CDATA_TYPES_TO_SQL_TYPES = {
  CDATA_DATA_TYPE_NOT_SPECIFIED: VARCHAR,
  CDATA_DATA_TYPE_BINARY: BINARY,
  CDATA_DATA_TYPE_BOOLEAN: BOOLEAN,
  CDATA_DATA_TYPE_DATE: DATE,
  CDATA_DATA_TYPE_TIMESTAMP: TIMESTAMP,
  CDATA_DATA_TYPE_DECIMAL: DECIMAL,
  CDATA_DATA_TYPE_DOUBLE: DOUBLE,
  CDATA_DATA_TYPE_TINYINT: TINYINT,
  CDATA_DATA_TYPE_SMALLINT: SMALLINT,
  CDATA_DATA_TYPE_INTEGER: INTEGER,
  CDATA_DATA_TYPE_BIGINT: BIGINT,
  CDATA_DATA_TYPE_FLOAT: FLOAT,
  CDATA_DATA_TYPE_VARCHAR: VARCHAR,
  CDATA_DATA_TYPE_TIME: TIME,
  CDATA_DATA_TYPE_OBJECT: OBJECT,
  CDATA_DATA_TYPE_NUMERIC: NUMERIC,
  CDATA_DATA_TYPE_UUID: UUID
}
