from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from .log import class_logger, mtd_logger
from . import VARCHAR, CDATA_TYPES_TO_SQL_TYPES

RESERVED_WORDS = set(
  # TODO
)


class BaseExecutionContext(default.DefaultExecutionContext):
  def should_autocommit_text(self, statement):
    # Autocommit everything
    return True

  def create_server_side_cursor(self):
    super(BaseExecutionContext, self).create_server_side_cursor()

  def pre_exec(self):
    super(BaseExecutionContext, self).pre_exec()

  def post_exec(self):
    super(BaseExecutionContext, self).post_exec()

  def get_lastrowid(self):
    rp = self.connection.exec_driver_sql("SELECT SCOPE_IDENTITY()")
    result = [row[0] for row in rp.fetchall()]
    if len(result) == 0:
      return None
    else:
      return result[0]

class BaseCompiler(compiler.SQLCompiler):
  # Overridden from super SQLCompiler
  render_table_with_column_in_update_from = True

  pass


class BaseDDLCompiler(compiler.DDLCompiler):
  pass


class BaseTypeCompiler(compiler.GenericTypeCompiler):
  def visit_DOUBLE(self, type_, **kw):
    return "DOUBLE"

class BaseIdentifierPreparer(compiler.IdentifierPreparer):
  reserved_words = RESERVED_WORDS


@class_logger
class BaseDialect(default.DefaultDialect):
  name = "Base"
  default_paramstyle = 'format'

  supports_comments = True
  inline_comments = True
  supports_native_decimal = True

  statement_compiler = BaseCompiler
  ddl_compiler = BaseDDLCompiler
  type_compiler = BaseTypeCompiler
  execution_ctx_cls = BaseExecutionContext
  preparer = BaseIdentifierPreparer

  ischema_names = {}

  postfetch_lastrowid = False
  implicit_returning = False
  supports_empty_insert = False
  supports_native_boolean = False
  supports_default_values = False
  supports_sane_multi_rowcount = False
  supports_native_enum = False
  isolation_level = None

  @mtd_logger
  def __init__(self, isolation_level=None, **kwargs):
    super(BaseDialect, self).__init__(**kwargs)
    self.isolation_level = isolation_level

  @mtd_logger
  def do_rollback(self, dbapi_connection):
    # Transaction not supported yet
    pass

  @mtd_logger
  def do_commit(self, dbapi_connection):
    # Transaction not supported yet
    pass

  @mtd_logger
  def set_isolation_level(self, connection, level):
    # Transaction not supported yet
    pass

  @mtd_logger
  def has_table(self, connection, table_name, schema=None):
    """
    :return: bool
    """
    ret = bool(self.get_columns(connection, table_name, schema=schema))
    return ret

  @mtd_logger
  def get_foreign_keys(self, connection, table_name, schema=None, **kw):
    """
    :returns return foreign key information as a list of
             dicts with these keys:
      name
        the constraint's name

      constrained_columns
        a list of column names that make up the foreign key

      referred_schema
        the name of the referred schema

      referred_table
        the name of the referred table

      referred_columns
        a list of column names in the referred table that correspond to
        constrained_columns
    """
    sql = "SELECT " \
          "CatalogName," \
          "SchemaName," \
          "TableName," \
          "ColumnName," \
          "PrimaryKeyName," \
          "ForeignKeyName," \
          "ReferencedCatalogName," \
          "ReferencedSchemaName," \
          "ReferencedTableName," \
          "ReferencedColumnName," \
          "ForeignKeyType " \
          "FROM SYS_FOREIGNKEYS " \
          "WHERE TableName=?"
    parameters = [table_name]
    if schema:
      sql += " AND SchemaName=?"
      parameters.append(schema)

    parameters = tuple(parameters)
    rp = connection.exec_driver_sql(sql, parameters)

    foreign_keys = [
      {
        "name": row[5],
        "constrained_columns": [row[4]],
        "referred_schema": row[8],
        "referred_table": row[9],
        "referred_columns": [row[10]],
        "catalog_name": row[0],
        "schema_name": row[1],
        "table_name": row[2],
        "column_name": row[3],
        "primary_key_name": row[4],
        "referred_catalog": row[7],
        "foreign_key_type": row[11],
      } for row in rp.fetchall()
    ]

    return foreign_keys

  @mtd_logger
  def get_indexes(self, connection, table_name, schema=None, **kw):
    """
    :return: return index information as a list of
             dictionaries with these keys:
        name
          the index's name

        column_names
          list of column names in order

        unique
          boolean
    """
    sql = "SELECT IndexName, ColumnName, IsUnique FROM SYS_INDEXES WHERE TableName=?"
    parameters = [table_name]
    if schema:
      sql += " AND SchemaName=?"
      parameters.append(schema)

    parameters = tuple(parameters)
    rp = connection.exec_driver_sql(sql, parameters)

    indexes = [
      {
        "name": row[1],
        "column_names": [row[2]],
        "unique": self._get_value_as_bool(row[3]),
      } for row in rp.fetchall()
    ]

    return indexes

  @mtd_logger
  def get_schema_names(self, connection, **kw):
    sql = "SELECT SchemaName FROM sys_schemas"
    rp = connection.exec_driver_sql(sql)
    schemas = [row[0] for row in rp.fetchall()]
    return schemas

  @mtd_logger
  def get_table_names(self, connection, schema=None, **kw):
    """
    :return: a list of table names for `schema`
    """
    sql = "SELECT TableName FROM SYS_TABLES WHERE TableType='TABLE'"
    parameters = []
    if schema:
      sql += " AND SchemaName=?"
      parameters.append(schema)

    parameters = tuple(parameters)
    rp = connection.exec_driver_sql(sql, parameters)
    tables = [row[0] for row in rp.fetchall()]
    return tables

  @mtd_logger
  def get_view_names(self, connection, schema=None, **kw):
    """
    :return: Return a list of all view names available in the database.
    """
    sql = "SELECT TableName FROM SYS_TABLES WHERE TableType='VIEW'"
    parameters = []
    if schema:
      sql += " AND SchemaName=?"
      parameters.append(schema)

    parameters = tuple(parameters)
    rp = connection.exec_driver_sql(sql, parameters)
    views = [row[0] for row in rp.fetchall()]
    return views

  @mtd_logger
  def get_columns(self, connection, table_name, schema=None, **kw):
    """
    :returns return column information as a list of
             dictionaries with these keys::
      name
        the column's name

      type
        [sqlalchemy.types#TypeEngine]

      nullable
        boolean

      default
        the column's default value

      autoincrement
        boolean

      sequence
        a dictionary of the form
            {'name' : str, 'start' :int, 'increment': int, 'minvalue': int,
             'maxvalue': int, 'nominvalue': bool, 'nomaxvalue': bool,
             'cycle': bool, 'cache': int, 'order': bool}

      Additional column attributes may be present.
    """
    sql = "SELECT " \
          "CatalogName," \
          "SchemaName," \
          "TableName," \
          "ColumnName," \
          "DataType," \
          "NumericScale," \
          "IsNullable," \
          "Ordinal," \
          "IsAutoIncrement," \
          "IsKey," \
          "NumericPrecision " \
          "FROM sys_tablecolumns " \
          "WHERE TableName=?"
    parameters = [table_name]
    if schema:
      sql += " AND SchemaName=?"
      parameters.append(schema)

    parameters = tuple(parameters)
    rp = connection.exec_driver_sql(sql, parameters)

    columns = [
      {
        "name": row[3],
        "type": self._get_column_type(row[4]),
        "nullable": row[6],
        "default": None,
        "autoincrement": self._get_value_as_bool(row[8]),
        "is_key": self._get_value_as_bool(row[9]),
        "sequence": {
          "name": row[4],
          "start": 0,
          "increment": 1,
          "minvalue": 0,
          "maxvalue": 0,
          "nominvalue": True,
          "nomaxvalue": True,
          "cycle": False,
          "cache": 0,
          "order": False,
        },
        "catalog_name": row[0],
        "schema_name": row[1],
        "table_name": row[2],
      } for row in rp.fetchall()
    ]

    return columns

  @mtd_logger
  def get_primary_keys(self, connection, table_name, schema=None, **kw):
    """
    :return information about primary keys in `table_name`.
    """
    sql = "SELECT " \
          "CatalogName," \
          "SchemaName," \
          "TableName," \
          "ColumnName," \
          "KeyName " \
          "FROM SYS_PRIMARYKEYS " \
          "WHERE TableName=?"
    parameters = [table_name]
    if schema:
      sql += " AND SchemaName=?"
      parameters.append(schema)

    parameters = tuple(parameters)
    rp = connection.exec_driver_sql(sql, parameters)

    primary_keys = [
      {
        "catalog_name": row[0],
        "schema_name": row[1],
        "table_name": row[2],
        "column_name": row[3],
        "name": row[4],
      } for row in rp.fetchall()
    ]

    return primary_keys

  @mtd_logger
  def get_pk_constraint(self, connection, table_name, schema=None, **kw):
    """
    :returns return primary key information as a
             dictionary with these keys::
      constrained_columns
        a list of column names that make up the primary key

      name
        optional name of the primary key constraint.
    """
    sql = "SELECT " \
          "ColumnName," \
          "PrimaryKeyName " \
          "FROM SYS_KEYCOLUMNS " \
          "WHERE IsKey='True' AND TableName=?"
    parameters = [table_name]
    if schema:
      sql += " AND SchemaName=?"
      parameters.append(schema)

    parameters = tuple(parameters)
    rp = connection.exec_driver_sql(sql, parameters)
    pk_constraint = {
      "constrained_columns": [],
      "name": None
    }

    for row in rp.fetchall():
      pk_constraint["constrained_columns"].append(row[0])
      pk_constraint["name"] = row[1]

    return pk_constraint

  @staticmethod
  def _build_table_name(*args):
    return ".".join([i for i in args if i is not None])

  @staticmethod
  def _get_column_type(cdata_type):
    if type(cdata_type) is int or type(cdata_type) is str:
      cdata_type = int(cdata_type)
      if cdata_type in CDATA_TYPES_TO_SQL_TYPES:
        return CDATA_TYPES_TO_SQL_TYPES[cdata_type]

    return VARCHAR

  @staticmethod
  def _get_value_as_bool(val):
    if type(val) is str:
      val_lower = val.lower()
      if val_lower == "true" or val_lower == "yes":
        return True
      return False

    return bool(val)

