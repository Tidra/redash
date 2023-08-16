import base64
import logging
import os
from base64 import b64decode
from tempfile import NamedTemporaryFile

from redash.query_runner import (
    TYPE_BOOLEAN,
    TYPE_DATE,
    TYPE_DATETIME,
    TYPE_FLOAT,
    TYPE_INTEGER,
    TYPE_STRING,
    BaseSQLQueryRunner,
    JobTimeoutException,
    register,
)
from redash.utils import json_dumps

logger = logging.getLogger(__name__)

try:
    from pyhive import hive
    from pyhive.exc import DatabaseError
    from thrift.transport import THttpClient
    from krbcontext import krbcontext

    enabled = True
except ImportError:
    enabled = False

COLUMN_NAME = 0
COLUMN_TYPE = 1

types_map = {
    "BIGINT_TYPE": TYPE_INTEGER,
    "TINYINT_TYPE": TYPE_INTEGER,
    "SMALLINT_TYPE": TYPE_INTEGER,
    "INT_TYPE": TYPE_INTEGER,
    "DOUBLE_TYPE": TYPE_FLOAT,
    "DECIMAL_TYPE": TYPE_FLOAT,
    "FLOAT_TYPE": TYPE_FLOAT,
    "REAL_TYPE": TYPE_FLOAT,
    "BOOLEAN_TYPE": TYPE_BOOLEAN,
    "TIMESTAMP_TYPE": TYPE_DATETIME,
    "DATE_TYPE": TYPE_DATE,
    "CHAR_TYPE": TYPE_STRING,
    "STRING_TYPE": TYPE_STRING,
    "VARCHAR_TYPE": TYPE_STRING,
}


class Hive(BaseSQLQueryRunner):
    should_annotate_query = False
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "number"},
                "database": {"type": "string"},
                "username": {"type": "string"},
                "auth": {"type": "string", "enum": ["NONE", "NOSASL", "KERBEROS", "LDAP", "CUSTOM"]},
                "kerberosServiceName": {"type": "string"},
                "keytabFile": {"type": "string"},
                "principal": {"type": "string"},
            },
            "order": ["host", "port", "database", "username", "auth"],
            "extra_options": ["kerberosServiceName", "keytabFile", "principal"],
            "required": ["host"],
            "secret": ["keytabFile"],
        }

    @classmethod
    def type(cls):
        return "hive"

    @classmethod
    def enabled(cls):
        return enabled

    def _get_tables(self, schema):
        schemas_query = "show schemas"

        tables_query = "show tables in %s"

        columns_query = "show columns in %s.%s"

        for schema_name in [
            a for a in [str(a["database_name"]) for a in self._run_query_internal(schemas_query)] if len(a) > 0
        ]:
            for table_name in [
                a
                for a in [str(a["tab_name"]) for a in self._run_query_internal(tables_query % schema_name)]
                if len(a) > 0
            ]:
                columns = [
                    a
                    for a in [
                        str(a["field"]) for a in self._run_query_internal(columns_query % (schema_name, table_name))
                    ]
                    if len(a) > 0
                ]

                if schema_name != "default":
                    table_name = "{}.{}".format(schema_name, table_name)

                schema[table_name] = {"name": table_name, "columns": columns}
        return list(schema.values())

    def _get_connection(self):
        host = self.configuration["host"]
        auth = self.configuration.get("auth", None)
        connection = None
        try:
            if auth == 'KERBEROS':
                principal = self.configuration["principal"]
                keytab_file = self._generate_keytab_file()
                krb_ccache_file = '/tmp/krb5cc_hive_{0}'.format(principal)
                with krbcontext(using_keytab=True, principal=principal, keytab_file=keytab_file,
                                ccache_file=krb_ccache_file):
                    connection = hive.connect(
                        host=host,
                        port=self.configuration.get("port", None),
                        database=self.configuration.get("database", "default"),
                        username=self.configuration.get("username", None),
                        auth=auth,
                        kerberos_service_name=self.configuration.get('kerberosServiceName', None),
                    )
            else:
                connection = hive.connect(
                    host=host,
                    port=self.configuration.get("port", None),
                    database=self.configuration.get("database", "default"),
                    username=self.configuration.get("username", None),
                    auth=auth,
                )
        except Exception as e:
            raise e
        finally:
            if keytab_file:
                os.remove(keytab_file)
        return connection

    def _generate_keytab_file(self):
        keytab_encoded_bytes = self.configuration.get("keytabFile", None)
        if keytab_encoded_bytes:
            with NamedTemporaryFile(mode="w+b", delete=False, suffix=".keytab") as keytab_file:
                keytab_bytes = b64decode(keytab_encoded_bytes)
                keytab_file.write(keytab_bytes)
            return keytab_file.name
        return None

    def run_query(self, query, user):
        connection = None
        try:
            cursor = connection.cursor()
            cursor.execute(query)

            column_names = []
            columns = []

            for column in cursor.description:
                column_name = column[COLUMN_NAME]
                column_names.append(column_name)

                columns.append(
                    {
                        "name": column_name,
                        "friendly_name": column_name,
                        "type": types_map.get(column[COLUMN_TYPE], None),
                    }
                )

            rows = [dict(zip(column_names, row)) for row in cursor]
            data = {"columns": columns, "rows": rows}
            json_data = json_dumps(data)
            error = None
        except (KeyboardInterrupt, JobTimeoutException):
            if connection:
                connection.cancel()
            raise
        except DatabaseError as e:
            try:
                error = e.args[0].status.errorMessage
            except AttributeError:
                error = str(e)
            json_data = None
        finally:
            if connection:
                connection.close()

        return json_data, error


class HiveHttp(Hive):
    @classmethod
    def name(cls):
        return "Hive (HTTP)"

    @classmethod
    def type(cls):
        return "hive_http"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "number"},
                "database": {"type": "string"},
                "username": {"type": "string"},
                "http_scheme": {
                    "type": "string",
                    "title": "HTTP Scheme (http or https)",
                    "default": "https",
                },
                "http_path": {"type": "string", "title": "HTTP Path"},
                "http_password": {"type": "string", "title": "Password"},
            },
            "order": [
                "host",
                "port",
                "http_path",
                "username",
                "http_password",
                "database",
                "http_scheme",
            ],
            "secret": ["http_password"],
            "required": ["host", "http_path"],
        }

    def _get_connection(self):
        host = self.configuration["host"]

        scheme = self.configuration.get("http_scheme", "https")

        # if path is set but is missing initial slash, append it
        path = self.configuration.get("http_path", "")
        if path and path[0] != "/":
            path = "/" + path

        # if port is set prepend colon
        port = self.configuration.get("port", "")
        if port:
            port = ":" + str(port)

        http_uri = "{}://{}{}{}".format(scheme, host, port, path)

        # create transport
        transport = THttpClient.THttpClient(http_uri)

        # if username or password is set, add Authorization header
        username = self.configuration.get("username", "")
        password = self.configuration.get("http_password", "")
        if username or password:
            auth = base64.b64encode(username.encode("ascii") + b":" + password.encode("ascii"))
            transport.setCustomHeaders({"Authorization": "Basic " + auth.decode()})

        # create connection
        connection = hive.connect(thrift_transport=transport)

        return connection


register(Hive)
register(HiveHttp)
