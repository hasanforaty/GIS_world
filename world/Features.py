from multiprocessing import connection

import psycopg2
from django.contrib.gis.geos import GEOSGeometry
from psycopg2.extras import RealDictCursor
from psycopg2.sql import Identifier, SQL


def getDatabaseConnection():
    NAME = 'geodjango'
    USER = "geo"
    PASSWORD = 'sahan'
    HOST = 'localhost'
    return psycopg2.connect(
        'dbname=' + NAME + ' user=' + USER + ' password=' + PASSWORD + 'host=' + HOST + 'port=5432')


class Features:
    table_name: str

    def __init__(self, table_name):
        self.table_name = table_name

    def getPrimaryColumn(self, connection):
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type
                FROM
                    pg_class AS c
                    JOIN pg_index AS i ON c.oid = i.indrelid AND i.indisprimary
                    JOIN pg_attribute AS a ON c.oid = a.attrelid AND a.attnum = ANY(i.indkey)
                    WHERE c.oid = %s::regclass
                """, (self.table_name,)
            )
            return cursor.fetchone()[0]

    def getGeometryColumns(self, connection):
        with connection.cursor() as cursor:
            cursor.execute("select f_geometry_column from geometry_columns where f_table_name = %s", (self.table_name,))
            return cursor.fetchone()[0]

    def create(self, geometry: str, properties: dict):

        geos = GEOSGeometry(geometry)
        wkb = geos.wkb
        with getDatabaseConnection() as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as curser:
                geoColumn = self.getGeometryColumns(connection)
                sql_command = "Insert into {} " + "(" + geoColumn
                for key in properties.keys():
                    sql_command += ', ' + key
                sql_command += ') values (%s'
                for _ in properties.values():
                    sql_command += ', %s'
                sql_command += ")"
                print(sql_command)
                sql = SQL(sql_command).format(Identifier(self.table_name))
                value = list(properties.values())
                value.insert(0, wkb)
                curser.execute(sql, value)
                result = geos.geojson
