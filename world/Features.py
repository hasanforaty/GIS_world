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

    def getPrimaryColumn(self, con):
        with con.cursor() as cursor:
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

    def getGeometryColumns(self, con):
        with con.cursor() as cursor:
            cursor.execute("select f_geometry_column from geometry_columns where f_table_name = %s", (self.table_name,))
            return cursor.fetchone()[0]

    def create(self, geometry: str, properties: dict):

        geos = GEOSGeometry(geometry)
        wkb = geos.wkb
        with getDatabaseConnection() as con:
            with con.cursor(cursor_factory=RealDictCursor) as curser:
                geoColumn = self.getGeometryColumns(con)
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
                return geos.geojson

    def get(self, **kwargs):
        limit = kwargs.pop('limit', 1000)
        offset = kwargs.pop('offset', 0)
        pk = kwargs.pop('pk',None)

        with (getDatabaseConnection() as con):
            geo_table = self.getGeometryColumns(con)
            if pk is not None:
                pk_column = self.getPrimaryColumn(con)
                kwargs[pk_column] = pk
            sql_command_query = ""
            if len(kwargs) > 0:
                sql_command_query = "where "
                for key, value in kwargs.items():
                    sql_command_query += f" And {key} LIKE {value} "
                sql_command_query.replace('And', '', 1)
            sql_schema = SQL('Select * from {} LIMIT %s OFFSET %s '+sql_command_query).format(
                Identifier(self.table_name),
            )

            with con.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql_schema, (limit, offset))
                results = cursor.fetchall()
                geo_jsons = []
                for result in results:
                    geo_bin = result.pop(geo_table, None)
                    geometry = GEOSGeometry(geo_bin)
                    geo_json = {
                        "geometry": geometry.geojson,
                        "properties": result,
                    }
                    geo_jsons.append(geo_json)

                return geo_jsons
