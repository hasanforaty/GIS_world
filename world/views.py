import datetime
import os
import zipfile

import psycopg2
from django.contrib.gis.geos import GEOSGeometry
from psycopg2.sql import Identifier, SQL
from psycopg2.extras import RealDictCursor
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from urllib.parse import urlparse, parse_qsl, urlencode

from rest_framework.viewsets import ViewSet

from world.serializers import ShapeFileSerializer


class ShapeFileUploadViewSet(ViewSet):

    def create(self, request):
        serializer = ShapeFileSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(status.HTTP_400_BAD_REQUEST)


class FeaturesApiView(APIView):

    def post(self, request, layer_name, format=None):
        try:
            geos = GEOSGeometry(str(request.data['geometry']))
            properties = request.data['properties']
            wkb = geos.wkb
            with getDatabaseConnection() as connection:
                with connection.cursor(cursor_factory=RealDictCursor) as curser:
                    geoColumn = getGeometryColumns(connection, layer_name)
                    sql_command = "Insert into {} " + "(" + geoColumn
                    for key in properties.keys():
                        sql_command += ', ' + key
                    sql_command += ') values (%s'
                    for _ in properties.values():
                        sql_command += ', %s'
                    sql_command += ")"
                    print(sql_command)
                    sql = SQL(sql_command).format(Identifier(layer_name))
                    value = list(properties.values())
                    value.insert(0, wkb)
                    curser.execute(sql, value)
                    result = geos.geojson
        except ValueError as e:
            return Response(status=422, exception=e)
        except psycopg2.errors.InvalidParameterValue as e:
            return Response(status=400, data="Missmatch :" + str(e))
        return Response(status=200, data=result)

    def get(self, request, layer_name):
        Limit = 1000
        Offset = 0
        page = 1
        previous_url = None
        url = request.build_absolute_uri().split("?")[0]
        try:
            query_set = request.GET
            page = int(query_set['page'])
            Limit = query_set['limit']
            Offset = (page - 1) * Limit
            if page > 1:
                query = f'?page={page - 1}&limit={Limit}'
                previous_url = url + query
        except KeyError as e:
            pass
        query = f'?page={page + 1}&limit={Limit}'
        next_url = url + query
        with (getDatabaseConnection() as connection):
            geo_table = getGeometryColumns(connection, layer_name)
            sql_schema = SQL('Select * from {} LIMIT %s OFFSET %s').format(
                Identifier(layer_name),
            )
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                try:
                    cursor.execute(sql_schema, (Limit, Offset))
                    results = cursor.fetchall()
                    list = []
                    for result in results:
                        geo_bin = result.pop(geo_table, None)
                        geometry = GEOSGeometry(geo_bin)
                        geo_json = {
                            "geometry": geometry.geojson,
                            "properties": result,
                        }
                        list.append(geo_json)

                    return Response(status=200, data={
                        "page": page,
                        "next": next_url,
                        "previous": previous_url,
                        "results": list
                    })
                except Exception as e:
                    raise e


class FeatureDetailApiView(APIView):
    def get(self, reqeust, layer_name, pk):
        with (getDatabaseConnection() as connection):
            geo_table = getGeometryColumns(connection, layer_name)
            primary_column = getPrimaryColumn(connection, layer_name)
            sql_schema = SQL('Select * from {} where {}=%s').format(
                Identifier(layer_name), Identifier(primary_column)
            )
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                try:

                    cursor.execute(sql_schema, (pk,))
                    result = cursor.fetchone()

                    geo_bin = result.pop(geo_table, None)
                    geometry = GEOSGeometry(geo_bin)
                    geo_json = {
                        "geometry": geometry.geojson,
                        "properties": result,
                    }

                    return Response(status=200, data=geo_json)
                except Exception as e:
                    raise e

    def put(self, request, layer_name, pk):
        try:
            geos = GEOSGeometry(str(request.data['geometry']))

            properties = request.data['properties']
            wkb = geos.wkb
            with getDatabaseConnection() as connection:
                primary_column = getPrimaryColumn(connection, layer_name)
                properties.pop(primary_column, None)
                with connection.cursor(cursor_factory=RealDictCursor) as curser:
                    geoColumn = getGeometryColumns(connection, layer_name)
                    sql_command = "Update {} " + "Set " + geoColumn + " = %s"
                    for key in properties.keys():
                        sql_command += ', ' + key + " = %s"
                    sql_command += " Where  {} = " + pk
                    print(sql_command)
                    sql = SQL(sql_command).format(Identifier(layer_name), Identifier(primary_column))
                    value = list(properties.values())
                    value.insert(0, wkb)
                    curser.execute(sql, value)
        except ValueError as e:
            return Response(status=422, exception=e)
        except psycopg2.errors.InvalidParameterValue as e:
            return Response(status=400, data="Missmatch :" + str(e))
        return Response(status=200)


def getDatabase():
    NAME = 'geodjango'
    USER = "geo"
    PASSWORD = "sahan"
    return f'user={USER}  dbname={NAME} host=localhost port=5432 password={PASSWORD}'


def getDatabaseConnection():
    NAME = 'geodjango'
    USER = "geo"
    PASSWORD = 'sahan'
    HOST = 'localhost'
    return psycopg2.connect('dbname=' + NAME + ' user=' + USER + ' password=' + PASSWORD + 'host=' + HOST + 'port=5432')


def getGeometryColumns(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute("select f_geometry_column from geometry_columns where f_table_name = %s", (table_name,))
        return cursor.fetchone()[0]


def getPrimaryColumn(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type
            FROM
                pg_class AS c
                JOIN pg_index AS i ON c.oid = i.indrelid AND i.indisprimary
                JOIN pg_attribute AS a ON c.oid = a.attrelid AND a.attnum = ANY(i.indkey)
                WHERE c.oid = %s::regclass
            """, (table_name,)
        )
        return cursor.fetchone()[0]
