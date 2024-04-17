import datetime
import os
import zipfile

import psycopg2
from django.contrib.gis.geos import GEOSGeometry
from psycopg2.sql import Identifier, SQL
from rest_framework.response import Response
from rest_framework.views import APIView


class FileUploadAPIView(APIView):
    def post(self, request, format=None):

        try:
            file_obj = request.data['file']
            today_date = datetime.date.today().strftime("%Y%m%d")
            folder_path = f"./tmp/shapefiles/{file_obj.name}_{today_date}"

            with zipfile.ZipFile(file_obj, 'r') as zip_ref:
                zip_ref.extractall(path=folder_path)
            databaseInfo = getDatabase()
            file_name = file_obj.name.rsplit(".")[0] + today_date
            command = f"""
            ogr2ogr -f PostgreSQL  PG:"{databaseInfo}" {folder_path} -nln {file_name} -nlt MULTIPOLYGON"""
            print(command)
            os.system(command)
        except Exception as e:
            print(e)
            return Response(status=500)
        return Response(status=204)


class FeatureApiView(APIView):

    def post(self, request, layer_name, format=None):
        try:
            geos = GEOSGeometry(str(request.data['geometry']))
            properties = request.data['properties']
            wkb = geos.wkb
            with getDatabaseConnection() as connection:
                with connection.cursor() as curser:
                    geoColumn = getGeometryColumns(curser, layer_name)
                    sql_command = "Insert into {} " + "(" + geoColumn
                    for key in properties.keys():
                        sql_command += ', ' + key
                    sql_command += ') values (%s'
                    for value in properties.values():
                        sql_command += ', %s'
                    sql_command += ")"
                    print(sql_command)
                    sql = SQL(sql_command).format(Identifier(layer_name))
                    value = list(properties.values())
                    value.insert(0, wkb)
                    curser.execute(sql, value)
        except ValueError as e:
            return Response(status=422, exception=e)
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


def getGeometryColumns(curser, table_name):
    curser.execute("select f_geometry_column from geometry_columns where f_table_name = %s", (table_name,))
    return curser.fetchone()[0]
