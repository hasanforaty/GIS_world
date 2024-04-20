import datetime
import os
import zipfile
from geo.Geoserver import Geoserver
from rest_framework import serializers


class ShapeFileSerializer(serializers.Serializer):
    file = serializers.FileField()

    def create(self, validated_data):
        file_obj = validated_data.get('file')
        today_date = datetime.date.today().strftime("%Y%m%d")
        folder_path = f"./tmp/shapefiles/{file_obj.name}_{today_date}"
        file_name = file_obj.name.rsplit(".")[0] + today_date
        with zipfile.ZipFile(file_obj, 'r') as zip_ref:
            zip_ref.extractall(path=folder_path)
        databaseInfo = getDatabase()
        command = f"""
                    ogr2ogr  -f PostgreSQL  PG:"{databaseInfo}" {folder_path} -nln {file_name} -nlt MULTIPOLYGON """
        os.system(command)
        username = os.environ.get('GEOSERVER_USERNAME')
        password = os.environ.get('GEOSERVER_PASSWORD')
        workspace = os.environ.get('GEOSERVER_WORKSPACE')
        host = os.environ.get('GEOSERVER_HOST')
        DB_NAME = os.environ.get('DB_NAME')
        DB_USER = os.environ.get("DB_USER")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        DB_HOST = os.environ.get('DB_GEOSERVER_HOST')
        geo = Geoserver(host, username=username, password=password)
        store_name = f'store_{file_name}'
        geo.create_featurestore(store_name=store_name, workspace=workspace, db=DB_NAME, host=DB_HOST,
                                pg_user=DB_USER,
                                pg_password=DB_PASSWORD)
        geo.publish_featurestore(workspace=workspace, store_name=store_name, pg_table=file_name)

        return file_obj


def getDatabase():
    NAME = os.environ.get('DB_NAME')
    USER = os.environ.get("DB_USER")
    PASSWORD = os.environ.get("DB_PASSWORD")
    HOST = os.environ.get('DB_HOST')
    PORT = os.environ.get("DB_PORT")
    return f'user={USER}  dbname={NAME} host={HOST} port={PORT} password={PASSWORD}'
