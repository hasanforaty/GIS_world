import datetime
import os
import zipfile
import os.environ.getiron

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
        return file_obj


def getDatabase():
    
    NAME = os.environ.get('DB_NAME')
    USER = os.environ.get("DB_USER")
    PASSWORD = os.environ.get("DB_PASSWORD")
    HOST = os.environ.get('DB_HOST')
    PORT = os.environ.get("DB_PORT")
    return f'user={USER}  dbname={NAME} host={HOST} port={PORT} password={PASSWORD}'

