# from rest_framework import serializers
# from django.contrib.gis.utils import LayerMapping
# from .models import WorldBorder
#
#
# class FileUploadSerializer(serializers.Serializer):
#     class Meta:
#         fields = ('file',)
#
#
# world_mapping = {
#     "fips": "FIPS",
#     "iso2": "ISO2",
#     "iso3": "ISO3",
#     "un": "UN",
#     "name": "NAME",
#     "area": "AREA",
#     "pop2005": "POP2005",
#     "region": "REGION",
#     "subregion": "SUBREGION",
#     "lon": "LON",
#     "lat": "LAT",
#     "mpoly": "MULTIPOLYGON",
# }
#
#
# def run(path, verbose=True):
#
#     lm = LayerMapping(WorldBorder, path, world_mapping, transform=False)
#     lm.save(strict=True, verbose=verbose)


import datetime
import os
import zipfile

from rest_framework import serializers

from GIS_World import settings


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
    NAME = 'geodjango'
    USER = "geo"
    PASSWORD = "sahan"
    return f'user={USER}  dbname={NAME} host=localhost port=5432 password={PASSWORD}'

