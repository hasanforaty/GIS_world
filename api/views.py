import psycopg2
from django.http import Http404
from rest_framework import status
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

from api.serializers import ShapeFileSerializer, FeatureSerializer
from .features import Features, DoseNotExist


class ShapeFileUploadViewSet(GenericViewSet):
    serializer_class = ShapeFileSerializer

    def create(self, request):
        serializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(status=status.HTTP_400_BAD_REQUEST)


class FeaturesApiView(GenericViewSet):
    serializer_class = FeatureSerializer

    def create(self, request, layer_name):
        try:
            data = request.data
            data['table_name'] = layer_name
            serializer = self.get_serializer(data=data)
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_201_CREATED)
        except ValueError as e:
            print(e)
            return Response(status=422, exception=e)
        except psycopg2.errors.InvalidParameterValue as e:
            return Response(status=400, data="Missmatch :" + str(e))
        return Response(status=status.HTTP_400_BAD_REQUEST)

    def list(self, request, layer_name):
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
        try:
            result = Features(table_name=layer_name).filter(offset=Offset, limit=Limit)

            return Response(status=200, data={
                "page": page,
                "next": next_url,
                "previous": previous_url,
                "results": result
            })
        except Exception as e:
            raise e

    def retrieve(self, request, pk, layer_name):
        try:
            geo_jsons = Features(table_name=layer_name).get(pk=pk)
            return Response(status=200, data=geo_jsons)
        except DoseNotExist as d:
            raise Http404(d)

    def update(self, request, pk, layer_name):
        try:
            data = request.data

            data['table_name'] = layer_name
            data['pk'] = pk
            serializer = self.get_serializer(data, data=data)
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_200_OK)
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
