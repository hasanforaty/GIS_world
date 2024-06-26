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
        query_set = {}
        query_set.update(request.GET)
        try:

            page = query_set.pop('page', 1)
            Limit = query_set.pop('limit', 1000)
            Offset = (page - 1) * Limit
            if page > 1:
                query = f'?page={page - 1}&limit={Limit}'
                previous_url = url + query
        except KeyError as e:
            pass
        query = f'?page={page + 1}&limit={Limit}'
        next_url = url + query
        try:
            result = Features(table_name=layer_name).filter(offset=Offset, limit=Limit, **query_set)

            return Response(status=200, data={
                "page": page,
                "next": next_url,
                "previous": previous_url,
                "results": result
            })
        except ValueError as e:
            return Response(status=status.HTTP_400_BAD_REQUEST, data=e.args)
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
