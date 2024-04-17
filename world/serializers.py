from rest_framework import serializers
from django.contrib.gis.utils import LayerMapping
from .models import WorldBorder


class FileUploadSerializer(serializers.Serializer):
    class Meta:
        fields = ('file',)


world_mapping = {
    "fips": "FIPS",
    "iso2": "ISO2",
    "iso3": "ISO3",
    "un": "UN",
    "name": "NAME",
    "area": "AREA",
    "pop2005": "POP2005",
    "region": "REGION",
    "subregion": "SUBREGION",
    "lon": "LON",
    "lat": "LAT",
    "mpoly": "MULTIPOLYGON",
}


def run(path, verbose=True):

    lm = LayerMapping(WorldBorder, path, world_mapping, transform=False)
    lm.save(strict=True, verbose=verbose)
