from django.urls import path, include

from api.views import FeaturesApiView, ShapeFileUploadViewSet
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(r'file', ShapeFileUploadViewSet, basename='file')
router.register('layer/(?P<layer_name>[^/.]+)/features', FeaturesApiView, basename='features')
urlpatterns = [
    path('', include(router.urls)),
]
