from django.conf.urls import url 
from unitelegale import views 
 
urlpatterns = [ 
    url(r'^api/unitelegale/(?P<siren_id>[0-9]+)$', views.unitelegale_detail),
    url(r'^api/unitelegales/(?P<siren_id>[0-9]+)$', views.unitelegales_detail)
]