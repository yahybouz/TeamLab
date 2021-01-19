from django.shortcuts import render

from django.http.response import JsonResponse
from rest_framework.parsers import JSONParser 
from rest_framework import status
 
from unitelegale.models import Unitelegale
from unitelegale.serializers import UnitelegaleSerializer
from rest_framework.decorators import api_view


# Create your views here.
@api_view(['GET'])
def unitelegale_detail(request, siren_id):
    '''
    Get unitelegale information by siren identifier
    '''
    try: 
        unitelegale = Unitelegale.objects.filter().get(pk=siren_id)
        print(unitelegale)
    except Unitelegale.DoesNotExist: 
        return JsonResponse({'message': 'Siren does not exist'}, status=status.HTTP_404_NOT_FOUND) 

    if request.method == 'GET': 
        unitelegale_serializer = UnitelegaleSerializer(unitelegale)
        return JsonResponse(unitelegale_serializer.data)
   



@api_view(['GET'])
def unitelegales_detail(request,siren_id):


    unitelegale = Unitelegale.objects.filter(siren=siren_id)
        
    if request.method == 'GET': 
        unitelegale_serializer = UnitelegaleSerializer(unitelegale, many=True)
        return JsonResponse(unitelegale_serializer.data, safe=False)