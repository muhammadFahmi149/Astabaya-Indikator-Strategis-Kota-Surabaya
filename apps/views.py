from multiprocessing import context
from django.shortcuts import get_object_or_404, render, redirect
from django.http import HttpResponse, Http404
from django.conf import settings
from django.urls import reverse
import requests
from urllib.parse import urlparse
import os
import re
from .services.API_service import (
    BPSInfographicService, BPSNewsService, BPSPublicationService, 
    IPMService, HotelOccupancyCombinedService, HotelOccupancyYearlyService, GiniRatioService,
    IPM_UHH_SPService, IPM_HLSService, IPM_RLSService, IPM_PengeluaranPerKapitaService,
    IPM_IndeksKesehatanService, IPM_IndeksHidupLayakService, IPM_IndeksPendidikanService,
    KetenagakerjaanTPTService, KetenagakerjaanTPAKService, KemiskinanSurabayaService,
    KemiskinanJawaTimurService, KependudukanService, PDRBPengeluaranService,
    PDRBLapanganUsahaService, InflasiService
)
from rest_framework.authtoken.models import Token
from django.contrib.auth import authenticate, logout, login as auth_login
from rest_framework.response import Response
from rest_framework import serializers, status, viewsets
from django.core.exceptions import ObjectDoesNotExist
from rest_framework.authtoken.models import Token
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from .serializers import *
from .models import *
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.db.models import Q, Max, Case, When, IntegerField, CharField
from django.db.models.functions import ExtractYear, Length

class NewsViewSet(viewsets.ModelViewSet):
    queryset = News.objects.all()
    serializer_class = NewsSerializer


class InpographicViewSet(viewsets.ModelViewSet):
    queryset = Infographic.objects.all()
    serializer_class = InfographicSerializer

class PublicationViewSet(viewsets.ModelViewSet):
    queryset = Publication.objects.all()
    serializer_class = PublicationSerializer

class HumanDevelopmentIndexViewSet(viewsets.ModelViewSet):
    queryset = HumanDevelopmentIndex.objects.all()
    serializer_class = HumanDevelopmentIndexSerializer

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def add_bookmark(request):
    """
    Menambahkan item ke bookmark pengguna.
    Membutuhkan 'content_type_name' ('news', 'infographic', 'publication') dan 'object_id'.
    """
    serializer = BookmarkSerializer(data=request.data)
    if serializer.is_valid():
        # Memeriksa apakah bookmark sudah ada
        content_type = serializer.validated_data['content_type']
        object_id = serializer.validated_data['object_id']
        if Bookmark.objects.filter(user=request.user, content_type=content_type, object_id=object_id).exists():
            return Response({"error": "Item ini sudah ada di bookmark Anda."}, status=status.HTTP_409_CONFLICT)
        
        # Menyimpan bookmark dengan user yang sedang login
        serializer.save(user=request.user)
        return Response(serializer.data, status=status.HTTP_201_CREATED)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def view_bookmarks(request):
    """
    Menampilkan semua bookmark milik pengguna yang sedang login.
    """
    bookmarks = Bookmark.objects.filter(user=request.user)
    serializer = BookmarkSerializer(bookmarks, many=True, context={'request': request})
    return Response(serializer.data)

@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def delete_bookmark(request, pk):
    bookmark = get_object_or_404(Bookmark, pk=pk, user=request.user)
    bookmark.delete()
    return Response(status=status.HTTP_204_NO_CONTENT)

@api_view(['GET'])
def sync_bps_news(request):
    try:
        created_count, updated_count = BPSNewsService.sync_news()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi berita selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)
        
@api_view(['GET'])
def sync_bps_infographic(request):
    try:
        created_count, updated_count = BPSInfographicService.sync_infographic()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi infografis selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)
        
@api_view(['GET'])
def sync_bps_publication(request):
    try:
        created_count, updated_count = BPSPublicationService.sync_publication()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi publikasi selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_human_development_index(request):
    try:
        created_count, updated_count = IPMService.sync_ipm()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi IPM selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_hotel_occupancy_combined(request):
    try:
        created_count, updated_count = HotelOccupancyCombinedService.sync_hotel_occupancy_combined()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi Tingkat Hunian Hotel (Gabung Semua) selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_hotel_occupancy_yearly(request):
    try:
        created_count, updated_count = HotelOccupancyYearlyService.sync_hotel_occupancy_yearly()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi Tingkat Hunian Hotel (Year-to-Year) selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_gini_ratio(request):
    try:
        created_count, updated_count = GiniRatioService.sync_gini_ratio()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi Gini Ratio selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_ipm_uhh_sp(request):
    try:
        created_count, updated_count = IPM_UHH_SPService.sync_ipm_uhh_sp()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi IPM UHH SP selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_ipm_hls(request):
    try:
        created_count, updated_count = IPM_HLSService.sync_ipm_hls()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi IPM HLS selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_ipm_rls(request):
    try:
        created_count, updated_count = IPM_RLSService.sync_ipm_rls()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi IPM RLS selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_ipm_pengeluaran_per_kapita(request):
    try:
        created_count, updated_count = IPM_PengeluaranPerKapitaService.sync_ipm_pengeluaran_per_kapita()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi IPM Pengeluaran per Kapita selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_ipm_indeks_kesehatan(request):
    try:
        created_count, updated_count = IPM_IndeksKesehatanService.sync_ipm_indeks_kesehatan()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi IPM Indeks Kesehatan selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_ipm_indeks_hidup_layak(request):
    try:
        created_count, updated_count = IPM_IndeksHidupLayakService.sync_ipm_indeks_hidup_layak()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi IPM Indeks Hidup Layak selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_ipm_indeks_pendidikan(request):
    try:
        created_count, updated_count = IPM_IndeksPendidikanService.sync_ipm_indeks_pendidikan()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi IPM Indeks Pendidikan selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)     

@api_view(['GET'])
def sync_kemiskinan_surabaya(request):
    try:
        created_count, updated_count = KemiskinanSurabayaService.sync_kemiskinan_surabaya()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi Kemiskinan Surabaya selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_kemiskinan_jawa_timur(request):
    try:
        created_count, updated_count = KemiskinanJawaTimurService.sync_kemiskinan_jawa_timur()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi Kemiskinan Jawa Timur selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)
        
@api_view(['GET'])
def sync_kependudukan(request):
    try:
        created_count, updated_count = KependudukanService.sync_kependudukan()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi Kependudukan selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)   

@api_view(['GET'])
def sync_ketenagakerjaan_tpt(request):
    try:
        created_count, updated_count = KetenagakerjaanTPTService.sync_ketenagakerjaan_tpt()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi Ketenagakerjaan TPT selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_ketenagakerjaan_tpak(request):
    try:
        created_count, updated_count = KetenagakerjaanTPAKService.sync_ketenagakerjaan_tpak()
        return Response({
            "status": "success",
            "message": f"Sinkronisasi Ketenagakerjaan TPAK selesai. Data baru: {created_count}, data diperbarui: {updated_count}.",
            "details": {"created": created_count, "updated": updated_count}
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)
        
@api_view(['GET'])
def sync_pdrb_pengeluaran(request):
    try:
        results = PDRBPengeluaranService.sync_all_pdrb_pengeluaran()
        
        # Calculate totals
        total_created = sum(sheet_result['created'] for sheet_result in results.values())
        total_updated = sum(sheet_result['updated'] for sheet_result in results.values())
        
        return Response({
            "status": "success",
            "message": f"Sinkronisasi PDRB Pengeluaran selesai. Total data baru: {total_created}, total data diperbarui: {total_updated}.",
            "details": {
                "total_created": total_created,
                "total_updated": total_updated,
                "sheets": results
            }
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

@api_view(['GET'])
def sync_pdrb_lapangan_usaha(request):
    try:
        results = PDRBLapanganUsahaService.sync_all_pdrb_lapangan_usaha()
        
        # Calculate totals
        total_created = sum(sheet_result['created'] for sheet_result in results.values())
        total_updated = sum(sheet_result['updated'] for sheet_result in results.values())
        
        return Response({
            "status": "success",
            "message": f"Sinkronisasi PDRB Lapangan Usaha selesai. Total data baru: {total_created}, total data diperbarui: {total_updated}.",
            "details": {
                "total_created": total_created,
                "total_updated": total_updated,
                "sheets": results
            }
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)     

@api_view(['GET'])
def sync_inflasi(request):
    try:
        results = InflasiService.sync_all_inflasi()
        
        # Calculate totals
        total_created = sum(sheet_result['created'] for sheet_result in results.values())
        total_updated = sum(sheet_result['updated'] for sheet_result in results.values())
        
        return Response({
            "status": "success",
            "message": f"Sinkronisasi Inflasi selesai. Total data baru: {total_created}, total data diperbarui: {total_updated}.",
            "details": {
                "total_created": total_created,
                "total_updated": total_updated,
                "sheets": results
            }
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

def get_month_order():
    """Helper function untuk mendapatkan urutan bulan secara kronologis."""
    return Case(
        When(month='JANUARI', then=1),
        When(month='FEBRUARI', then=2),
        When(month='MARET', then=3),
        When(month='APRIL', then=4),
        When(month='MEI', then=5),
        When(month='JUNI', then=6),
        When(month='JULI', then=7),
        When(month='AGUSTUS', then=8),
        When(month='SEPTEMBER', then=9),
        When(month='OKTOBER', then=10),
        When(month='NOPEMBER', then=11),
        When(month='DESEMBER', then=12),
        default=0,
        output_field=IntegerField()
    )

@api_view(['GET'])
def get_inflasi_data(request):
    """API endpoint untuk mendapatkan data inflasi umum."""
    try:
        year = request.query_params.get('year', None)
        month = request.query_params.get('month', None)
        
        queryset = Inflasi.objects.all()
        
        if year:
            try:
                queryset = queryset.filter(year=int(year))
            except ValueError:
                return Response({
                    "status": "error",
                    "message": "Invalid year parameter"
                }, status=400)
        
        if month:
            queryset = queryset.filter(month=month)
        
        # Sort by year and month (chronologically)
        queryset = queryset.annotate(month_order=get_month_order()).order_by('year', 'month_order')
        
        serializer = InflasiSerializer(queryset, many=True)
        return Response({
            "status": "success",
            "data": serializer.data,
            "count": len(serializer.data)
        })
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Error in get_inflasi_data: {str(e)}")
        print(f"Traceback: {error_trace}")
        return Response({
            "status": "error",
            "message": str(e),
            "traceback": error_trace
        }, status=500)

@api_view(['GET'])
def get_inflasi_perkomoditas_data(request):
    """API endpoint untuk mendapatkan data inflasi per komoditas."""
    try:
        commodity_code = request.query_params.get('commodity_code', None)
        commodity_name = request.query_params.get('commodity_name', None)
        year = request.query_params.get('year', None)
        month = request.query_params.get('month', None)
        flag = request.query_params.get('flag', None)
        parent_code = request.query_params.get('parent_code', None)
        
        queryset = InflasiPerKomoditas.objects.all()
        
        if commodity_code:
            queryset = queryset.filter(commodity_code=commodity_code)
        
        if commodity_name:
            queryset = queryset.filter(commodity_name__icontains=commodity_name)
        
        if year:
            try:
                queryset = queryset.filter(year=int(year))
            except ValueError:
                return Response({
                    "status": "error",
                    "message": "Invalid year parameter"
                }, status=400)
        
        if month:
            queryset = queryset.filter(month=month)
        
        if flag:
            queryset = queryset.filter(flag=flag)
        
        if parent_code:
            # Filter by parent code (for flag 2 and 3, parent is the code prefix)
            queryset = queryset.filter(commodity_code__startswith=parent_code)
        
        # Sort by year, month (chronologically), and commodity_code
        queryset = queryset.annotate(month_order=get_month_order()).order_by('year', 'month_order', 'commodity_code')
        
        serializer = InflasiPerKomoditasSerializer(queryset, many=True)
        return Response({
            "status": "success",
            "data": serializer.data,
            "count": len(serializer.data)
        })
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Error in get_inflasi_perkomoditas_data: {str(e)}")
        print(f"Traceback: {error_trace}")
        return Response({
            "status": "error",
            "message": str(e),
            "traceback": error_trace
        }, status=500)

@api_view(['GET'])
def get_komoditas_by_flag(request):
    """API endpoint untuk mendapatkan komoditas berdasarkan flag dan parent."""
    try:
        flag = request.query_params.get('flag', None)
        year = request.query_params.get('year', None)
        parent_code = request.query_params.get('parent_code', None)
        
        queryset = InflasiPerKomoditas.objects.all()
        
        if flag:
            queryset = queryset.filter(flag=flag)
        
        if year:
            queryset = queryset.filter(year=int(year))
        
        if parent_code:
            # Hierarchical filtering based on commodity code structure:
            # - Flag 1 (Komoditas Umum): codes 1-11 (1-2 digits)
            # - Flag 2 (Sub Komoditas): codes that start with Flag 1 code + 1 digit (length = parent_length + 1)
            # - Flag 3 (Komoditas Spesifik): codes 6+ digits that start with Flag 2 code
            #
            # IMPORTANT: Same code can exist with different flags!
            # Example: Code "11" can be Flag 1 (PERAWATAN PRIBADI) or Flag 2 (MAKANAN)
            # So we MUST filter by flag first, then by parent_code prefix
            #
            # Examples:
            # - Parent "1" (Flag 1) -> Sub "11", "12", "14" (Flag 2, 2 digits)
            # - Parent "10" (Flag 1) -> Sub "101" (Flag 2, 3 digits)
            # - Parent "11" (Flag 1) -> Sub "111", "112", "113", "119" (Flag 2, 3 digits)
            # - Parent "11" (Flag 2) -> Spesifik "112001" (Flag 3, 6 digits)
            # - Parent "111" (Flag 2) -> Spesifik "1112001" (Flag 3, 6+ digits)
            
            parent_length = len(parent_code)
            
            if flag == '2':
                # Flag 2 (Sub Komoditas): length = parent_length + 1
                # Must start with parent_code, have exact expected_length, and flag='2'
                # Example: parent "1" (Flag 1) -> children "11", "12", "14" (all Flag 2, length 2)
                # Note: "11" with Flag 2 is "MAKANAN", not "11" with Flag 1 which is "PERAWATAN PRIBADI"
                expected_length = parent_length + 1
                
                # Filter by flag first (already done above), then by length and prefix
                # This ensures we get "11" with Flag 2, not "11" with Flag 1
                queryset = queryset.annotate(
                    code_length=Length('commodity_code')
                ).filter(
                    code_length=expected_length,
                    commodity_code__startswith=parent_code
                ).exclude(
                    commodity_code=parent_code
                )
                
            elif flag == '3':
                # Flag 3 (Komoditas Spesifik): 6 or more digits
                # Must start with parent_code (Flag 2 code) and have 6+ digits
                queryset = queryset.annotate(
                    code_length=Length('commodity_code')
                ).filter(
                    code_length__gte=6,  # At least 6 digits
                    commodity_code__startswith=parent_code
                ).exclude(
                    commodity_code=parent_code
                )
            else:
                # Default behavior for other flags
                queryset = queryset.filter(commodity_code__startswith=parent_code).exclude(commodity_code=parent_code)
        
        # Get unique commodities - sort by commodity_code to maintain hierarchy order
        commodities = queryset.values('commodity_code', 'commodity_name', 'flag').distinct().order_by('commodity_code')
        
        # Debug logging
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"get_komoditas_by_flag: flag={flag}, year={year}, parent_code={parent_code}, count={len(commodities)}")
        if parent_code and flag == '2':
            logger.info(f"Looking for Flag 2 children of parent_code='{parent_code}': {list(commodities)}")
            # Also print to console for immediate debugging
            print(f"[DEBUG] get_komoditas_by_flag: flag={flag}, year={year}, parent_code={parent_code}")
            print(f"[DEBUG] Found {len(commodities)} commodities: {list(commodities)}")
        
        return Response({
            "status": "success",
            "data": list(commodities),
            "count": len(commodities)
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)     
        
# --- Views to render HTML pages ---
def signup_page(request):
    """Renders the signup page."""
    return render(request, 'accounts/signup.html')

def login_page(request):
    """Renders the login page or handles form-based login."""
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        
        user = None
        if '@' in username:
            try:
                user = User.objects.get(email=username)
            except ObjectDoesNotExist:
                pass
        
        if not user:
            user = authenticate(request, username=username, password=password)
        
        if user:
            # Session-based login - specify backend for multiple backends
            auth_login(request, user, backend='django.contrib.auth.backends.ModelBackend')
            # Mark session as modified to ensure it's saved
            request.session.modified = True
            request.session.save()
            
            print(f"Form login - Session key: {request.session.session_key}")
            print(f"Form login - User authenticated: {request.user.is_authenticated}")
            print(f"Form login - User: {user.username} ({user.email})")
            print(f"Form login - Session data keys: {list(request.session.keys())}")
            
            # Create response with redirect
            response = redirect('dashboard')
            
            # Ensure session cookie is set in response
            if request.session.session_key:
                cookie_path = getattr(settings, 'SESSION_COOKIE_PATH', '/')
                cookie_domain = getattr(settings, 'SESSION_COOKIE_DOMAIN', None)
                response.set_cookie(
                    settings.SESSION_COOKIE_NAME,
                    request.session.session_key,
                    max_age=settings.SESSION_COOKIE_AGE,
                    path=cookie_path,
                    domain=cookie_domain,
                    secure=settings.SESSION_COOKIE_SECURE,
                    httponly=settings.SESSION_COOKIE_HTTPONLY,
                    samesite=settings.SESSION_COOKIE_SAMESITE
                )
                print(f"Form login - Set session cookie: {settings.SESSION_COOKIE_NAME}")
            
            return response
        else:
            # Return to login page with error
            return render(request, 'accounts/login.html', {
                'error': 'Invalid username or password'
            })
    
    # GET request - render login page
    return render(request, 'accounts/login.html')

# --- API Endpoints for Authentication ---
@api_view(['POST'])
def register_user(request):
    serializer = UserSerializer(data=request.data)
    if serializer.is_valid():
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

def user_login_form(request):
    """Handle form-based login (for session-based auth)."""
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')

        user = None
        if '@' in username:
            try:
                user = User.objects.get(email=username)
            except ObjectDoesNotExist:
                pass

        if not user:
            user = authenticate(request, username=username, password=password)

        if user:
            # Session-based login
            auth_login(request, user, backend='django.contrib.auth.backends.ModelBackend')
            # Mark session as modified to ensure it's saved
            request.session.modified = True
            
            # Verify session is saved
            print(f"Manual login - Session key: {request.session.session_key}")
            print(f"Manual login - User authenticated: {request.user.is_authenticated}")
            print(f"Manual login - User: {user.username} ({user.email})")
            
            # Redirect to dashboard
            return redirect('dashboard')
        else:
            # Return to login page with error
            return render(request, 'accounts/login.html', {
                'error': 'Invalid username or password'
            })
    
    # GET request - redirect to login page
    return redirect('login')

@api_view(['POST'])
def user_login(request):
    """API endpoint for login (kept for backward compatibility, but prefer user_login_form)."""
    username = request.data.get('username')
    password = request.data.get('password')

    user = None
    if '@' in username:
        try:
            user = User.objects.get(email=username)
        except ObjectDoesNotExist:
            pass

    if not user:
        user = authenticate(request, username=username, password=password)

    if user:
        # Session-based login
        auth_login(request, user)
        # Mark session as modified to ensure it's saved
        request.session.modified = True
        
        # Verify session is saved
        print(f"API Manual login - Session key: {request.session.session_key}")
        print(f"API Manual login - User authenticated: {request.user.is_authenticated}")
        print(f"API Manual login - User: {user.username} ({user.email})")
        
        token, _ = Token.objects.get_or_create(user=user)
        return Response({'token': token.key, 'redirect': '/dashboard/'}, status=status.HTTP_200_OK)
    return Response({'error': 'Invalid credentials'}, status=status.HTTP_401_UNAUTHORIZED)

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def user_logout(request):
    if request.method == 'POST':
        try:
            # Delete the user's token to logout
            request.user.auth_token.delete()
            return Response({'message': 'Successfully logged out.'}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return redirect('login')

def logout_view(request):
    """
    Logout view that clears both token and session.
    Accepts POST, GET and other methods and redirects to home.
    """
    # Delete token if the user has one
    try:
        if request.user.is_authenticated and hasattr(request.user, 'auth_token'):
            request.user.auth_token.delete()
    except Exception:
        pass
    
    # Clear Django session
    logout(request)
    
    # Redirect to dashboard
    return redirect('dashboard')

def google_login_redirect(request):
    """Redirect to Google OAuth login."""
    from allauth.socialaccount.providers.google.views import oauth2_login
    from django.shortcuts import redirect
    # Use allauth's built-in Google login
    return oauth2_login(request)

def google_oauth_callback(request):
    """Handle Google OAuth callback after redirect."""
    import requests
    from django.http import JsonResponse, HttpResponse
    import traceback
    
    code = request.GET.get('code')
    error = request.GET.get('error')
    
    if error:
        # If user denied access, redirect to dashboard
        return redirect('dashboard')
    
    if not code:
        # No code provided, redirect to dashboard
        return redirect('dashboard')
    
    # Exchange code for token
    google_client_id = os.getenv('GOOGLE_CLIENT_ID', '')
    google_client_secret = os.getenv('GOOGLE_CLIENT_SECRET', '')
    
    # Build redirect URI - use the same one used in the authorization request
    if 'localhost' in request.get_host():
        redirect_uri = 'http://localhost:8000/accounts/google/login/callback/'
    else:
        redirect_uri = 'http://127.0.0.1:8000/accounts/google/login/callback/'
    
    token_url = 'https://oauth2.googleapis.com/token'
    token_data = {
        'code': code,
        'client_id': google_client_id,
        'client_secret': google_client_secret,
        'redirect_uri': redirect_uri,
        'grant_type': 'authorization_code'
    }
    
    try:
        token_response = requests.post(token_url, data=token_data, timeout=10)
        token_response.raise_for_status()
        token_json = token_response.json()
        access_token = token_json.get('access_token')
        id_token = token_json.get('id_token')
        
        if not id_token:
            return redirect('dashboard')
        
        # Decode ID token to get user info
        import jwt
        decoded = jwt.decode(id_token, options={"verify_signature": False})
        
        email = decoded.get('email')
        given_name = decoded.get('given_name', '')
        family_name = decoded.get('family_name', '')
        name = decoded.get('name', '')
        
        if not email:
            return redirect('dashboard')
        
        # Get or create user - check by email first, then by username
        try:
            user = User.objects.get(email=email)
        except User.DoesNotExist:
            # Create new user
            username = email.split('@')[0] if '@' in email else email
            # Make sure username is unique
            base_username = username
            counter = 1
            while User.objects.filter(username=username).exists():
                username = f"{base_username}{counter}"
                counter += 1
            
            user = User.objects.create_user(
                username=username,
                email=email,
                first_name=given_name,
                last_name=family_name,
            )
            user.save()
            print(f"Created new user: {user.username} ({user.email}) - ID: {user.id}")
        else:
            # User exists, update info if needed
            if given_name and not user.first_name:
                user.first_name = given_name
            if family_name and not user.last_name:
                user.last_name = family_name
            user.save()
            print(f"Logged in existing user: {user.username} ({user.email}) - ID: {user.id}")
        
        # Login user with session - specify backend for multiple backends
        auth_login(request, user, backend='django.contrib.auth.backends.ModelBackend')
        
        # Mark session as modified to ensure it's saved
        request.session.modified = True
        
        # Force save session
        request.session.save()
        
        # Verify session is saved
        print(f"Google OAuth - Session key: {request.session.session_key}")
        print(f"Google OAuth - User authenticated: {request.user.is_authenticated}")
        print(f"Google OAuth - User ID in session: {request.user.id}")
        print(f"Google OAuth - Session data: {dict(request.session)}")
        
        # Create response with redirect
        response = redirect('dashboard')
        
        # Ensure session cookie is set
        if request.session.session_key:
            cookie_path = getattr(settings, 'SESSION_COOKIE_PATH', '/')
            cookie_domain = getattr(settings, 'SESSION_COOKIE_DOMAIN', None)
            response.set_cookie(
                settings.SESSION_COOKIE_NAME,
                request.session.session_key,
                max_age=settings.SESSION_COOKIE_AGE,
                path=cookie_path,
                domain=cookie_domain,
                secure=settings.SESSION_COOKIE_SECURE,
                httponly=settings.SESSION_COOKIE_HTTPONLY,
                samesite=settings.SESSION_COOKIE_SAMESITE
            )
            print(f"Google OAuth - Set session cookie: {settings.SESSION_COOKIE_NAME}")
        
        return response
        
    except requests.RequestException as e:
        print(f"Request error in Google OAuth callback: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return redirect('dashboard')
    except Exception as e:
        print(f"Error in Google OAuth callback: {str(e)}")
        print(traceback.format_exc())
        return redirect('dashboard')

@api_view(['POST'])
def google_signin_callback(request):
    """Handle Google Sign-In callback from JavaScript API."""
    import json
    import base64
    from django.http import JsonResponse
    
    try:
        credential = request.data.get('credential')
        if not credential:
            return JsonResponse({'success': False, 'error': 'No credential provided'}, status=400)
        
        # Decode JWT token (without verification for now, Google will verify on their end)
        # In production, you should verify the token signature
        import jwt
        from jwt import DecodeError
        
        # Get Google client ID from settings
        google_client_id = os.getenv('GOOGLE_CLIENT_ID', '')
        
        try:
            # Decode without verification first to get payload
            decoded = jwt.decode(credential, options={"verify_signature": False})
            
            # Verify it's from Google and for our client
            if decoded.get('aud') != google_client_id:
                return JsonResponse({'success': False, 'error': 'Invalid client ID'}, status=400)
            
            # Extract user info
            email = decoded.get('email')
            name = decoded.get('name', '')
            given_name = decoded.get('given_name', '')
            family_name = decoded.get('family_name', '')
            picture = decoded.get('picture', '')
            
            if not email:
                return JsonResponse({'success': False, 'error': 'No email in token'}, status=400)
            
            # Get or create user
            user, created = User.objects.get_or_create(
                email=email,
                defaults={
                    'username': email.split('@')[0] if '@' in email else email,
                    'first_name': given_name,
                    'last_name': family_name,
                }
            )
            
            # If user exists but name is empty, update it
            if not created and (not user.first_name or not user.last_name):
                if given_name:
                    user.first_name = given_name
                if family_name:
                    user.last_name = family_name
                user.save()
            
            # Login user
            auth_login(request, user, backend='django.contrib.auth.backends.ModelBackend')
            
            return JsonResponse({
                'success': True,
                'user': {
                    'email': user.email,
                    'username': user.username,
                    'name': user.get_full_name() or name
                }
            })
            
        except DecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid token'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
            
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@api_view(['GET'])
def ApiOverview(request):
    api_urls = {
        'all_items' : '/',
        'Search by Category' : '/?category=category_name',
        'Search by Subcategory' : '/?subcategory=category_name',
        'Add': '/create',
        'Update': '/update/pk',
        'Delete': '/item/pk/delete'
    }
    return Response(api_urls)


@api_view(['POST'])
def add_data (request):
    data = DataSerializers(data=request.data)
    if Data.objects.filter(**request.data).exists():
        raise serializers.ValidationError('This data already exists')
    if data.is_valid():
        data.save()
        return Response(data.data)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)

@api_view(['GET'])
def view_data (request):
    if request.query_params:
        data = Data.objects.filter(**request.query_params.dict())
    else:
        data = Data.objects.all()
    if data:
        serializer = DataSerializers(data, many=True)
        return Response(serializer.data)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)

@api_view(['POST'])
def update_data (request, pk):
    datas = Data.objects.get(pk=pk)
    data = DataSerializers(instance=datas, data=request.data)
    if data.is_valid():
        data.save()
        return Response(data.data)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)

@api_view(['DELETE'])
def delete_data (request, pk):
    data = get_object_or_404(Data, pk=pk)
    data.delete()
    return Response(status=status.HTTP_202_ACCEPTED)
    
def apps(request):
    # Fetch data directly from the service functions
    dataNews = News.objects.order_by('-release_date', '-news_id')[:5]
    dataInpographic = Infographic.objects.all()
    dataPublication = Publication.objects.all()

    context = {
        'dataNews': dataNews,
        'dataInpographic': dataInpographic,
        'dataPublication': dataPublication,
    }
    return render(request, 'index.html', context)

def contact_us(request):
    if request.method == 'POST':
        form = ContactForm(request.POST)
        if form.is_valid():
            try:
                name = form.cleaned_data['name']
                surname = form.cleaned_data['surname']
                email = form.cleaned_data['email']
                message = form.cleaned_data['message']
                
                subject = f"Pesan Baru dari {name} {surname} melalui Aastabaya"
                email_message = (
                    f"Anda menerima pesan baru dari formulir kontak Aastabaya:\n\n"
                    f"Nama: {name} {surname}\n"
                    f"Email: {email}\n\n"
                    f"Pesan:\n{message}"
                )

                send_mail(
                    subject,
                    email_message,
                    settings.EMAIL_HOST_USER,  # Alamat pengirim
                    [settings.EMAIL_HOST_USER], # Alamat penerima
                )
                messages.success(request, 'Pesan Anda telah berhasil terkirim!')
            except Exception as e:
                messages.error(request, 'Terjadi kesalahan saat mengirim pesan. Silakan coba lagi.')
                print(f"Error sending email: {e}") # Log error ke konsol
        else:
            # Jika formulir tidak valid, beri tahu pengguna
            messages.warning(request, 'Harap perbaiki kesalahan di bawah ini dan kirimkan formulir lagi.')
            # Di masa mendatang, Anda bisa merender ulang formulir dengan error
            
    return redirect('index')

def dashboard(request):
    # Debug: Check if user is authenticated
    print(f"Dashboard view - User authenticated: {request.user.is_authenticated}")
    if request.user.is_authenticated:
        print(f"Dashboard view - User: {request.user.username} ({request.user.email}) - ID: {request.user.id}")
    else:
        print("Dashboard view - User is NOT authenticated")
    
    # Fetch data directly from the service functions
    dataNewss = News.objects.order_by('-release_date')
    countNews = News.objects.count()
    dataNews = News.objects.order_by('-release_date', '-news_id')[:5]
    dataNewsLatest = dataNews
    dataInfographic = Infographic.objects.order_by('-id')[:5]
    dataInfographics = Infographic.objects.order_by('-id')
    dataInfographicLatest = Infographic.objects.order_by('-id')[:4]
    dataPublication = Publication.objects.order_by('-date')[:5]
    dataPublications = Publication.objects.order_by('-date')
    dataPublicationsLatest = Publication.objects.order_by('-date')[:5]

    # Add bookmark_id to dataNewsLatest, dataInfographicLatest, and dataPublicationsLatest if user is authenticated
    if request.user.is_authenticated:
        from django.contrib.contenttypes.models import ContentType
        
        # Add bookmark_id to news
        news_ct = ContentType.objects.get_for_model(News)
        news_bookmarks = Bookmark.objects.filter(
            user=request.user,
            content_type=news_ct
        ).values_list('object_id', 'id')
        news_bookmark_dict = {str(obj_id): bookmark_id for obj_id, bookmark_id in news_bookmarks}
        for news in dataNewsLatest:
            news.bookmark_id = news_bookmark_dict.get(str(news.news_id), None)
        
        # Add bookmark_id to infographics
        infographic_ct = ContentType.objects.get_for_model(Infographic)
        infographic_bookmarks = Bookmark.objects.filter(
            user=request.user,
            content_type=infographic_ct
        ).values_list('object_id', 'id')
        infographic_bookmark_dict = {str(obj_id): bookmark_id for obj_id, bookmark_id in infographic_bookmarks}
        for infographic in dataInfographicLatest:
            infographic.bookmark_id = infographic_bookmark_dict.get(str(infographic.id), None)
        
        # Add bookmark_id to publications
        publication_ct = ContentType.objects.get_for_model(Publication)
        publication_bookmarks = Bookmark.objects.filter(
            user=request.user,
            content_type=publication_ct
        ).values_list('object_id', 'id')
        publication_bookmark_dict = {str(obj_id): bookmark_id for obj_id, bookmark_id in publication_bookmarks}
        for publication in dataPublicationsLatest:
            publication.bookmark_id = publication_bookmark_dict.get(str(publication.pk), None)

    # --- Bookmark ---
    bookmarked_items = []
    if request.user.is_authenticated:
        # Ambil semua bookmark milik pengguna, dengan prefetch ke content_object
        user_bookmarks = Bookmark.objects.filter(user=request.user).select_related('content_type')

        for bookmark in user_bookmarks:
            item = bookmark.content_object
            if item:
                item_url = '#' # URL default jika tidak ditemukan
                icon_class = 'bi bi-bookmark-fill'  # Default icon
                content_type_label = ''  # Label untuk menampilkan asal bookmark
                
                # Tentukan URL, icon, dan label berdasarkan tipe model
                if isinstance(item, News):
                    # Arahkan ke halaman daftar berita dengan anchor ke ID item
                    item_url = reverse('news') + f'#news-{item.pk}'
                    icon_class = 'bi bi-file-earmark-text'  # Icon berita dari sidebar
                    content_type_label = 'Berita'
                elif isinstance(item, Infographic):
                    item_url = reverse('infographics') + f'#infographic-{item.pk}'
                    icon_class = 'bi bi-bar-chart-line'  # Icon infografis dari sidebar
                    content_type_label = 'Infografis'
                elif isinstance(item, Publication):
                    item_url = reverse('publications') + f'#publication-{item.pk}'
                    icon_class = 'icon-book'  # Icon publikasi dari sidebar
                    content_type_label = 'Publikasi'
                
                # Format: "Judul (Asal)"
                formatted_title = f"{item.title} ({content_type_label})" if content_type_label else item.title
                
                bookmarked_items.append({
                    'title': formatted_title,
                    'url': item_url,
                    'icon_class': icon_class,
                })

    # Helper function to get latest sub-category IPM data for carousel
    def get_latest_subcategory_data(model_class, location_name_keywords):
        """Helper function to get latest data for a sub-category"""
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            location_upper = data.location_name.upper().strip()
            if any(keyword in location_upper for keyword in location_name_keywords):
                filtered_data.append(data)
        if filtered_data:
            filtered_data.sort(key=lambda x: x.year)
            return filtered_data[-1] if filtered_data else None
        return None
    
    def get_previous_subcategory_data(model_class, location_name_keywords, current_year):
        """Helper function to get previous year data for calculating change"""
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            location_upper = data.location_name.upper().strip()
            if any(keyword in location_upper for keyword in location_name_keywords):
                filtered_data.append(data)
        if filtered_data:
            previous_data = [d for d in filtered_data if d.year == current_year - 1]
            return previous_data[0] if previous_data else None
        return None
    
    # Helper function to get latest data for PDRB models (looking for "TOTAL" or "JUMLAH" category)
    def get_latest_pdrb_data(model_class, category_keywords=['TOTAL', 'JUMLAH']):
        """Helper function to get latest data for PDRB models by category"""
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            category_upper = data.expenditure_category.upper().strip() if hasattr(data, 'expenditure_category') else data.industry_category.upper().strip()
            if any(keyword in category_upper for keyword in category_keywords):
                filtered_data.append(data)
        if filtered_data:
            # Sort by year, then by quarter (if exists) - quarters: I, II, III, IV
            quarter_order = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'TOTAL': 5}
            if hasattr(filtered_data[0], 'quarter'):
                filtered_data.sort(key=lambda x: (x.year, quarter_order.get(getattr(x, 'quarter', ''), 0)))
            else:
                filtered_data.sort(key=lambda x: x.year)
            return filtered_data[-1] if filtered_data else None
        return None
    
    def get_previous_pdrb_data(model_class, current_year, current_quarter=None, category_keywords=['TOTAL', 'JUMLAH']):
        """Helper function to get previous period data for PDRB models"""
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            category_upper = data.expenditure_category.upper().strip() if hasattr(data, 'expenditure_category') else data.industry_category.upper().strip()
            if any(keyword in category_upper for keyword in category_keywords):
                filtered_data.append(data)
        if filtered_data:
            if current_quarter:
                # For quarterly data, get previous quarter
                quarters = ['I', 'II', 'III', 'IV']
                if current_quarter in quarters:
                    prev_idx = quarters.index(current_quarter) - 1
                    if prev_idx >= 0:
                        prev_quarter = quarters[prev_idx]
                        prev_year = current_year
                    else:
                        prev_quarter = 'IV'
                        prev_year = current_year - 1
                    previous_data = [d for d in filtered_data if d.year == prev_year and d.quarter == prev_quarter]
                else:
                    # For annual comparison
                    previous_data = [d for d in filtered_data if d.year == current_year - 1]
            else:
                # Annual data
                previous_data = [d for d in filtered_data if d.year == current_year - 1]
            return previous_data[0] if previous_data else None
        return None
    
    # Get IPM sub-category data for carousel (Kota Surabaya)
    ipm_subcategories = []
    subcategory_models = [
        ('UHH SP', IPM_UHH_SP),
        ('HLS', IPM_HLS),
        ('RLS', IPM_RLS),
        ('Pengeluaran per Kapita', IPM_PengeluaranPerKapita),
        ('Indeks Kesehatan', IPM_IndeksKesehatan),
        ('Indeks Hidup Layak', IPM_IndeksHidupLayak),
        ('Indeks Pendidikan', IPM_IndeksPendidikan),
    ]
    
    for name, model_class in subcategory_models:
        latest = get_latest_subcategory_data(model_class, ['SURABAYA'])
        if latest:
            previous = get_previous_subcategory_data(model_class, ['SURABAYA'], latest.year)
            change = None
            change_percent = None
            if previous and latest.value and previous.value:
                change = float(latest.value) - float(previous.value)
                change_percent = (change / float(previous.value)) * 100 if float(previous.value) != 0 else 0
            
            ipm_subcategories.append({
                'name': name,
                'value': latest.value,
                'year': latest.year,
                'change': change,
                'change_percent': change_percent,
            })
    
    # Get latest IPM main data (Kota Surabaya)
    surabaya_keywords = ['KOTA SURABAYA', 'SURABAYA']
    all_ipm_data = list(HumanDevelopmentIndex.objects.all())
    surabaya_ipm_data = []
    for data in all_ipm_data:
        location_upper = data.location_name.upper().strip()
        if 'SURABAYA' in location_upper:
            if 'KOTA' in location_upper or location_upper.startswith('SURABAYA'):
                surabaya_ipm_data.append(data)
    
    latest_ipm = None
    previous_ipm = None
    ipm_change = None
    ipm_change_percent = None
    if surabaya_ipm_data:
        surabaya_ipm_data.sort(key=lambda x: x.year)
        latest_ipm = surabaya_ipm_data[-1]
        if len(surabaya_ipm_data) > 1:
            previous_ipm = surabaya_ipm_data[-2]
            if latest_ipm.ipm_value and previous_ipm.ipm_value:
                ipm_change = float(latest_ipm.ipm_value) - float(previous_ipm.ipm_value)
                ipm_change_percent = (ipm_change / float(previous_ipm.ipm_value)) * 100 if float(previous_ipm.ipm_value) != 0 else 0
    
    # Get latest Gini Ratio (Kota Surabaya)
    all_gini_data = list(GiniRatio.objects.all())
    surabaya_gini_data = []
    for data in all_gini_data:
        location_upper = data.location_name.upper().strip()
        if 'SURABAYA' in location_upper:
            if 'KOTA' in location_upper or location_upper.startswith('SURABAYA'):
                surabaya_gini_data.append(data)
    
    latest_gini = None
    previous_gini = None
    gini_change = None
    gini_change_percent = None
    if surabaya_gini_data:
        surabaya_gini_data.sort(key=lambda x: x.year)
        latest_gini = surabaya_gini_data[-1]
        if len(surabaya_gini_data) > 1:
            previous_gini = surabaya_gini_data[-2]
            if latest_gini.gini_ratio_value and previous_gini.gini_ratio_value:
                gini_change = float(latest_gini.gini_ratio_value) - float(previous_gini.gini_ratio_value)
                gini_change_percent = (gini_change / float(previous_gini.gini_ratio_value)) * 100 if float(previous_gini.gini_ratio_value) != 0 else 0
    
    # Get latest Hotel Occupancy Yearly data
    all_hotel_yearly = list(HotelOccupancyYearly.objects.all().order_by('-year'))
    latest_hotel_yearly = all_hotel_yearly[0] if all_hotel_yearly else None
    
    # Helper function to get latest data for PDRB models (looking for "TOTAL" or "JUMLAH" category)
    def get_latest_pdrb_data(model_class, category_keywords=['TOTAL', 'JUMLAH']):
        """Helper function to get latest data for PDRB models by category"""
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            category_upper = data.expenditure_category.upper().strip() if hasattr(data, 'expenditure_category') else data.industry_category.upper().strip()
            if any(keyword in category_upper for keyword in category_keywords):
                filtered_data.append(data)
        if filtered_data:
            # Sort by year, then by quarter (if exists) - quarters: I, II, III, IV
            quarter_order = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'TOTAL': 5}
            if hasattr(filtered_data[0], 'quarter'):
                filtered_data.sort(key=lambda x: (x.year, quarter_order.get(getattr(x, 'quarter', ''), 0)))
            else:
                filtered_data.sort(key=lambda x: x.year)
            return filtered_data[-1] if filtered_data else None
        return None
    
    def get_previous_pdrb_data(model_class, current_year, current_quarter=None, category_keywords=['TOTAL', 'JUMLAH']):
        """Helper function to get previous period data for PDRB models"""
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            category_upper = data.expenditure_category.upper().strip() if hasattr(data, 'expenditure_category') else data.industry_category.upper().strip()
            if any(keyword in category_upper for keyword in category_keywords):
                filtered_data.append(data)
        if filtered_data:
            if current_quarter:
                # For quarterly data, get previous quarter
                quarters = ['I', 'II', 'III', 'IV']
                if current_quarter in quarters:
                    prev_idx = quarters.index(current_quarter) - 1
                    if prev_idx >= 0:
                        prev_quarter = quarters[prev_idx]
                        prev_year = current_year
                    else:
                        prev_quarter = 'IV'
                        prev_year = current_year - 1
                    previous_data = [d for d in filtered_data if d.year == prev_year and d.quarter == prev_quarter]
                else:
                    # For annual comparison
                    previous_data = [d for d in filtered_data if d.year == current_year - 1]
            else:
                # Annual data
                previous_data = [d for d in filtered_data if d.year == current_year - 1]
            return previous_data[0] if previous_data else None
        return None
    
    # Collect all indicator data for carousel
    indicator_carousel_data = []
    
    # Month order mapping for Inflasi
    month_order = ['JANUARI', 'FEBRUARI', 'MARET', 'APRIL', 'MEI', 'JUNI', 
                   'JULI', 'AGUSTUS', 'SEPTEMBER', 'OKTOBER', 'NOPEMBER', 'DESEMBER']
    
    # 1. Inflasi M-to-M (Month to Month)
    latest_inflasi_mtm = Inflasi.objects.order_by('-year', '-month').first()
    if latest_inflasi_mtm and latest_inflasi_mtm.bulanan:
        # Get previous month data
        if latest_inflasi_mtm.month in month_order:
            current_month_idx = month_order.index(latest_inflasi_mtm.month)
            if current_month_idx > 0:
                # Previous month in same year
                prev_month = month_order[current_month_idx - 1]
                previous_inflasi_mtm = Inflasi.objects.filter(year=latest_inflasi_mtm.year, month=prev_month).first()
            else:
                # Previous month is December of previous year (current month is January)
                previous_inflasi_mtm = Inflasi.objects.filter(year=latest_inflasi_mtm.year - 1, month='DESEMBER').first()
        else:
            previous_inflasi_mtm = None
        
        change_mtm = None
        change_mtm_percent = None
        if previous_inflasi_mtm and previous_inflasi_mtm.bulanan:
            change_mtm = float(latest_inflasi_mtm.bulanan) - float(previous_inflasi_mtm.bulanan)
            change_mtm_percent = change_mtm  # For m-to-m, change is already a percentage difference
        
        previous_period = None
        if previous_inflasi_mtm:
            previous_period = f"{previous_inflasi_mtm.get_month_display()} {previous_inflasi_mtm.year}"
        
        indicator_carousel_data.append({
            'name': 'Inflasi M-to-M',
            'value': latest_inflasi_mtm.bulanan,
            'unit': '%',
            'period': f"{latest_inflasi_mtm.get_month_display()} {latest_inflasi_mtm.year}",
            'previous_period': previous_period,
            'change': change_mtm,
            'change_percent': change_mtm_percent,
            'change_type': 'm-to-m'
        })
    
    # 2. Inflasi Y-to-Y (Year to Year)
    if latest_inflasi_mtm and latest_inflasi_mtm.yoy:
        previous_inflasi_yty = Inflasi.objects.filter(
            year=latest_inflasi_mtm.year - 1,
            month=latest_inflasi_mtm.month
        ).first()
        
        change_yty = None
        change_yty_percent = None
        if previous_inflasi_yty and previous_inflasi_yty.yoy:
            change_yty = float(latest_inflasi_mtm.yoy) - float(previous_inflasi_yty.yoy)
            change_yty_percent = change_yty
        
        previous_period = None
        if previous_inflasi_yty:
            previous_period = f"{previous_inflasi_yty.get_month_display()} {previous_inflasi_yty.year}"
        
        indicator_carousel_data.append({
            'name': 'Inflasi Y-to-Y',
            'value': latest_inflasi_mtm.yoy,
            'unit': '%',
            'period': f"{latest_inflasi_mtm.get_month_display()} {latest_inflasi_mtm.year}",
            'previous_period': previous_period,
            'change': change_yty,
            'change_percent': change_yty_percent,
            'change_type': 'y-to-y'
        })
    
    # 3. PDRB Pengeluaran ADHB Tahunan
    latest_pdrb_peng_adhb = get_latest_pdrb_data(PDRBPengeluaranADHB)
    if latest_pdrb_peng_adhb:
        previous_pdrb_peng_adhb = get_previous_pdrb_data(PDRBPengeluaranADHB, latest_pdrb_peng_adhb.year)
        change = None
        change_percent = None
        if previous_pdrb_peng_adhb and latest_pdrb_peng_adhb.value and previous_pdrb_peng_adhb.value:
            change = float(latest_pdrb_peng_adhb.value) - float(previous_pdrb_peng_adhb.value)
            change_percent = (change / float(previous_pdrb_peng_adhb.value)) * 100 if float(previous_pdrb_peng_adhb.value) != 0 else 0
        
        previous_period = None
        if previous_pdrb_peng_adhb:
            previous_period = f"{previous_pdrb_peng_adhb.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Pengeluaran ADHB (Tahunan)',
            'value': latest_pdrb_peng_adhb.value,
            'unit': '',
            'period': f"{latest_pdrb_peng_adhb.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'y-to-y',
            'is_currency': True
        })
    
    # 4. PDRB Pengeluaran ADHK Tahunan
    latest_pdrb_peng_adhk = get_latest_pdrb_data(PDRBPengeluaranADHK)
    if latest_pdrb_peng_adhk:
        previous_pdrb_peng_adhk = get_previous_pdrb_data(PDRBPengeluaranADHK, latest_pdrb_peng_adhk.year)
        change = None
        change_percent = None
        if previous_pdrb_peng_adhk and latest_pdrb_peng_adhk.value and previous_pdrb_peng_adhk.value:
            change = float(latest_pdrb_peng_adhk.value) - float(previous_pdrb_peng_adhk.value)
            change_percent = (change / float(previous_pdrb_peng_adhk.value)) * 100 if float(previous_pdrb_peng_adhk.value) != 0 else 0
        
        previous_period = None
        if previous_pdrb_peng_adhk:
            previous_period = f"{previous_pdrb_peng_adhk.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Pengeluaran ADHK (Tahunan)',
            'value': latest_pdrb_peng_adhk.value,
            'unit': '',
            'period': f"{latest_pdrb_peng_adhk.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'y-to-y',
            'is_currency': True
        })
    
    # 5. PDRB Pengeluaran Laju Pertumbuhan Tahunan
    latest_pdrb_peng_laju = get_latest_pdrb_data(PDRBPengeluaranLajuPDRB)
    if latest_pdrb_peng_laju:
        previous_pdrb_peng_laju = get_previous_pdrb_data(PDRBPengeluaranLajuPDRB, latest_pdrb_peng_laju.year)
        change = None
        if previous_pdrb_peng_laju and latest_pdrb_peng_laju.value and previous_pdrb_peng_laju.value:
            change = float(latest_pdrb_peng_laju.value) - float(previous_pdrb_peng_laju.value)
        
        previous_period = None
        if previous_pdrb_peng_laju:
            previous_period = f"{previous_pdrb_peng_laju.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Pengeluaran Laju Pertumbuhan (Tahunan)',
            'value': latest_pdrb_peng_laju.value,
            'unit': '%',
            'period': f"{latest_pdrb_peng_laju.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change,
            'change_type': 'y-to-y'
        })
    
    # 6. PDRB Pengeluaran ADHB Triwulanan
    latest_pdrb_peng_adhb_q = get_latest_pdrb_data(PDRBPengeluaranADHBTriwulanan)
    if latest_pdrb_peng_adhb_q:
        previous_pdrb_peng_adhb_q = get_previous_pdrb_data(PDRBPengeluaranADHBTriwulanan, latest_pdrb_peng_adhb_q.year, latest_pdrb_peng_adhb_q.quarter)
        change = None
        change_percent = None
        if previous_pdrb_peng_adhb_q and latest_pdrb_peng_adhb_q.value and previous_pdrb_peng_adhb_q.value:
            change = float(latest_pdrb_peng_adhb_q.value) - float(previous_pdrb_peng_adhb_q.value)
            change_percent = (change / float(previous_pdrb_peng_adhb_q.value)) * 100 if float(previous_pdrb_peng_adhb_q.value) != 0 else 0
        
        previous_period = None
        if previous_pdrb_peng_adhb_q:
            previous_period = f"Q{previous_pdrb_peng_adhb_q.quarter} {previous_pdrb_peng_adhb_q.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Pengeluaran ADHB (Triwulanan)',
            'value': latest_pdrb_peng_adhb_q.value,
            'unit': '',
            'period': f"Q{latest_pdrb_peng_adhb_q.quarter} {latest_pdrb_peng_adhb_q.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'q-to-q',
            'is_currency': True
        })
    
    # 7. PDRB Pengeluaran ADHK Triwulanan
    latest_pdrb_peng_adhk_q = get_latest_pdrb_data(PDRBPengeluaranADHKTriwulanan)
    if latest_pdrb_peng_adhk_q:
        previous_pdrb_peng_adhk_q = get_previous_pdrb_data(PDRBPengeluaranADHKTriwulanan, latest_pdrb_peng_adhk_q.year, latest_pdrb_peng_adhk_q.quarter)
        change = None
        change_percent = None
        if previous_pdrb_peng_adhk_q and latest_pdrb_peng_adhk_q.value and previous_pdrb_peng_adhk_q.value:
            change = float(latest_pdrb_peng_adhk_q.value) - float(previous_pdrb_peng_adhk_q.value)
            change_percent = (change / float(previous_pdrb_peng_adhk_q.value)) * 100 if float(previous_pdrb_peng_adhk_q.value) != 0 else 0
        
        previous_period = None
        if previous_pdrb_peng_adhk_q:
            previous_period = f"Q{previous_pdrb_peng_adhk_q.quarter} {previous_pdrb_peng_adhk_q.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Pengeluaran ADHK (Triwulanan)',
            'value': latest_pdrb_peng_adhk_q.value,
            'unit': '',
            'period': f"Q{latest_pdrb_peng_adhk_q.quarter} {latest_pdrb_peng_adhk_q.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'q-to-q',
            'is_currency': True
        })
    
    # 8. PDRB Pengeluaran Laju Q-to-Q Triwulanan
    latest_pdrb_peng_laju_qtoq = get_latest_pdrb_data(PDRBPengeluaranLajuQtoQ)
    if latest_pdrb_peng_laju_qtoq:
        previous_pdrb_peng_laju_qtoq = get_previous_pdrb_data(PDRBPengeluaranLajuQtoQ, latest_pdrb_peng_laju_qtoq.year, latest_pdrb_peng_laju_qtoq.quarter)
        change = None
        if previous_pdrb_peng_laju_qtoq and latest_pdrb_peng_laju_qtoq.value and previous_pdrb_peng_laju_qtoq.value:
            change = float(latest_pdrb_peng_laju_qtoq.value) - float(previous_pdrb_peng_laju_qtoq.value)
        
        previous_period = None
        if previous_pdrb_peng_laju_qtoq:
            previous_period = f"Q{previous_pdrb_peng_laju_qtoq.quarter} {previous_pdrb_peng_laju_qtoq.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Pengeluaran Laju Q-to-Q (Triwulanan)',
            'value': latest_pdrb_peng_laju_qtoq.value,
            'unit': '%',
            'period': f"Q{latest_pdrb_peng_laju_qtoq.quarter} {latest_pdrb_peng_laju_qtoq.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change,
            'change_type': 'q-to-q'
        })
    
    # 8b. PDRB Pengeluaran Laju Y-to-Y Triwulanan
    latest_pdrb_peng_laju_yty_q = get_latest_pdrb_data(PDRBPengeluaranLajuYtoY)
    if latest_pdrb_peng_laju_yty_q:
        previous_pdrb_peng_laju_yty_q = get_previous_pdrb_data(PDRBPengeluaranLajuYtoY, latest_pdrb_peng_laju_yty_q.year, latest_pdrb_peng_laju_yty_q.quarter)
        change = None
        if previous_pdrb_peng_laju_yty_q and latest_pdrb_peng_laju_yty_q.value and previous_pdrb_peng_laju_yty_q.value:
            change = float(latest_pdrb_peng_laju_yty_q.value) - float(previous_pdrb_peng_laju_yty_q.value)
        
        previous_period = None
        if previous_pdrb_peng_laju_yty_q:
            previous_period = f"Q{previous_pdrb_peng_laju_yty_q.quarter} {previous_pdrb_peng_laju_yty_q.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Pengeluaran Laju Y-to-Y (Triwulanan)',
            'value': latest_pdrb_peng_laju_yty_q.value,
            'unit': '%',
            'period': f"Q{latest_pdrb_peng_laju_yty_q.quarter} {latest_pdrb_peng_laju_yty_q.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change,
            'change_type': 'y-to-y'
        })
    
    # 8c. PDRB Pengeluaran Laju C-to-C Triwulanan
    latest_pdrb_peng_laju_ctoc = get_latest_pdrb_data(PDRBPengeluaranLajuCtoC)
    if latest_pdrb_peng_laju_ctoc:
        previous_pdrb_peng_laju_ctoc = get_previous_pdrb_data(PDRBPengeluaranLajuCtoC, latest_pdrb_peng_laju_ctoc.year, latest_pdrb_peng_laju_ctoc.quarter)
        change = None
        if previous_pdrb_peng_laju_ctoc and latest_pdrb_peng_laju_ctoc.value and previous_pdrb_peng_laju_ctoc.value:
            change = float(latest_pdrb_peng_laju_ctoc.value) - float(previous_pdrb_peng_laju_ctoc.value)
        
        previous_period = None
        if previous_pdrb_peng_laju_ctoc:
            previous_period = f"Q{previous_pdrb_peng_laju_ctoc.quarter} {previous_pdrb_peng_laju_ctoc.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Pengeluaran Laju C-to-C (Triwulanan)',
            'value': latest_pdrb_peng_laju_ctoc.value,
            'unit': '%',
            'period': f"Q{latest_pdrb_peng_laju_ctoc.quarter} {latest_pdrb_peng_laju_ctoc.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change,
            'change_type': 'c-to-c'
        })
    
    # 9. PDRB Lapangan Usaha ADHB Tahunan
    latest_pdrb_lap_adhb = get_latest_pdrb_data(PDRBLapanganUsahaADHB)
    if latest_pdrb_lap_adhb:
        previous_pdrb_lap_adhb = get_previous_pdrb_data(PDRBLapanganUsahaADHB, latest_pdrb_lap_adhb.year)
        change = None
        change_percent = None
        if previous_pdrb_lap_adhb and latest_pdrb_lap_adhb.value and previous_pdrb_lap_adhb.value:
            change = float(latest_pdrb_lap_adhb.value) - float(previous_pdrb_lap_adhb.value)
            change_percent = (change / float(previous_pdrb_lap_adhb.value)) * 100 if float(previous_pdrb_lap_adhb.value) != 0 else 0
        
        previous_period = None
        if previous_pdrb_lap_adhb:
            previous_period = f"{previous_pdrb_lap_adhb.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Lapangan Usaha ADHB (Tahunan)',
            'value': latest_pdrb_lap_adhb.value,
            'unit': '',
            'period': f"{latest_pdrb_lap_adhb.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'y-to-y',
            'is_currency': True
        })
    
    # 10. PDRB Lapangan Usaha ADHK Tahunan
    latest_pdrb_lap_adhk = get_latest_pdrb_data(PDRBLapanganUsahaADHK)
    if latest_pdrb_lap_adhk:
        previous_pdrb_lap_adhk = get_previous_pdrb_data(PDRBLapanganUsahaADHK, latest_pdrb_lap_adhk.year)
        change = None
        change_percent = None
        if previous_pdrb_lap_adhk and latest_pdrb_lap_adhk.value and previous_pdrb_lap_adhk.value:
            change = float(latest_pdrb_lap_adhk.value) - float(previous_pdrb_lap_adhk.value)
            change_percent = (change / float(previous_pdrb_lap_adhk.value)) * 100 if float(previous_pdrb_lap_adhk.value) != 0 else 0
        
        previous_period = None
        if previous_pdrb_lap_adhk:
            previous_period = f"{previous_pdrb_lap_adhk.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Lapangan Usaha ADHK (Tahunan)',
            'value': latest_pdrb_lap_adhk.value,
            'unit': '',
            'period': f"{latest_pdrb_lap_adhk.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'y-to-y',
            'is_currency': True
        })
    
    # 11. PDRB Lapangan Usaha Laju Pertumbuhan Tahunan
    latest_pdrb_lap_laju = get_latest_pdrb_data(PDRBLapanganUsahaLajuPDRB)
    if latest_pdrb_lap_laju:
        previous_pdrb_lap_laju = get_previous_pdrb_data(PDRBLapanganUsahaLajuPDRB, latest_pdrb_lap_laju.year)
        change = None
        if previous_pdrb_lap_laju and latest_pdrb_lap_laju.value and previous_pdrb_lap_laju.value:
            change = float(latest_pdrb_lap_laju.value) - float(previous_pdrb_lap_laju.value)
        
        previous_period = None
        if previous_pdrb_lap_laju:
            previous_period = f"{previous_pdrb_lap_laju.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Lapangan Usaha Laju Pertumbuhan (Tahunan)',
            'value': latest_pdrb_lap_laju.value,
            'unit': '%',
            'period': f"{latest_pdrb_lap_laju.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change,
            'change_type': 'y-to-y'
        })
    
    # 12. PDRB Lapangan Usaha ADHB Triwulanan
    latest_pdrb_lap_adhb_q = get_latest_pdrb_data(PDRBLapanganUsahaADHBTriwulanan)
    if latest_pdrb_lap_adhb_q:
        previous_pdrb_lap_adhb_q = get_previous_pdrb_data(PDRBLapanganUsahaADHBTriwulanan, latest_pdrb_lap_adhb_q.year, latest_pdrb_lap_adhb_q.quarter)
        change = None
        change_percent = None
        if previous_pdrb_lap_adhb_q and latest_pdrb_lap_adhb_q.value and previous_pdrb_lap_adhb_q.value:
            change = float(latest_pdrb_lap_adhb_q.value) - float(previous_pdrb_lap_adhb_q.value)
            change_percent = (change / float(previous_pdrb_lap_adhb_q.value)) * 100 if float(previous_pdrb_lap_adhb_q.value) != 0 else 0
        
        previous_period = None
        if previous_pdrb_lap_adhb_q:
            previous_period = f"Q{previous_pdrb_lap_adhb_q.quarter} {previous_pdrb_lap_adhb_q.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Lapangan Usaha ADHB (Triwulanan)',
            'value': latest_pdrb_lap_adhb_q.value,
            'unit': '',
            'period': f"Q{latest_pdrb_lap_adhb_q.quarter} {latest_pdrb_lap_adhb_q.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'q-to-q',
            'is_currency': True
        })
    
    # 13. PDRB Lapangan Usaha ADHK Triwulanan
    latest_pdrb_lap_adhk_q = get_latest_pdrb_data(PDRBLapanganUsahaADHKTriwulanan)
    if latest_pdrb_lap_adhk_q:
        previous_pdrb_lap_adhk_q = get_previous_pdrb_data(PDRBLapanganUsahaADHKTriwulanan, latest_pdrb_lap_adhk_q.year, latest_pdrb_lap_adhk_q.quarter)
        change = None
        change_percent = None
        if previous_pdrb_lap_adhk_q and latest_pdrb_lap_adhk_q.value and previous_pdrb_lap_adhk_q.value:
            change = float(latest_pdrb_lap_adhk_q.value) - float(previous_pdrb_lap_adhk_q.value)
            change_percent = (change / float(previous_pdrb_lap_adhk_q.value)) * 100 if float(previous_pdrb_lap_adhk_q.value) != 0 else 0
        
        previous_period = None
        if previous_pdrb_lap_adhk_q:
            previous_period = f"Q{previous_pdrb_lap_adhk_q.quarter} {previous_pdrb_lap_adhk_q.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Lapangan Usaha ADHK (Triwulanan)',
            'value': latest_pdrb_lap_adhk_q.value,
            'unit': '',
            'period': f"Q{latest_pdrb_lap_adhk_q.quarter} {latest_pdrb_lap_adhk_q.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'q-to-q',
            'is_currency': True
        })
    
    # 14a. PDRB Lapangan Usaha Laju Q-to-Q Triwulanan
    latest_pdrb_lap_laju_qtoq = get_latest_pdrb_data(PDRBLapanganUsahaLajuQtoQ)
    if latest_pdrb_lap_laju_qtoq:
        previous_pdrb_lap_laju_qtoq = get_previous_pdrb_data(PDRBLapanganUsahaLajuQtoQ, latest_pdrb_lap_laju_qtoq.year, latest_pdrb_lap_laju_qtoq.quarter)
        change = None
        if previous_pdrb_lap_laju_qtoq and latest_pdrb_lap_laju_qtoq.value and previous_pdrb_lap_laju_qtoq.value:
            change = float(latest_pdrb_lap_laju_qtoq.value) - float(previous_pdrb_lap_laju_qtoq.value)
        
        previous_period = None
        if previous_pdrb_lap_laju_qtoq:
            previous_period = f"Q{previous_pdrb_lap_laju_qtoq.quarter} {previous_pdrb_lap_laju_qtoq.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Lapangan Usaha Laju Q-to-Q (Triwulanan)',
            'value': latest_pdrb_lap_laju_qtoq.value,
            'unit': '%',
            'period': f"Q{latest_pdrb_lap_laju_qtoq.quarter} {latest_pdrb_lap_laju_qtoq.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change,
            'change_type': 'q-to-q'
        })
    
    # 14b. PDRB Lapangan Usaha Laju Y-to-Y Triwulanan
    latest_pdrb_lap_laju_yty_q = get_latest_pdrb_data(PDRBLapanganUsahaLajuYtoY)
    if latest_pdrb_lap_laju_yty_q:
        previous_pdrb_lap_laju_yty_q = get_previous_pdrb_data(PDRBLapanganUsahaLajuYtoY, latest_pdrb_lap_laju_yty_q.year, latest_pdrb_lap_laju_yty_q.quarter)
        change = None
        if previous_pdrb_lap_laju_yty_q and latest_pdrb_lap_laju_yty_q.value and previous_pdrb_lap_laju_yty_q.value:
            change = float(latest_pdrb_lap_laju_yty_q.value) - float(previous_pdrb_lap_laju_yty_q.value)
        
        previous_period = None
        if previous_pdrb_lap_laju_yty_q:
            previous_period = f"Q{previous_pdrb_lap_laju_yty_q.quarter} {previous_pdrb_lap_laju_yty_q.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Lapangan Usaha Laju Y-to-Y (Triwulanan)',
            'value': latest_pdrb_lap_laju_yty_q.value,
            'unit': '%',
            'period': f"Q{latest_pdrb_lap_laju_yty_q.quarter} {latest_pdrb_lap_laju_yty_q.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change,
            'change_type': 'y-to-y'
        })
    
    # 14c. PDRB Lapangan Usaha Laju C-to-C Triwulanan
    latest_pdrb_lap_laju_ctoc = get_latest_pdrb_data(PDRBLapanganUsahaLajuCtoC)
    if latest_pdrb_lap_laju_ctoc:
        previous_pdrb_lap_laju_ctoc = get_previous_pdrb_data(PDRBLapanganUsahaLajuCtoC, latest_pdrb_lap_laju_ctoc.year, latest_pdrb_lap_laju_ctoc.quarter)
        change = None
        if previous_pdrb_lap_laju_ctoc and latest_pdrb_lap_laju_ctoc.value and previous_pdrb_lap_laju_ctoc.value:
            change = float(latest_pdrb_lap_laju_ctoc.value) - float(previous_pdrb_lap_laju_ctoc.value)
        
        previous_period = None
        if previous_pdrb_lap_laju_ctoc:
            previous_period = f"Q{previous_pdrb_lap_laju_ctoc.quarter} {previous_pdrb_lap_laju_ctoc.year}"
        
        indicator_carousel_data.append({
            'name': 'PDRB Lapangan Usaha Laju C-to-C (Triwulanan)',
            'value': latest_pdrb_lap_laju_ctoc.value,
            'unit': '%',
            'period': f"Q{latest_pdrb_lap_laju_ctoc.quarter} {latest_pdrb_lap_laju_ctoc.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change,
            'change_type': 'c-to-c'
        })
    
    # 15. TPK (Tingkat Pengangguran Terbuka) - Tahunan
    latest_tpk = KetenagakerjaanTPT.objects.order_by('-year').first()
    if latest_tpk and latest_tpk.total:
        previous_tpk = KetenagakerjaanTPT.objects.filter(year=latest_tpk.year - 1).first()
        change = None
        change_percent = None
        previous_period = None
        if previous_tpk and previous_tpk.total:
            change = float(latest_tpk.total) - float(previous_tpk.total)
            change_percent = (change / float(previous_tpk.total)) * 100 if float(previous_tpk.total) != 0 else 0
            previous_period = f"{previous_tpk.year}"
        
        indicator_carousel_data.append({
            'name': 'TPT (Tingkat Pengangguran Terbuka)',
            'value': latest_tpk.total,
            'unit': '%',
            'period': f"{latest_tpk.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'y-to-y'
        })
    
    # 15b. TPK (Tingkat Pengangguran Terbuka) - Bulanan (M-to-M)
    latest_tpk_monthly = HotelOccupancyCombined.objects.order_by('-year', '-month').first()
    if latest_tpk_monthly and latest_tpk_monthly.tpk is not None:
        # Get previous month data
        if latest_tpk_monthly.month in month_order:
            current_month_idx = month_order.index(latest_tpk_monthly.month)
            if current_month_idx > 0:
                prev_month = month_order[current_month_idx - 1]
                previous_tpk_monthly = HotelOccupancyCombined.objects.filter(year=latest_tpk_monthly.year, month=prev_month).first()
            else:
                previous_tpk_monthly = HotelOccupancyCombined.objects.filter(year=latest_tpk_monthly.year - 1, month='DESEMBER').first()
        else:
            previous_tpk_monthly = None
        
        change = None
        change_percent = None
        previous_period = None
        if previous_tpk_monthly and previous_tpk_monthly.tpk is not None:
            change = float(latest_tpk_monthly.tpk) - float(previous_tpk_monthly.tpk)
            change_percent = change  # For m-to-m, change is already a percentage difference
            # Format previous period - need to get month display name
            month_map = {'JANUARI': 'Januari', 'FEBRUARI': 'Februari', 'MARET': 'Maret', 'APRIL': 'April',
                        'MEI': 'Mei', 'JUNI': 'Juni', 'JULI': 'Juli', 'AGUSTUS': 'Agustus',
                        'SEPTEMBER': 'September', 'OKTOBER': 'Oktober', 'NOPEMBER': 'November', 'DESEMBER': 'Desember'}
            prev_month_display = month_map.get(previous_tpk_monthly.month, previous_tpk_monthly.month)
            previous_period = f"{prev_month_display} {previous_tpk_monthly.year}"
        
        month_map = {'JANUARI': 'Januari', 'FEBRUARI': 'Februari', 'MARET': 'Maret', 'APRIL': 'April',
                    'MEI': 'Mei', 'JUNI': 'Juni', 'JULI': 'Juli', 'AGUSTUS': 'Agustus',
                    'SEPTEMBER': 'September', 'OKTOBER': 'Oktober', 'NOPEMBER': 'November', 'DESEMBER': 'Desember'}
        current_month_display = month_map.get(latest_tpk_monthly.month, latest_tpk_monthly.month)
        
        indicator_carousel_data.append({
            'name': 'TPK M-to-M',
            'value': latest_tpk_monthly.tpk,
            'unit': '%',
            'period': f"{current_month_display} {latest_tpk_monthly.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'm-to-m'
        })
    
    # 16. RLMT Gabungan - Tahunan
    if latest_hotel_yearly and latest_hotel_yearly.rlmtgab:
        previous_rlmtgab = HotelOccupancyYearly.objects.filter(year=latest_hotel_yearly.year - 1).first()
        change = None
        change_percent = None
        previous_period = None
        if previous_rlmtgab and previous_rlmtgab.rlmtgab:
            change = float(latest_hotel_yearly.rlmtgab) - float(previous_rlmtgab.rlmtgab)
            change_percent = (change / float(previous_rlmtgab.rlmtgab)) * 100 if float(previous_rlmtgab.rlmtgab) != 0 else 0
            previous_period = f"{previous_rlmtgab.year}"
        
        indicator_carousel_data.append({
            'name': 'RLMT Gabungan',
            'value': latest_hotel_yearly.rlmtgab,
            'unit': '%',
            'period': f"{latest_hotel_yearly.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'y-to-y'
        })
    
    # 16b. RLMT Gabungan - Bulanan (M-to-M)
    latest_rlmt_monthly = HotelOccupancyCombined.objects.order_by('-year', '-month').first()
    if latest_rlmt_monthly and latest_rlmt_monthly.rlmtgab is not None:
        # Get previous month data
        if latest_rlmt_monthly.month in month_order:
            current_month_idx = month_order.index(latest_rlmt_monthly.month)
            if current_month_idx > 0:
                prev_month = month_order[current_month_idx - 1]
                previous_rlmt_monthly = HotelOccupancyCombined.objects.filter(year=latest_rlmt_monthly.year, month=prev_month).first()
            else:
                previous_rlmt_monthly = HotelOccupancyCombined.objects.filter(year=latest_rlmt_monthly.year - 1, month='DESEMBER').first()
        else:
            previous_rlmt_monthly = None
        
        change = None
        change_percent = None
        previous_period = None
        if previous_rlmt_monthly and previous_rlmt_monthly.rlmtgab is not None:
            change = float(latest_rlmt_monthly.rlmtgab) - float(previous_rlmt_monthly.rlmtgab)
            change_percent = change  # For m-to-m, change is already a percentage difference
            # Format previous period
            month_map = {'JANUARI': 'Januari', 'FEBRUARI': 'Februari', 'MARET': 'Maret', 'APRIL': 'April',
                        'MEI': 'Mei', 'JUNI': 'Juni', 'JULI': 'Juli', 'AGUSTUS': 'Agustus',
                        'SEPTEMBER': 'September', 'OKTOBER': 'Oktober', 'NOPEMBER': 'November', 'DESEMBER': 'Desember'}
            prev_month_display = month_map.get(previous_rlmt_monthly.month, previous_rlmt_monthly.month)
            previous_period = f"{prev_month_display} {previous_rlmt_monthly.year}"
        
        month_map = {'JANUARI': 'Januari', 'FEBRUARI': 'Februari', 'MARET': 'Maret', 'APRIL': 'April',
                    'MEI': 'Mei', 'JUNI': 'Juni', 'JULI': 'Juli', 'AGUSTUS': 'Agustus',
                    'SEPTEMBER': 'September', 'OKTOBER': 'Oktober', 'NOPEMBER': 'November', 'DESEMBER': 'Desember'}
        current_month_display = month_map.get(latest_rlmt_monthly.month, latest_rlmt_monthly.month)
        
        indicator_carousel_data.append({
            'name': 'RLMT Gabungan M-to-M',
            'value': latest_rlmt_monthly.rlmtgab,
            'unit': '%',
            'period': f"{current_month_display} {latest_rlmt_monthly.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'm-to-m'
        })
    
    # 17. Gini Ratio
    if latest_gini:
        previous_period = None
        if previous_gini:
            previous_period = f"{previous_gini.year}"
        
        indicator_carousel_data.append({
            'name': 'Gini Ratio',
            'value': latest_gini.gini_ratio_value,
            'unit': '',
            'period': f"{latest_gini.year}",
            'previous_period': previous_period,
            'change': gini_change,
            'change_percent': gini_change_percent,
            'change_type': 'y-to-y'
        })
    
    # 18. Kemiskinan (Persentase Penduduk Miskin)
    latest_kemiskinan = KemiskinanSurabaya.objects.order_by('-year').first()
    if latest_kemiskinan and latest_kemiskinan.persentase_penduduk_miskin:
        previous_kemiskinan = KemiskinanSurabaya.objects.filter(year=latest_kemiskinan.year - 1).first()
        change = None
        change_percent = None
        if previous_kemiskinan and previous_kemiskinan.persentase_penduduk_miskin:
            change = float(latest_kemiskinan.persentase_penduduk_miskin) - float(previous_kemiskinan.persentase_penduduk_miskin)
            change_percent = (change / float(previous_kemiskinan.persentase_penduduk_miskin)) * 100 if float(previous_kemiskinan.persentase_penduduk_miskin) != 0 else 0
        
        previous_period = None
        if previous_kemiskinan:
            previous_period = f"{previous_kemiskinan.year}"
        
        indicator_carousel_data.append({
            'name': 'Kemiskinan (Persentase)',
            'value': latest_kemiskinan.persentase_penduduk_miskin,
            'unit': '%',
            'period': f"{latest_kemiskinan.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'y-to-y'
        })
    
    # 19. Ketenagakerjaan TPAK
    latest_tpak = KetenagakerjaanTPAK.objects.order_by('-year').first()
    if latest_tpak and latest_tpak.total:
        previous_tpak = KetenagakerjaanTPAK.objects.filter(year=latest_tpak.year - 1).first()
        change = None
        change_percent = None
        if previous_tpak and previous_tpak.total:
            change = float(latest_tpak.total) - float(previous_tpak.total)
            change_percent = (change / float(previous_tpak.total)) * 100 if float(previous_tpak.total) != 0 else 0
        
        previous_period = None
        if previous_tpak:
            previous_period = f"{previous_tpak.year}"
        
        indicator_carousel_data.append({
            'name': 'TPAK (Tingkat Partisipasi Angkatan Kerja)',
            'value': latest_tpak.total,
            'unit': '%',
            'period': f"{latest_tpak.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'y-to-y'
        })
    
    # 20. Kependudukan (Total Penduduk)
    latest_kependudukan = Kependudukan.objects.filter(gender='TOTAL').order_by('-year').first()
    if latest_kependudukan and latest_kependudukan.population:
        previous_kependudukan = Kependudukan.objects.filter(gender='TOTAL', year=latest_kependudukan.year - 1).first()
        change = None
        change_percent = None
        if previous_kependudukan and previous_kependudukan.population:
            change = float(latest_kependudukan.population) - float(previous_kependudukan.population)
            change_percent = (change / float(previous_kependudukan.population)) * 100 if float(previous_kependudukan.population) != 0 else 0
        
        previous_period = None
        if previous_kependudukan:
            previous_period = f"{previous_kependudukan.year}"
        
        indicator_carousel_data.append({
            'name': 'Kependudukan (Total Penduduk)',
            'value': latest_kependudukan.population,
            'unit': '',
            'period': f"{latest_kependudukan.year}",
            'previous_period': previous_period,
            'change': change,
            'change_percent': change_percent,
            'change_type': 'y-to-y',
            'is_currency': False
        })
    
    # 21. IPM
    if latest_ipm:
        previous_period = None
        if previous_ipm:
            previous_period = f"{previous_ipm.year}"
        
        indicator_carousel_data.append({
            'name': 'IPM (Indeks Pembangunan Manusia)',
            'value': latest_ipm.ipm_value,
            'unit': '',
            'period': f"{latest_ipm.year}",
            'previous_period': previous_period,
            'change': ipm_change,
            'change_percent': ipm_change_percent,
            'change_type': 'y-to-y'
        })
    
    # 22. All IPM Indicators (already collected in ipm_subcategories)
    for subcat in ipm_subcategories:
        previous_period = None
        if subcat.get('year'):
            previous_period = f"{subcat['year'] - 1}"
        
        indicator_carousel_data.append({
            'name': f'IPM {subcat["name"]}',
            'value': subcat['value'],
            'unit': '',
            'period': f"{subcat['year']}",
            'previous_period': previous_period,
            'change': subcat.get('change'),
            'change_percent': subcat.get('change_percent'),
            'change_type': 'y-to-y'
        })

    context = {
        'countNews':countNews,
        'dataNewss':dataNewss,
        'dataNews': dataNews,
        'dataInfographic': dataInfographic,
        'dataInfographics': dataInfographics,
        'dataInfographicLatest': dataInfographicLatest,
        'dataPublication': dataPublication,
        'dataPublications': dataPublications,
        'dataNewsLatest': dataNewsLatest,
        'dataPublicationsLatest': dataPublicationsLatest,
        'bookmarked_items': bookmarked_items,
        # IPM sub-categories for carousel
        'ipm_subcategories': ipm_subcategories,
        # IPM main for summary card
        'latest_ipm': latest_ipm,
        'ipm_change': ipm_change,
        'ipm_change_percent': ipm_change_percent,
        # Gini Ratio for summary card
        'latest_gini': latest_gini,
        'gini_change': gini_change,
        'gini_change_percent': gini_change_percent,
        # Hotel Occupancy for summary cards
        'latest_hotel_yearly': latest_hotel_yearly,
        # Indicator carousel data
        'indicator_carousel_data': indicator_carousel_data,
        'user': request.user,  # Explicitly pass user to template
    }
    return render(request, 'dashboard/dashboard.html', context)

def infographics(request):
    """Merender halaman infografis."""
    infographics_list = Infographic.objects.all().order_by('-created_at', '-id')

    paginator = Paginator(infographics_list, 12)
    page = request.GET.get('page', 1)
    
    try:
        infographics_data = paginator.page(page)
    except PageNotAnInteger:
        infographics_data = paginator.page(1)
    except EmptyPage:
        infographics_data = paginator.page(paginator.num_pages)
    
    # Add bookmark_id to each infographic if user is authenticated
    if request.user.is_authenticated:
        from django.contrib.contenttypes.models import ContentType
        infographic_ct = ContentType.objects.get_for_model(Infographic)
        user_bookmarks = Bookmark.objects.filter(
            user=request.user,
            content_type=infographic_ct
        ).values_list('object_id', 'id')
        bookmark_dict = {str(obj_id): bookmark_id for obj_id, bookmark_id in user_bookmarks}
        
        # Add bookmark_id to each infographic object
        for infographic in infographics_data:
            infographic.bookmark_id = bookmark_dict.get(str(infographic.id), None)
    
    # Get latest news for sidebar
    latest_news = News.objects.order_by('-release_date', '-news_id')[:5]
    news_count = News.objects.count()
    
    # Get all infographics for related items (exclude current page items)
    all_infographics = list(Infographic.objects.all().order_by('-created_at', '-id')[:20])
    
    # Add bookmark_id to all_infographics if user is authenticated
    if request.user.is_authenticated:
        from django.contrib.contenttypes.models import ContentType
        infographic_ct = ContentType.objects.get_for_model(Infographic)
        user_bookmarks = Bookmark.objects.filter(
            user=request.user,
            content_type=infographic_ct
        ).values_list('object_id', 'id')
        bookmark_dict = {str(obj_id): bookmark_id for obj_id, bookmark_id in user_bookmarks}
        
        for infographic in all_infographics:
            infographic.bookmark_id = bookmark_dict.get(str(infographic.id), None)
    
    context = {
        'dataInpographic': infographics_data,
        'allInfographics': all_infographics,  # For related infographics
        'dataNews': latest_news,
        'countNews': news_count,
        'countInfographic': Infographic.objects.count(),
        'page_title': 'Infographics',
        'user': request.user,
    }

    return render(request, 'dashboard/infographics.html', context)

def download_infographic(request, infographic_id):
    """Download infographic image dengan proxy untuk memastikan file terdownload dengan benar."""
    # Check if user is authenticated
    if not request.user.is_authenticated:
        # Redirect to login page
        return redirect('login')
    
    try:
        infographic = get_object_or_404(Infographic, id=infographic_id)
        
        # Prioritaskan dl URL, jika tidak ada gunakan image URL
        download_url = infographic.dl or infographic.image
        
        if not download_url:
            raise Http404("Download URL tidak tersedia")
        
        # Fetch image dari URL BPS
        try:
            response = requests.get(download_url, stream=True, timeout=30, allow_redirects=True)
            response.raise_for_status()
            
            # Deteksi content type dari response
            content_type = response.headers.get('Content-Type', 'image/png')
            
            # Ambil data gambar
            image_data = response.content
            
            # Validasi bahwa ini adalah gambar (bukan HTML)
            # Cek magic bytes untuk format gambar umum
            is_image = False
            if image_data.startswith(b'\x89PNG\r\n\x1a\n'):  # PNG
                is_image = True
                content_type = 'image/png'
            elif image_data.startswith(b'\xff\xd8\xff'):  # JPEG
                is_image = True
                content_type = 'image/jpeg'
            elif image_data.startswith(b'GIF87a') or image_data.startswith(b'GIF89a'):  # GIF
                is_image = True
                content_type = 'image/gif'
            elif image_data.startswith(b'RIFF') and b'WEBP' in image_data[:12]:  # WEBP
                is_image = True
                content_type = 'image/webp'
            elif 'text/html' in content_type.lower() or image_data.strip().startswith(b'<'):
                # Jika HTML, coba fallback ke image URL
                if infographic.image and infographic.image != download_url:
                    img_response = requests.get(infographic.image, stream=True, timeout=30, allow_redirects=True)
                    img_response.raise_for_status()
                    content_type = img_response.headers.get('Content-Type', 'image/png')
                    image_data = img_response.content
                    # Validasi lagi
                    if not (image_data.startswith(b'\x89PNG') or image_data.startswith(b'\xff\xd8') or 
                           image_data.startswith(b'GIF') or (image_data.startswith(b'RIFF') and b'WEBP' in image_data[:12])):
                        raise Http404("File yang diambil bukan file gambar yang valid")
                else:
                    raise Http404("File gambar tidak ditemukan atau format tidak didukung")
            
            # Generate filename dari title atau gunakan default
            if infographic.title:
                # Clean title untuk filename - handle Indonesian characters
                filename = infographic.title
                # Replace spaces and special chars
                filename = re.sub(r'[^\w\s-]', '', filename)
                filename = re.sub(r'[-\s]+', '_', filename)
                filename = filename.strip('_')[:100]  # Limit panjang filename
                if not filename:
                    filename = f"infographic_{infographic_id}"
            else:
                filename = f"infographic_{infographic_id}"
            
            # Deteksi ekstensi dari content type
            if 'png' in content_type.lower():
                ext = '.png'
            elif 'jpeg' in content_type.lower() or 'jpg' in content_type.lower():
                ext = '.jpg'
            elif 'gif' in content_type.lower():
                ext = '.gif'
            elif 'webp' in content_type.lower():
                ext = '.webp'
            else:
                # Coba deteksi dari URL
                parsed_url = urlparse(download_url)
                path_ext = os.path.splitext(parsed_url.path)[1].lower()
                ext = path_ext if path_ext in ['.png', '.jpg', '.jpeg', '.gif', '.webp'] else '.png'
            
            # Pastikan filename tidak sudah memiliki ekstensi
            if not filename.lower().endswith(ext.lower()):
                filename = filename + ext
            
            # Create HTTP response dengan proper headers
            http_response = HttpResponse(image_data, content_type=content_type)
            http_response['Content-Disposition'] = f'attachment; filename="{filename}"'
            http_response['Content-Length'] = len(image_data)
            # Tambahkan header untuk mencegah caching
            http_response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            http_response['Pragma'] = 'no-cache'
            http_response['Expires'] = '0'
            
            return http_response
            
        except requests.RequestException as e:
            raise Http404(f"Gagal mengambil file: {str(e)}")
            
    except Http404:
        raise
    except Exception as e:
        raise Http404(f"Error: {str(e)}")

def download_publication(request, pub_id):
    """Redirect ke URL download publikasi asli dari BPS setelah cek authentication."""
    # Check if user is authenticated
    if not request.user.is_authenticated:
        # Redirect to login page
        return redirect('login')
    
    try:
        # Strip trailing slash if present
        pub_id = pub_id.rstrip('/')
        publication = get_object_or_404(Publication, pub_id=pub_id)
        
        # Get download URL from publication
        download_url = publication.dl
        
        if not download_url:
            raise Http404("Download URL tidak tersedia")
        
        # Direct redirect to BPS download URL - browser will handle PDF display
        return redirect(download_url)
        
    except Http404:
        raise
    except Exception as e:
        raise Http404(f"Error: {str(e)}")

def publications(request):
    """Merender halaman publikasi."""
    # Get year filter and search query from request
    selected_year = request.GET.get('year', '').strip()
    search_query = request.GET.get('search', '').strip()
    
    # Start with all publications
    publications_list = Publication.objects.all()
    
    # Apply search filter if provided
    if search_query:
        publications_list = publications_list.filter(
            Q(title__icontains=search_query) |
            Q(abstract__icontains=search_query)
        )
    
    # Apply year filter if selected
    if selected_year:
        try:
            year_int = int(selected_year)
            # Filter for years 1984 to current year
            from datetime import datetime
            current_year = datetime.now().year
            if 1984 <= year_int <= current_year:
                publications_list = publications_list.filter(date__year=year_int)
        except ValueError:
            # Invalid year, ignore filter
            pass
    
    # Order by date
    publications_list = publications_list.order_by('-date')
    
    # Get available years from database (1984 to current year)
    from datetime import datetime
    current_year = datetime.now().year
    available_years = Publication.objects.exclude(
        date__isnull=True
    ).annotate(
        year=ExtractYear('date')
    ).values_list('year', flat=True).distinct()
    
    # Filter years 1984 to current year and sort descending
    available_years = sorted([y for y in available_years if 1984 <= y <= current_year], reverse=True)
    
    # If no years found, generate default years from 1984 to current year
    if not available_years:
        available_years = list(range(current_year, 1983, -1))
    
    # Get total count
    total_count = Publication.objects.count()
    filtered_count = publications_list.count()
    
    # Pagination
    paginator = Paginator(publications_list, 10)
    page = request.GET.get('page', 1)
    
    try:
        publications_data = paginator.page(page)
    except PageNotAnInteger:
        publications_data = paginator.page(1)
    except EmptyPage:
        publications_data = paginator.page(paginator.num_pages)
    
    # Get latest news for sidebar
    latest_news = News.objects.order_by('-release_date', '-news_id')[:5]
    news_count = News.objects.count()
    
    # Add bookmark_id to each publication if user is authenticated
    if request.user.is_authenticated:
        from django.contrib.contenttypes.models import ContentType
        publication_ct = ContentType.objects.get_for_model(Publication)
        user_bookmarks = Bookmark.objects.filter(
            user=request.user,
            content_type=publication_ct
        ).values_list('object_id', 'id')
        bookmark_dict = {str(obj_id): bookmark_id for obj_id, bookmark_id in user_bookmarks}
        
        # Add bookmark_id to each publication object
        # Note: Bookmark.object_id uses Publication.pk (primary key), not pub_id
        for publication in publications_data:
            publication.bookmark_id = bookmark_dict.get(str(publication.pk), None)
    
    context = {
        'dataPublication': publications_data,
        'dataNews': latest_news,
        'countNews': news_count,
        'countPublication': total_count,
        'filtered_count': filtered_count,
        'selected_year': selected_year,
        'search_query': search_query,
        'available_years': available_years,
        'page_title': 'Publications',
        'user': request.user,
    }

    return render(request, 'dashboard/publications.html', context)

def news(request):
    # Get filters and search query
    search_query = request.GET.get('search', '').strip()
    category_id_filter = request.GET.get('category_id', '').strip()
    sort_order = request.GET.get('sort', 'latest')
    
    # Start with all news
    news_list = News.objects.all()
    
    # Apply search filter
    if search_query:
        news_list = news_list.filter(
            Q(title__icontains=search_query) |
            Q(content__icontains=search_query) |
            Q(category_name__icontains=search_query)
        )
    
    # Apply category ID filter
    if category_id_filter:
        news_list = news_list.filter(category_id=category_id_filter)
    
    # Apply sorting
    if sort_order == 'oldest':
        news_list = news_list.order_by('release_date', 'news_id')
    else:  # latest (default)
        news_list = news_list.order_by('-release_date', '-news_id')
    
    # Get total count after filtering
    total_count = news_list.count()
    
    # Get available categories for filter dropdown (using category_id)
    available_categories = News.objects.filter(
        category_id__isnull=False,
        category_name__isnull=False
    ).values('category_id', 'category_name').distinct().order_by('category_name')
    
    # Pagination - 15 items per page
    paginator = Paginator(news_list, 15)
    page = request.GET.get('page', 1)
    
    try:
        news_data = paginator.page(page)
    except PageNotAnInteger:
        news_data = paginator.page(1)
    except EmptyPage:
        news_data = paginator.page(paginator.num_pages)
    
    # Add bookmark_id to each news if user is authenticated
    if request.user.is_authenticated:
        from django.contrib.contenttypes.models import ContentType
        news_ct = ContentType.objects.get_for_model(News)
        user_bookmarks = Bookmark.objects.filter(
            user=request.user,
            content_type=news_ct
        ).values_list('object_id', 'id')
        bookmark_dict = {str(obj_id): bookmark_id for obj_id, bookmark_id in user_bookmarks}
        
        # Add bookmark_id to each news object
        for news in news_data:
            news.bookmark_id = bookmark_dict.get(str(news.news_id), None)
    
    context = {
        'dataNewss': news_data,
        'countNews': News.objects.count(),
        'filtered_count': total_count,
        'search_query': search_query,
        'category_id_filter': category_id_filter,
        'sort_order': sort_order,
        'available_categories': available_categories,
        'page_title': 'News',
        'user': request.user,
    }
    
    return render(request, 'dashboard/news.html', context)

# ======= Tampilan Indikator Strategis Kota Surabaya =======
def ipm(request):
    """Merender halaman IPM dengan visualisasi data untuk Kota Surabaya dan Jawa Timur."""
    # Filter data untuk Kota Surabaya dan Jawa Timur
    surabaya_keywords = ['KOTA SURABAYA', 'SURABAYA']
    jatim_keywords = ['JAWA TIMUR', 'JATIM']
    
    # Get all IPM data
    all_ipm_data = list(HumanDevelopmentIndex.objects.all())
    
    # Filter untuk Kota Surabaya
    surabaya_data = []
    for data in all_ipm_data:
        location_upper = data.location_name.upper().strip()
        if 'SURABAYA' in location_upper:
            if 'KOTA' in location_upper or location_upper.startswith('SURABAYA'):
                surabaya_data.append(data)
    
    # Filter untuk Jawa Timur
    jatim_data = []
    for data in all_ipm_data:
        location_upper = data.location_name.upper().strip()
        if 'JAWA TIMUR' in location_upper or location_upper == 'JATIM':
            jatim_data.append(data)
    
    # Sort by year
    surabaya_data.sort(key=lambda x: x.year)
    jatim_data.sort(key=lambda x: x.year)
    
    # Get distinct years
    all_years = sorted(set([d.year for d in all_ipm_data]))
    latest_year = max(all_years) if all_years else None
    
    # Get latest data for summary cards
    latest_surabaya = None
    latest_jatim = None
    previous_surabaya = None
    previous_jatim = None
    
    if latest_year:
        latest_surabaya = next((d for d in surabaya_data if d.year == latest_year), None)
        latest_jatim = next((d for d in jatim_data if d.year == latest_year), None)
        
        if latest_year > min(all_years) if all_years else False:
            previous_year = latest_year - 1
            previous_surabaya = next((d for d in surabaya_data if d.year == previous_year), None)
            previous_jatim = next((d for d in jatim_data if d.year == previous_year), None)
    
    # Calculate changes
    surabaya_change = None
    jatim_change = None
    if latest_surabaya and previous_surabaya:
        surabaya_change = float(latest_surabaya.ipm_value) - float(previous_surabaya.ipm_value)
    if latest_jatim and previous_jatim:
        jatim_change = float(latest_jatim.ipm_value) - float(previous_jatim.ipm_value)
    
    # Get sub-category data - both latest and historical data
    def get_subcategory_data(model_class, location_name_keywords):
        """Helper function to get all historical data for a sub-category"""
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            location_upper = data.location_name.upper().strip()
            if any(keyword in location_upper for keyword in location_name_keywords):
                filtered_data.append(data)
        if filtered_data:
            filtered_data.sort(key=lambda x: x.year)
            return filtered_data
        return []
    
    def get_latest_subcategory_data(model_class, location_name_keywords):
        """Helper function to get latest data for a sub-category"""
        all_data = get_subcategory_data(model_class, location_name_keywords)
        if all_data:
            return all_data[-1] if all_data else None
        return None
    
    # Get all historical data for each sub-category (Kota Surabaya)
    uhh_sp_surabaya_data = get_subcategory_data(IPM_UHH_SP, ['SURABAYA'])
    hls_surabaya_data = get_subcategory_data(IPM_HLS, ['SURABAYA'])
    rls_surabaya_data = get_subcategory_data(IPM_RLS, ['SURABAYA'])
    pengeluaran_surabaya_data = get_subcategory_data(IPM_PengeluaranPerKapita, ['SURABAYA'])
    indeks_kesehatan_surabaya_data = get_subcategory_data(IPM_IndeksKesehatan, ['SURABAYA'])
    indeks_hidup_layak_surabaya_data = get_subcategory_data(IPM_IndeksHidupLayak, ['SURABAYA'])
    indeks_pendidikan_surabaya_data = get_subcategory_data(IPM_IndeksPendidikan, ['SURABAYA'])
    
    # Get latest data for summary cards
    latest_uhh_sp_surabaya = get_latest_subcategory_data(IPM_UHH_SP, ['SURABAYA'])
    latest_hls_surabaya = get_latest_subcategory_data(IPM_HLS, ['SURABAYA'])
    latest_rls_surabaya = get_latest_subcategory_data(IPM_RLS, ['SURABAYA'])
    latest_pengeluaran_surabaya = get_latest_subcategory_data(IPM_PengeluaranPerKapita, ['SURABAYA'])
    latest_indeks_kesehatan_surabaya = get_latest_subcategory_data(IPM_IndeksKesehatan, ['SURABAYA'])
    latest_indeks_hidup_layak_surabaya = get_latest_subcategory_data(IPM_IndeksHidupLayak, ['SURABAYA'])
    latest_indeks_pendidikan_surabaya = get_latest_subcategory_data(IPM_IndeksPendidikan, ['SURABAYA'])
    
    context = {
        'surabaya_data': surabaya_data,
        'jatim_data': jatim_data,
        'all_years': all_years,
        'latest_year': latest_year,
        'latest_surabaya': latest_surabaya,
        'latest_jatim': latest_jatim,
        'previous_surabaya': previous_surabaya,
        'previous_jatim': previous_jatim,
        'surabaya_change': surabaya_change,
        'jatim_change': jatim_change,
        # Sub-category latest data for summary cards
        'latest_uhh_sp_surabaya': latest_uhh_sp_surabaya,
        'latest_hls_surabaya': latest_hls_surabaya,
        'latest_rls_surabaya': latest_rls_surabaya,
        'latest_pengeluaran_surabaya': latest_pengeluaran_surabaya,
        'latest_indeks_kesehatan_surabaya': latest_indeks_kesehatan_surabaya,
        'latest_indeks_hidup_layak_surabaya': latest_indeks_hidup_layak_surabaya,
        'latest_indeks_pendidikan_surabaya': latest_indeks_pendidikan_surabaya,
        # Sub-category historical data for charts
        'uhh_sp_surabaya_data': uhh_sp_surabaya_data,
        'hls_surabaya_data': hls_surabaya_data,
        'rls_surabaya_data': rls_surabaya_data,
        'pengeluaran_surabaya_data': pengeluaran_surabaya_data,
        'indeks_kesehatan_surabaya_data': indeks_kesehatan_surabaya_data,
        'indeks_hidup_layak_surabaya_data': indeks_hidup_layak_surabaya_data,
        'indeks_pendidikan_surabaya_data': indeks_pendidikan_surabaya_data,
        'page_title': 'Indeks Pembangunan Manusia',
    }
    
    return render(request, 'dashboard/indikator/IPM.html', context)

def indeks_pembangunan_manusia(request):
    """Merender halaman Indeks Pembangunan Manusia dengan visualisasi alternatif."""
    # Filter data untuk Kota Surabaya dan Jawa Timur
    surabaya_keywords = ['KOTA SURABAYA', 'SURABAYA']
    jatim_keywords = ['JAWA TIMUR', 'JATIM']
    
    # Get all IPM data
    all_ipm_data = list(HumanDevelopmentIndex.objects.all())
    
    # Filter untuk Kota Surabaya
    surabaya_data = []
    for data in all_ipm_data:
        location_upper = data.location_name.upper().strip()
        if 'SURABAYA' in location_upper:
            if 'KOTA' in location_upper or location_upper.startswith('SURABAYA'):
                surabaya_data.append(data)
    
    # Filter untuk Jawa Timur
    jatim_data = []
    for data in all_ipm_data:
        location_upper = data.location_name.upper().strip()
        if 'JAWA TIMUR' in location_upper or location_upper == 'JATIM':
            jatim_data.append(data)
    
    # Sort by year
    surabaya_data.sort(key=lambda x: x.year)
    jatim_data.sort(key=lambda x: x.year)
    
    # Get distinct years
    all_years = sorted(set([d.year for d in all_ipm_data]))
    latest_year = max(all_years) if all_years else None
    
    # Get latest data for summary cards
    latest_surabaya = None
    latest_jatim = None
    previous_surabaya = None
    previous_jatim = None
    
    if latest_year:
        latest_surabaya = next((d for d in surabaya_data if d.year == latest_year), None)
        latest_jatim = next((d for d in jatim_data if d.year == latest_year), None)
        
        if latest_year > min(all_years) if all_years else False:
            previous_year = latest_year - 1
            previous_surabaya = next((d for d in surabaya_data if d.year == previous_year), None)
            previous_jatim = next((d for d in jatim_data if d.year == previous_year), None)
    
    # Calculate changes
    surabaya_change = None
    jatim_change = None
    if latest_surabaya and previous_surabaya:
        surabaya_change = float(latest_surabaya.ipm_value) - float(previous_surabaya.ipm_value)
    if latest_jatim and previous_jatim:
        jatim_change = float(latest_jatim.ipm_value) - float(previous_jatim.ipm_value)
    
    # Get sub-category data - both latest and historical data
    def get_subcategory_data(model_class, location_name_keywords):
        """Helper function to get all historical data for a sub-category"""
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            location_upper = data.location_name.upper().strip()
            if any(keyword in location_upper for keyword in location_name_keywords):
                filtered_data.append(data)
        if filtered_data:
            filtered_data.sort(key=lambda x: x.year)
            return filtered_data
        return []
    
    def get_latest_subcategory_data(model_class, location_name_keywords):
        """Helper function to get latest data for a sub-category"""
        all_data = get_subcategory_data(model_class, location_name_keywords)
        if all_data:
            return all_data[-1] if all_data else None
        return None
    
    # Get all historical data for each sub-category (Kota Surabaya)
    uhh_sp_surabaya_data = get_subcategory_data(IPM_UHH_SP, ['SURABAYA'])
    hls_surabaya_data = get_subcategory_data(IPM_HLS, ['SURABAYA'])
    rls_surabaya_data = get_subcategory_data(IPM_RLS, ['SURABAYA'])
    pengeluaran_surabaya_data = get_subcategory_data(IPM_PengeluaranPerKapita, ['SURABAYA'])
    indeks_kesehatan_surabaya_data = get_subcategory_data(IPM_IndeksKesehatan, ['SURABAYA'])
    indeks_hidup_layak_surabaya_data = get_subcategory_data(IPM_IndeksHidupLayak, ['SURABAYA'])
    indeks_pendidikan_surabaya_data = get_subcategory_data(IPM_IndeksPendidikan, ['SURABAYA'])
    
    # Get latest data for summary cards
    latest_uhh_sp_surabaya = get_latest_subcategory_data(IPM_UHH_SP, ['SURABAYA'])
    latest_hls_surabaya = get_latest_subcategory_data(IPM_HLS, ['SURABAYA'])
    latest_rls_surabaya = get_latest_subcategory_data(IPM_RLS, ['SURABAYA'])
    latest_pengeluaran_surabaya = get_latest_subcategory_data(IPM_PengeluaranPerKapita, ['SURABAYA'])
    latest_indeks_kesehatan_surabaya = get_latest_subcategory_data(IPM_IndeksKesehatan, ['SURABAYA'])
    latest_indeks_hidup_layak_surabaya = get_latest_subcategory_data(IPM_IndeksHidupLayak, ['SURABAYA'])
    latest_indeks_pendidikan_surabaya = get_latest_subcategory_data(IPM_IndeksPendidikan, ['SURABAYA'])
    
    context = {
        'surabaya_data': surabaya_data,
        'jatim_data': jatim_data,
        'all_years': all_years,
        'latest_year': latest_year,
        'latest_surabaya': latest_surabaya,
        'latest_jatim': latest_jatim,
        'previous_surabaya': previous_surabaya,
        'previous_jatim': previous_jatim,
        'surabaya_change': surabaya_change,
        'jatim_change': jatim_change,
        # Sub-category latest data for summary cards
        'latest_uhh_sp_surabaya': latest_uhh_sp_surabaya,
        'latest_hls_surabaya': latest_hls_surabaya,
        'latest_rls_surabaya': latest_rls_surabaya,
        'latest_pengeluaran_surabaya': latest_pengeluaran_surabaya,
        'latest_indeks_kesehatan_surabaya': latest_indeks_kesehatan_surabaya,
        'latest_indeks_hidup_layak_surabaya': latest_indeks_hidup_layak_surabaya,
        'latest_indeks_pendidikan_surabaya': latest_indeks_pendidikan_surabaya,
        # Sub-category historical data for charts
        'uhh_sp_surabaya_data': uhh_sp_surabaya_data,
        'hls_surabaya_data': hls_surabaya_data,
        'rls_surabaya_data': rls_surabaya_data,
        'pengeluaran_surabaya_data': pengeluaran_surabaya_data,
        'indeks_kesehatan_surabaya_data': indeks_kesehatan_surabaya_data,
        'indeks_hidup_layak_surabaya_data': indeks_hidup_layak_surabaya_data,
        'indeks_pendidikan_surabaya_data': indeks_pendidikan_surabaya_data,
        'page_title': 'Indeks Pembangunan Manusia',
    }
    
    return render(request, 'dashboard/indikator/indeks pembangunan manusia.html', context)

def hotel_occupancy(request):
    """Merender halaman Tingkat Hunian Hotel dengan visualisasi data TPK."""
    # Fetch hotel occupancy data from database
    
    # Month order for proper sorting
    month_order = ['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 
                  'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember']
    
    # Get all data - we'll sort manually by month order
    all_occupancy_data = list(HotelOccupancyCombined.objects.all())
    
    # Get yearly data for annual TPK chart
    yearly_occupancy_data = list(HotelOccupancyYearly.objects.all().order_by('year'))
    
    # Helper function to get month index for sorting
    def get_month_index(month_name):
        try:
            return month_order.index(month_name)
        except ValueError:
            return 999  # Put unknown months at the end
    
    # Sort all data by year (descending) and month (chronological)
    all_occupancy_data.sort(key=lambda x: (x.year, get_month_index(x.month)), reverse=False)
    
    # Convert back to queryset-like structure for template
    from django.db.models import QuerySet
    occupancy_data = all_occupancy_data
    
    # Get distinct years for dropdown
    distinct_years = sorted(set([d.year for d in all_occupancy_data]))
    
    # Get latest year
    latest_year = max([d.year for d in all_occupancy_data]) if all_occupancy_data else None
    
    # Get latest data for reference (last item after sorting)
    latest_data = all_occupancy_data[-1] if all_occupancy_data else None
    
    # Get latest month data - find the latest year, then latest month in that year
    latest_month_data = None
    if latest_year:
        # Get all data for latest year
        current_year_data = [d for d in all_occupancy_data if d.year == latest_year]
        
        # Sort by month order to get the actual latest month
        current_year_data.sort(key=lambda x: get_month_index(x.month))
        
        if current_year_data:
            latest_month_data = current_year_data[-1]  # Get the last month after sorting
        
        previous_year_data = [d for d in all_occupancy_data if d.year == latest_year - 1] if latest_year > 2020 else []
        
        # Get previous month for comparison
        if latest_month_data:
            try:
                current_month_idx = month_order.index(latest_month_data.month)
                if current_month_idx > 0:
                    prev_month = month_order[current_month_idx - 1]
                    previous_month_data = next((d for d in current_year_data if d.month == prev_month), None)
                else:
                    # If it's January, check previous year's December
                    if latest_year > 2020 and previous_year_data:
                        previous_year_data_sorted = sorted(previous_year_data, key=lambda x: get_month_index(x.month))
                        previous_month_data = next((d for d in previous_year_data_sorted if d.month == 'Desember'), None)
                    else:
                        previous_month_data = None
            except ValueError:
                previous_month_data = None
            
            # Calculate changes for all fields
            changes = {}
            if previous_month_data:
                if latest_month_data.tpk and previous_month_data.tpk:
                    changes['tpk'] = float(latest_month_data.tpk) - float(previous_month_data.tpk)
                if latest_month_data.mktj and previous_month_data.mktj:
                    changes['mktj'] = float(latest_month_data.mktj) - float(previous_month_data.mktj)
                if latest_month_data.rlmtgab and previous_month_data.rlmtgab:
                    changes['rlmtgab'] = float(latest_month_data.rlmtgab) - float(previous_month_data.rlmtgab)
                if latest_month_data.gpr and previous_month_data.gpr:
                    changes['gpr'] = float(latest_month_data.gpr) - float(previous_month_data.gpr)
            else:
                changes = {}
            
            # Keep backward compatibility
            tpk_change = changes.get('tpk')
            tpk_change_abs = abs(tpk_change) if tpk_change else None
        else:
            previous_month_data = None
            changes = {}
            tpk_change = None
            tpk_change_abs = None
    else:
        current_year_data = None
        previous_year_data = None
        latest_month_data = None
        previous_month_data = None
        changes = {}
        tpk_change = None
        tpk_change_abs = None
    
    context = {
        'occupancy_data': occupancy_data,
        'yearly_occupancy_data': yearly_occupancy_data,
        'distinct_years': distinct_years,
        'latest_data': latest_data,
        'current_year_data': current_year_data if 'current_year_data' in locals() else None,
        'previous_year_data': previous_year_data if 'previous_year_data' in locals() else None,
        'latest_month_data': latest_month_data,
        'previous_month_data': previous_month_data if 'previous_month_data' in locals() else None,
        'tpk_change': tpk_change if 'tpk_change' in locals() else None,
        'tpk_change_abs': tpk_change_abs if 'tpk_change_abs' in locals() else None,
        'changes': changes if 'changes' in locals() else {},
        'latest_year': latest_year,
        'page_title': 'Tingkat Hunian Hotel',
    }
    
    return render(request, 'dashboard/indikator/hotel_occupancy.html', context)

@api_view(['GET'])
def get_hotel_occupancy_data(request):
    """API endpoint untuk mendapatkan data tingkat hunian hotel."""
    try:
        year = request.query_params.get('year', None)
        
        if year:
            occupancy_data = HotelOccupancyCombined.objects.filter(year=int(year)).order_by('month')
        else:
            occupancy_data = HotelOccupancyCombined.objects.all().order_by('year', 'month')
        
        serializer = HotelOccupancyCombinedSerializer(occupancy_data, many=True)
        return Response({
            "status": "success",
            "data": serializer.data,
            "count": len(serializer.data)
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

def gini_ratio(request):
    """Merender halaman Gini Ratio dengan visualisasi data untuk Kota Surabaya dan Jawa Timur."""
    # Filter data untuk Kota Surabaya dan Jawa Timur
    # Mencari dengan berbagai variasi nama yang mungkin ada di database
    surabaya_keywords = ['KOTA SURABAYA', 'SURABAYA']
    jatim_keywords = ['JAWA TIMUR', 'JATIM']
    
    # Get all Gini Ratio data
    all_gini_data = list(GiniRatio.objects.all())
    
    # Filter untuk Kota Surabaya - lebih spesifik untuk menghindari duplikasi
    surabaya_data = []
    for data in all_gini_data:
        location_upper = data.location_name.upper().strip()
        # Cek apakah mengandung kata kunci Surabaya dan bukan hanya bagian dari kata lain
        if 'SURABAYA' in location_upper:
            # Pastikan ini adalah Kota Surabaya, bukan kabupaten lain yang mengandung kata Surabaya
            if 'KOTA' in location_upper or location_upper.startswith('SURABAYA'):
                surabaya_data.append(data)
    
    # Filter untuk Jawa Timur
    jatim_data = []
    for data in all_gini_data:
        location_upper = data.location_name.upper().strip()
        # Cek untuk Jawa Timur
        if 'JAWA TIMUR' in location_upper or location_upper == 'JATIM':
            jatim_data.append(data)
    
    # Sort by year
    surabaya_data.sort(key=lambda x: x.year)
    jatim_data.sort(key=lambda x: x.year)
    
    # Get distinct years
    all_years = sorted(set([d.year for d in all_gini_data]))
    latest_year = max(all_years) if all_years else None
    
    # Get latest data for summary cards
    latest_surabaya = None
    latest_jatim = None
    previous_surabaya = None
    previous_jatim = None
    
    if latest_year:
        latest_surabaya = next((d for d in surabaya_data if d.year == latest_year), None)
        latest_jatim = next((d for d in jatim_data if d.year == latest_year), None)
        
        if latest_year > min(all_years) if all_years else False:
            prev_year = latest_year - 1
            previous_surabaya = next((d for d in surabaya_data if d.year == prev_year), None)
            previous_jatim = next((d for d in jatim_data if d.year == prev_year), None)
    
    # Calculate changes
    surabaya_change = None
    jatim_change = None
    if latest_surabaya and previous_surabaya:
        surabaya_change = float(latest_surabaya.gini_ratio_value) - float(previous_surabaya.gini_ratio_value)
    if latest_jatim and previous_jatim:
        jatim_change = float(latest_jatim.gini_ratio_value) - float(previous_jatim.gini_ratio_value)
    
    context = {
        'surabaya_data': surabaya_data,
        'jatim_data': jatim_data,
        'all_years': all_years,
        'latest_year': latest_year,
        'latest_surabaya': latest_surabaya,
        'latest_jatim': latest_jatim,
        'previous_surabaya': previous_surabaya,
        'previous_jatim': previous_jatim,
        'surabaya_change': surabaya_change,
        'jatim_change': jatim_change,
        'page_title': 'Gini Ratio',
    }
    
    return render(request, 'dashboard/indikator/gini_ratio.html', context)

@api_view(['GET'])
def get_gini_ratio_data(request):
    """API endpoint untuk mendapatkan data Gini Ratio."""
    try:
        location = request.query_params.get('location', None)
        year = request.query_params.get('year', None)
        
        queryset = GiniRatio.objects.all()
        
        if location:
            # Filter by location (case-insensitive)
            queryset = queryset.filter(location_name__icontains=location)
        
        if year:
            queryset = queryset.filter(year=int(year))
        
        queryset = queryset.order_by('year', 'location_name')
        
        serializer = GiniRatioSerializer(queryset, many=True)
        return Response({
            "status": "success",
            "data": serializer.data,
            "count": len(serializer.data)
        })
    except Exception as e:
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

def kemiskinan(request):
    """Merender halaman Kemiskinan dengan visualisasi data untuk Kota Surabaya dan Jawa Timur."""
    # Get all Kemiskinan data
    surabaya_data = list(KemiskinanSurabaya.objects.all().order_by('year'))
    jatim_data = list(KemiskinanJawaTimur.objects.all().order_by('year'))
    
    # Get distinct years
    all_years = sorted(set([d.year for d in surabaya_data] + [d.year for d in jatim_data]))
    latest_year = max(all_years) if all_years else None
    
    # Get latest and previous data for summary cards
    latest_surabaya = None
    latest_jatim = None
    previous_surabaya = None
    previous_jatim = None
    
    if latest_year:
        latest_surabaya = next((d for d in surabaya_data if d.year == latest_year), None)
        latest_jatim = next((d for d in jatim_data if d.year == latest_year), None)
        
        if latest_year > min(all_years) if all_years else False:
            prev_year = latest_year - 1
            previous_surabaya = next((d for d in surabaya_data if d.year == prev_year), None)
            previous_jatim = next((d for d in jatim_data if d.year == prev_year), None)
    
    # Calculate changes for all indicators
    def calculate_change(latest, previous, field_name):
        if latest and previous:
            latest_val = getattr(latest, field_name)
            previous_val = getattr(previous, field_name)
            if latest_val is not None and previous_val is not None:
                return float(latest_val) - float(previous_val)
        return None
    
    # Calculate changes for Surabaya
    surabaya_changes = {
        'jumlah_penduduk_miskin': calculate_change(latest_surabaya, previous_surabaya, 'jumlah_penduduk_miskin'),
        'persentase_penduduk_miskin': calculate_change(latest_surabaya, previous_surabaya, 'persentase_penduduk_miskin'),
        'indeks_kedalaman_kemiskinan_p1': calculate_change(latest_surabaya, previous_surabaya, 'indeks_kedalaman_kemiskinan_p1'),
        'indeks_keparahan_kemiskinan_p2': calculate_change(latest_surabaya, previous_surabaya, 'indeks_keparahan_kemiskinan_p2'),
        'garis_kemiskinan': calculate_change(latest_surabaya, previous_surabaya, 'garis_kemiskinan'),
    }
    
    # Calculate percentage changes
    def calculate_percent_change(latest, previous, field_name):
        if latest and previous:
            latest_val = getattr(latest, field_name)
            previous_val = getattr(previous, field_name)
            if latest_val is not None and previous_val is not None and float(previous_val) != 0:
                change = float(latest_val) - float(previous_val)
                return (change / float(previous_val)) * 100
        return None
    
    surabaya_percent_changes = {
        'jumlah_penduduk_miskin': calculate_percent_change(latest_surabaya, previous_surabaya, 'jumlah_penduduk_miskin'),
        'persentase_penduduk_miskin': calculate_percent_change(latest_surabaya, previous_surabaya, 'persentase_penduduk_miskin'),
        'indeks_kedalaman_kemiskinan_p1': calculate_percent_change(latest_surabaya, previous_surabaya, 'indeks_kedalaman_kemiskinan_p1'),
        'indeks_keparahan_kemiskinan_p2': calculate_percent_change(latest_surabaya, previous_surabaya, 'indeks_keparahan_kemiskinan_p2'),
        'garis_kemiskinan': calculate_percent_change(latest_surabaya, previous_surabaya, 'garis_kemiskinan'),
    }
    
    context = {
        'surabaya_data': surabaya_data,
        'jatim_data': jatim_data,
        'all_years': all_years,
        'latest_year': latest_year,
        'latest_surabaya': latest_surabaya,
        'latest_jatim': latest_jatim,
        'previous_surabaya': previous_surabaya,
        'previous_jatim': previous_jatim,
        'surabaya_changes': surabaya_changes,
        'surabaya_percent_changes': surabaya_percent_changes,
        'page_title': 'Kemiskinan',
    }
    
    return render(request, 'dashboard/indikator/kemiskinan.html', context)

def kependudukan(request):
    """Merender halaman Kependudukan dengan visualisasi data populasi."""
    from django.db.models import Sum, Q
    from collections import defaultdict
    
    # Get all population data
    all_data = Kependudukan.objects.all()
    
    # Get distinct years - handle empty queryset
    all_years = sorted(set([d.year for d in all_data if d.year])) if all_data.exists() else []
    latest_year = max(all_years) if all_years else None
    
    # Get selected year from request (default to latest year)
    selected_year = request.GET.get('year', None)
    if selected_year:
        try:
            selected_year = int(selected_year)
            if selected_year not in all_years:
                selected_year = latest_year
        except:
            selected_year = latest_year
    else:
        selected_year = latest_year
    
    # Get selected year data for summary cards
    selected_data = all_data.filter(year=selected_year) if selected_year else Kependudukan.objects.none()
    
    # Get previous year data for comparison
    previous_year = None
    previous_data = Kependudukan.objects.none()
    if selected_year and all_years:
        year_index = all_years.index(selected_year)
        if year_index > 0:
            previous_year = all_years[year_index - 1]
            previous_data = all_data.filter(year=previous_year)
    
    # Calculate summary statistics for selected year
    total_population = 0
    total_male = 0
    total_female = 0
    
    if selected_year:
        total_data = selected_data.filter(gender='TOTAL')
        male_data = selected_data.filter(gender='LK')
        female_data = selected_data.filter(gender='PR')
        
        total_population = sum([d.population for d in total_data if d.population]) or 0
        total_male = sum([d.population for d in male_data if d.population]) or 0
        total_female = sum([d.population for d in female_data if d.population]) or 0
    
    # Calculate previous year statistics
    prev_total_population = 0
    prev_total_male = 0
    prev_total_female = 0
    
    if previous_year:
        prev_total_data = previous_data.filter(gender='TOTAL')
        prev_male_data = previous_data.filter(gender='LK')
        prev_female_data = previous_data.filter(gender='PR')
        
        prev_total_population = sum([d.population for d in prev_total_data if d.population]) or 0
        prev_total_male = sum([d.population for d in prev_male_data if d.population]) or 0
        prev_total_female = sum([d.population for d in prev_female_data if d.population]) or 0
    
    # Calculate changes
    total_change = total_population - prev_total_population if prev_total_population > 0 else None
    male_change = total_male - prev_total_male if prev_total_male > 0 else None
    female_change = total_female - prev_total_female if prev_total_female > 0 else None
    
    # Calculate ratio (male to female) - format as L/P ratio (e.g., 102-103)
    # Ratio is calculated as (male/female) * 100, then rounded to get range
    population_ratio = (total_male / total_female * 100) if total_female > 0 else None
    if population_ratio is not None:
        population_ratio_rounded = round(population_ratio)
        # Create range format (e.g., 102-103)
        population_ratio_display = f"{population_ratio_rounded}-{population_ratio_rounded + 1}"
    else:
        population_ratio_display = None
    
    prev_population_ratio = (prev_total_male / prev_total_female * 100) if prev_total_female > 0 else None
    if prev_population_ratio is not None:
        prev_population_ratio_rounded = round(prev_population_ratio)
        prev_population_ratio_display = f"{prev_population_ratio_rounded}-{prev_population_ratio_rounded + 1}"
    else:
        prev_population_ratio_display = None
    
    # Calculate ratio change - both must be not None
    ratio_change = None
    if population_ratio is not None and prev_population_ratio is not None:
        ratio_change = population_ratio - prev_population_ratio
    
    # Get last 5 years for trend chart
    last_5_years = sorted(all_years)[-5:] if len(all_years) >= 5 else (all_years if all_years else [])
    
    # Prepare trend data
    trend_data = []
    for year in last_5_years:
        year_data = all_data.filter(year=year)
        total = sum([d.population for d in year_data.filter(gender='TOTAL') if d.population]) or 0
        male = sum([d.population for d in year_data.filter(gender='LK') if d.population]) or 0
        female = sum([d.population for d in year_data.filter(gender='PR') if d.population]) or 0
        
        trend_data.append({
            'year': year,
            'total': total,
            'male': male,
            'female': female
        })
    
    # Get distribution by age groups for selected year
    age_distribution = []
    if selected_year:
        selected_age_data = selected_data.filter(gender='TOTAL').exclude(age_group__in=['JUMLAH', 'TOTAL', ''])
        for data in selected_age_data:
            if data.age_group and data.population:
                age_distribution.append({
                    'age_group': data.age_group,
                    'population': data.population
                })
    
    # Group by age categories for pie chart
    def get_age_category(age_str):
        """Categorize age group into 5 categories."""
        try:
            # Extract age range (e.g., "0-4" -> 0, 4)
            if '-' in age_str:
                parts = age_str.split('-')
                start_age = int(parts[0].strip())
                end_age = int(parts[1].strip())
            elif '+' in age_str:
                # For "75+" type
                start_age = int(age_str.replace('+', '').strip())
                end_age = start_age + 10  # Assume range for categorization
            else:
                # Single age
                start_age = int(age_str.strip())
                end_age = start_age
            
            # Categorize: Bayi dan balita (0-5), Anak-anak (5-11), Remaja (12-25), Dewasa (26-55), Lansia (56+)
            # For overlapping ranges, use start_age
            if start_age >= 0 and start_age <= 5:
                return 'Bayi dan Balita (0-5)'
            elif start_age >= 6 and start_age <= 11:
                return 'Anak-anak (6-11)'
            elif start_age >= 12 and start_age <= 25:
                return 'Remaja (12-25)'
            elif start_age >= 26 and start_age <= 55:
                return 'Dewasa (26-55)'
            elif start_age >= 56:
                return 'Lansia (56+)'
            else:
                return 'Lainnya'
        except:
            return 'Lainnya'
    
    # Calculate pie chart data by age category
    age_category_data = defaultdict(int)
    if selected_year:
        selected_age_data = selected_data.filter(gender='TOTAL').exclude(age_group__in=['JUMLAH', 'TOTAL', ''])
        for data in selected_age_data:
            if data.population:
                category = get_age_category(data.age_group)
                age_category_data[category] += data.population
    
    # Order: Bayi dan Balita, Anak-anak, Remaja, Dewasa, Lansia
    category_order = [
        'Bayi dan Balita (0-5)',
        'Anak-anak (6-11)',
        'Remaja (12-25)',
        'Dewasa (26-55)',
        'Lansia (56+)'
    ]
    
    pie_chart_data = []
    for category in category_order:
        if category in age_category_data:
            pie_chart_data.append({
                'name': category,
                'value': age_category_data[category]
            })
    
    # Add any remaining categories not in the order list
    for k, v in age_category_data.items():
        if k not in category_order:
            pie_chart_data.append({'name': k, 'value': v})
    
    # Get pyramid data (selected year, LK and PR only)
    pyramid_data = []
    if selected_year:
        pyramid_lk = selected_data.filter(gender='LK').order_by('age_group')
        pyramid_pr = selected_data.filter(gender='PR').order_by('age_group')
        
        # Create a map of age groups
        age_groups = sorted(set([d.age_group for d in pyramid_lk if d.age_group] + 
                                 [d.age_group for d in pyramid_pr if d.age_group]))
        
        for age_group in age_groups:
            lk_data = next((d for d in pyramid_lk if d.age_group == age_group), None)
            pr_data = next((d for d in pyramid_pr if d.age_group == age_group), None)
            
            lk_pop = lk_data.population if lk_data and lk_data.population else 0
            pr_pop = pr_data.population if pr_data and pr_data.population else 0
            
            pyramid_data.append({
                'age_group': age_group,
                'male': lk_pop,
                'female': pr_pop
            })
    
    context = {
        'total_population': total_population,
        'total_male': total_male,
        'total_female': total_female,
        'population_ratio': population_ratio,
        'population_ratio_display': population_ratio_display,
        'prev_population_ratio_display': prev_population_ratio_display,
        'total_change': total_change,
        'male_change': male_change,
        'female_change': female_change,
        'ratio_change': ratio_change,
        'selected_year': selected_year,
        'previous_year': previous_year,
        'latest_year': latest_year,
        'all_years': all_years,
        'trend_data': trend_data,
        'age_distribution': age_distribution,
        'pie_chart_data': pie_chart_data,
        'pyramid_data': pyramid_data,
        'page_title': 'Kependudukan',
    }
    
    return render(request, 'dashboard/indikator/kependudukan.html', context)

def ketenagakerjaan(request):
    """Merender halaman Ketenagakerjaan dengan 2 tab: TPT dan TPAK."""
    # Get all TPT data sorted by year
    all_tpt_data = list(KetenagakerjaanTPT.objects.all().order_by('year'))
    
    # Get TPT latest data for summary cards
    tpt_latest_data = all_tpt_data[-1] if all_tpt_data else None
    tpt_previous_data = all_tpt_data[-2] if len(all_tpt_data) >= 2 else None
    
    # Calculate TPT changes
    tpt_total_change = None
    tpt_laki_laki_change = None
    tpt_perempuan_change = None
    
    if tpt_latest_data and tpt_previous_data:
        if tpt_latest_data.total is not None and tpt_previous_data.total is not None:
            tpt_total_change = float(tpt_latest_data.total) - float(tpt_previous_data.total)
        if tpt_latest_data.laki_laki is not None and tpt_previous_data.laki_laki is not None:
            tpt_laki_laki_change = float(tpt_latest_data.laki_laki) - float(tpt_previous_data.laki_laki)
        if tpt_latest_data.perempuan is not None and tpt_previous_data.perempuan is not None:
            tpt_perempuan_change = float(tpt_latest_data.perempuan) - float(tpt_previous_data.perempuan)
    
    # Get all TPAK data sorted by year
    all_tpak_data = list(KetenagakerjaanTPAK.objects.all().order_by('year'))
    
    # Get TPAK latest data for summary cards
    tpak_latest_data = all_tpak_data[-1] if all_tpak_data else None
    tpak_previous_data = all_tpak_data[-2] if len(all_tpak_data) >= 2 else None
    
    # Calculate TPAK changes
    tpak_total_change = None
    tpak_laki_laki_change = None
    tpak_perempuan_change = None
    
    if tpak_latest_data and tpak_previous_data:
        if tpak_latest_data.total is not None and tpak_previous_data.total is not None:
            tpak_total_change = float(tpak_latest_data.total) - float(tpak_previous_data.total)
        if tpak_latest_data.laki_laki is not None and tpak_previous_data.laki_laki is not None:
            tpak_laki_laki_change = float(tpak_latest_data.laki_laki) - float(tpak_previous_data.laki_laki)
        if tpak_latest_data.perempuan is not None and tpak_previous_data.perempuan is not None:
            tpak_perempuan_change = float(tpak_latest_data.perempuan) - float(tpak_previous_data.perempuan)
    
    context = {
        # TPT data
        'tpt_data': all_tpt_data,
        'tpt_latest_data': tpt_latest_data,
        'tpt_previous_data': tpt_previous_data,
        'tpt_total_change': tpt_total_change,
        'tpt_laki_laki_change': tpt_laki_laki_change,
        'tpt_perempuan_change': tpt_perempuan_change,
        # TPAK data
        'tpak_data': all_tpak_data,
        'tpak_latest_data': tpak_latest_data,
        'tpak_previous_data': tpak_previous_data,
        'tpak_total_change': tpak_total_change,
        'tpak_laki_laki_change': tpak_laki_laki_change,
        'tpak_perempuan_change': tpak_perempuan_change,
        'page_title': 'Ketenagakerjaan',
    }
    
    return render(request, 'dashboard/indikator/ketenagakerjaan.html', context)

def ketenagakerjaan_tpt(request):
    """Merender halaman Tingkat Pengangguran Terbuka (TPT) dengan visualisasi data."""
    # Get all TPT data sorted by year
    all_tpt_data = list(KetenagakerjaanTPT.objects.all().order_by('year'))
    
    # Get latest data for summary cards
    latest_data = all_tpt_data[-1] if all_tpt_data else None
    previous_data = all_tpt_data[-2] if len(all_tpt_data) >= 2 else None
    
    # Calculate changes
    total_change = None
    laki_laki_change = None
    perempuan_change = None
    
    if latest_data and previous_data:
        if latest_data.total is not None and previous_data.total is not None:
            total_change = float(latest_data.total) - float(previous_data.total)
        if latest_data.laki_laki is not None and previous_data.laki_laki is not None:
            laki_laki_change = float(latest_data.laki_laki) - float(previous_data.laki_laki)
        if latest_data.perempuan is not None and previous_data.perempuan is not None:
            perempuan_change = float(latest_data.perempuan) - float(previous_data.perempuan)
    
    # Get all years
    all_years = sorted(set([d.year for d in all_tpt_data]))
    latest_year = max(all_years) if all_years else None
    
    context = {
        'tpt_data': all_tpt_data,
        'latest_data': latest_data,
        'previous_data': previous_data,
        'total_change': total_change,
        'laki_laki_change': laki_laki_change,
        'perempuan_change': perempuan_change,
        'all_years': all_years,
        'latest_year': latest_year,
        'page_title': 'Tingkat Pengangguran Terbuka (TPT)',
    }
    
    return render(request, 'dashboard/indikator/ketenagakerjaan_tpt.html', context)

def ketenagakerjaan_tpak(request):
    """Merender halaman Tingkat Partisipasi Angkatan Kerja (TPAK) dengan visualisasi data."""
    # Get all TPAK data sorted by year
    all_tpak_data = list(KetenagakerjaanTPAK.objects.all().order_by('year'))
    
    # Get latest data for summary cards
    latest_data = all_tpak_data[-1] if all_tpak_data else None
    previous_data = all_tpak_data[-2] if len(all_tpak_data) >= 2 else None
    
    # Calculate changes
    total_change = None
    laki_laki_change = None
    perempuan_change = None
    
    if latest_data and previous_data:
        if latest_data.total is not None and previous_data.total is not None:
            total_change = float(latest_data.total) - float(previous_data.total)
        if latest_data.laki_laki is not None and previous_data.laki_laki is not None:
            laki_laki_change = float(latest_data.laki_laki) - float(previous_data.laki_laki)
        if latest_data.perempuan is not None and previous_data.perempuan is not None:
            perempuan_change = float(latest_data.perempuan) - float(previous_data.perempuan)
    
    # Get all years
    all_years = sorted(set([d.year for d in all_tpak_data]))
    latest_year = max(all_years) if all_years else None
    
    context = {
        'tpak_data': all_tpak_data,
        'latest_data': latest_data,
        'previous_data': previous_data,
        'total_change': total_change,
        'laki_laki_change': laki_laki_change,
        'perempuan_change': perempuan_change,
        'all_years': all_years,
        'latest_year': latest_year,
        'page_title': 'Tingkat Partisipasi Angkatan Kerja (TPAK)',
    }
    
    return render(request, 'dashboard/indikator/ketenagakerjaan_tpak.html', context)

# ======= Views untuk Indikator IPM Individual =======
def ipm_uhh_sp(request):
    """Merender halaman IPM Usia Harapan Hidup saat Lahir (UHH SP) dengan visualisasi data."""
    # Helper function to get all historical data for a sub-category
    def get_subcategory_data(model_class, location_name_keywords):
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            location_upper = data.location_name.upper().strip()
            if any(keyword in location_upper for keyword in location_name_keywords):
                filtered_data.append(data)
        if filtered_data:
            filtered_data.sort(key=lambda x: x.year)
            return filtered_data
        return []
    
    # Get all historical data for UHH SP (Kota Surabaya dan Jawa Timur)
    surabaya_data = get_subcategory_data(IPM_UHH_SP, ['SURABAYA'])
    jatim_data = get_subcategory_data(IPM_UHH_SP, ['JAWA TIMUR', 'JATIM'])
    
    # Get latest and previous data for summary cards (Surabaya)
    latest_data = surabaya_data[-1] if surabaya_data else None
    previous_data = surabaya_data[-2] if len(surabaya_data) >= 2 else None
    
    # Get latest and previous data for Jawa Timur
    latest_jatim = jatim_data[-1] if jatim_data else None
    previous_jatim = jatim_data[-2] if len(jatim_data) >= 2 else None
    
    # Calculate change
    change = None
    if latest_data and previous_data:
        if latest_data.value is not None and previous_data.value is not None:
            change = float(latest_data.value) - float(previous_data.value)
    
    jatim_change = None
    if latest_jatim and previous_jatim:
        if latest_jatim.value is not None and previous_jatim.value is not None:
            jatim_change = float(latest_jatim.value) - float(previous_jatim.value)
    
    context = {
        'data_list': surabaya_data,
        'jatim_data': jatim_data,
        'latest_data': latest_data,
        'previous_data': previous_data,
        'latest_jatim': latest_jatim,
        'previous_jatim': previous_jatim,
        'change': change,
        'jatim_change': jatim_change,
        'page_title': 'IPM - Usia Harapan Hidup saat Lahir (UHH SP)',
    }
    
    return render(request, 'dashboard/indikator/ipm_uhh_sp.html', context)

def ipm_hls(request):
    """Merender halaman IPM Harapan Lama Sekolah (HLS) dengan visualisasi data."""
    def get_subcategory_data(model_class, location_name_keywords):
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            location_upper = data.location_name.upper().strip()
            if any(keyword in location_upper for keyword in location_name_keywords):
                filtered_data.append(data)
        if filtered_data:
            filtered_data.sort(key=lambda x: x.year)
            return filtered_data
        return []
    
    surabaya_data = get_subcategory_data(IPM_HLS, ['SURABAYA'])
    jatim_data = get_subcategory_data(IPM_HLS, ['JAWA TIMUR', 'JATIM'])
    
    latest_data = surabaya_data[-1] if surabaya_data else None
    previous_data = surabaya_data[-2] if len(surabaya_data) >= 2 else None
    
    latest_jatim = jatim_data[-1] if jatim_data else None
    previous_jatim = jatim_data[-2] if len(jatim_data) >= 2 else None
    
    change = None
    if latest_data and previous_data:
        if latest_data.value is not None and previous_data.value is not None:
            change = float(latest_data.value) - float(previous_data.value)
    
    jatim_change = None
    if latest_jatim and previous_jatim:
        if latest_jatim.value is not None and previous_jatim.value is not None:
            jatim_change = float(latest_jatim.value) - float(previous_jatim.value)
    
    context = {
        'data_list': surabaya_data,
        'jatim_data': jatim_data,
        'latest_data': latest_data,
        'previous_data': previous_data,
        'latest_jatim': latest_jatim,
        'previous_jatim': previous_jatim,
        'change': change,
        'jatim_change': jatim_change,
        'page_title': 'IPM - Harapan Lama Sekolah (HLS)',
    }
    
    return render(request, 'dashboard/indikator/ipm_hls.html', context)

def ipm_rls(request):
    """Merender halaman IPM Rata-rata Lama Sekolah (RLS) dengan visualisasi data."""
    def get_subcategory_data(model_class, location_name_keywords):
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            location_upper = data.location_name.upper().strip()
            if any(keyword in location_upper for keyword in location_name_keywords):
                filtered_data.append(data)
        if filtered_data:
            filtered_data.sort(key=lambda x: x.year)
            return filtered_data
        return []
    
    surabaya_data = get_subcategory_data(IPM_RLS, ['SURABAYA'])
    jatim_data = get_subcategory_data(IPM_RLS, ['JAWA TIMUR', 'JATIM'])
    
    latest_data = surabaya_data[-1] if surabaya_data else None
    previous_data = surabaya_data[-2] if len(surabaya_data) >= 2 else None
    
    latest_jatim = jatim_data[-1] if jatim_data else None
    previous_jatim = jatim_data[-2] if len(jatim_data) >= 2 else None
    
    change = None
    if latest_data and previous_data:
        if latest_data.value is not None and previous_data.value is not None:
            change = float(latest_data.value) - float(previous_data.value)
    
    jatim_change = None
    if latest_jatim and previous_jatim:
        if latest_jatim.value is not None and previous_jatim.value is not None:
            jatim_change = float(latest_jatim.value) - float(previous_jatim.value)
    
    context = {
        'data_list': surabaya_data,
        'jatim_data': jatim_data,
        'latest_data': latest_data,
        'previous_data': previous_data,
        'latest_jatim': latest_jatim,
        'previous_jatim': previous_jatim,
        'change': change,
        'jatim_change': jatim_change,
        'page_title': 'IPM - Rata-rata Lama Sekolah (RLS)',
    }
    
    return render(request, 'dashboard/indikator/ipm_rls.html', context)

def ipm_pengeluaran_per_kapita(request):
    """Merender halaman IPM Pengeluaran per Kapita dengan visualisasi data."""
    def get_subcategory_data(model_class, location_name_keywords):
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            location_upper = data.location_name.upper().strip()
            if any(keyword in location_upper for keyword in location_name_keywords):
                filtered_data.append(data)
        if filtered_data:
            filtered_data.sort(key=lambda x: x.year)
            return filtered_data
        return []
    
    surabaya_data = get_subcategory_data(IPM_PengeluaranPerKapita, ['SURABAYA'])
    jatim_data = get_subcategory_data(IPM_PengeluaranPerKapita, ['JAWA TIMUR', 'JATIM'])
    
    latest_data = surabaya_data[-1] if surabaya_data else None
    previous_data = surabaya_data[-2] if len(surabaya_data) >= 2 else None
    
    latest_jatim = jatim_data[-1] if jatim_data else None
    previous_jatim = jatim_data[-2] if len(jatim_data) >= 2 else None
    
    change = None
    if latest_data and previous_data:
        if latest_data.value is not None and previous_data.value is not None:
            change = float(latest_data.value) - float(previous_data.value)
    
    jatim_change = None
    if latest_jatim and previous_jatim:
        if latest_jatim.value is not None and previous_jatim.value is not None:
            jatim_change = float(latest_jatim.value) - float(previous_jatim.value)
    
    context = {
        'data_list': surabaya_data,
        'jatim_data': jatim_data,
        'latest_data': latest_data,
        'previous_data': previous_data,
        'latest_jatim': latest_jatim,
        'previous_jatim': previous_jatim,
        'change': change,
        'jatim_change': jatim_change,
        'page_title': 'IPM - Pengeluaran per Kapita',
    }
    
    return render(request, 'dashboard/indikator/ipm_pengeluaran_per_kapita.html', context)

def ipm_indeks_kesehatan(request):
    """Merender halaman IPM Indeks Kesehatan dengan visualisasi data."""
    def get_subcategory_data(model_class, location_name_keywords):
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            location_upper = data.location_name.upper().strip()
            if any(keyword in location_upper for keyword in location_name_keywords):
                filtered_data.append(data)
        if filtered_data:
            filtered_data.sort(key=lambda x: x.year)
            return filtered_data
        return []
    
    surabaya_data = get_subcategory_data(IPM_IndeksKesehatan, ['SURABAYA'])
    jatim_data = get_subcategory_data(IPM_IndeksKesehatan, ['JAWA TIMUR', 'JATIM'])
    
    latest_data = surabaya_data[-1] if surabaya_data else None
    previous_data = surabaya_data[-2] if len(surabaya_data) >= 2 else None
    
    latest_jatim = jatim_data[-1] if jatim_data else None
    previous_jatim = jatim_data[-2] if len(jatim_data) >= 2 else None
    
    change = None
    if latest_data and previous_data:
        if latest_data.value is not None and previous_data.value is not None:
            change = float(latest_data.value) - float(previous_data.value)
    
    jatim_change = None
    if latest_jatim and previous_jatim:
        if latest_jatim.value is not None and previous_jatim.value is not None:
            jatim_change = float(latest_jatim.value) - float(previous_jatim.value)
    
    context = {
        'data_list': surabaya_data,
        'jatim_data': jatim_data,
        'latest_data': latest_data,
        'previous_data': previous_data,
        'latest_jatim': latest_jatim,
        'previous_jatim': previous_jatim,
        'change': change,
        'jatim_change': jatim_change,
        'page_title': 'IPM - Indeks Kesehatan',
    }
    
    return render(request, 'dashboard/indikator/ipm_indeks_kesehatan.html', context)

def ipm_indeks_hidup_layak(request):
    """Merender halaman IPM Indeks Hidup Layak dengan visualisasi data."""
    def get_subcategory_data(model_class, location_name_keywords):
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            location_upper = data.location_name.upper().strip()
            if any(keyword in location_upper for keyword in location_name_keywords):
                filtered_data.append(data)
        if filtered_data:
            filtered_data.sort(key=lambda x: x.year)
            return filtered_data
        return []
    
    surabaya_data = get_subcategory_data(IPM_IndeksHidupLayak, ['SURABAYA'])
    jatim_data = get_subcategory_data(IPM_IndeksHidupLayak, ['JAWA TIMUR', 'JATIM'])
    
    latest_data = surabaya_data[-1] if surabaya_data else None
    previous_data = surabaya_data[-2] if len(surabaya_data) >= 2 else None
    
    latest_jatim = jatim_data[-1] if jatim_data else None
    previous_jatim = jatim_data[-2] if len(jatim_data) >= 2 else None
    
    change = None
    if latest_data and previous_data:
        if latest_data.value is not None and previous_data.value is not None:
            change = float(latest_data.value) - float(previous_data.value)
    
    jatim_change = None
    if latest_jatim and previous_jatim:
        if latest_jatim.value is not None and previous_jatim.value is not None:
            jatim_change = float(latest_jatim.value) - float(previous_jatim.value)
    
    context = {
        'data_list': surabaya_data,
        'jatim_data': jatim_data,
        'latest_data': latest_data,
        'previous_data': previous_data,
        'latest_jatim': latest_jatim,
        'previous_jatim': previous_jatim,
        'change': change,
        'jatim_change': jatim_change,
        'page_title': 'IPM - Indeks Hidup Layak',
    }
    
    return render(request, 'dashboard/indikator/ipm_indeks_hidup_layak.html', context)

def ipm_indeks_pendidikan(request):
    """Merender halaman IPM Indeks Pendidikan dengan visualisasi data."""
    def get_subcategory_data(model_class, location_name_keywords):
        all_data = list(model_class.objects.all())
        filtered_data = []
        for data in all_data:
            location_upper = data.location_name.upper().strip()
            if any(keyword in location_upper for keyword in location_name_keywords):
                filtered_data.append(data)
        if filtered_data:
            filtered_data.sort(key=lambda x: x.year)
            return filtered_data
        return []
    
    surabaya_data = get_subcategory_data(IPM_IndeksPendidikan, ['SURABAYA'])
    jatim_data = get_subcategory_data(IPM_IndeksPendidikan, ['JAWA TIMUR', 'JATIM'])
    
    latest_data = surabaya_data[-1] if surabaya_data else None
    previous_data = surabaya_data[-2] if len(surabaya_data) >= 2 else None
    
    latest_jatim = jatim_data[-1] if jatim_data else None
    previous_jatim = jatim_data[-2] if len(jatim_data) >= 2 else None
    
    change = None
    if latest_data and previous_data:
        if latest_data.value is not None and previous_data.value is not None:
            change = float(latest_data.value) - float(previous_data.value)
    
    jatim_change = None
    if latest_jatim and previous_jatim:
        if latest_jatim.value is not None and previous_jatim.value is not None:
            jatim_change = float(latest_jatim.value) - float(previous_jatim.value)
    
    context = {
        'data_list': surabaya_data,
        'jatim_data': jatim_data,
        'latest_data': latest_data,
        'previous_data': previous_data,
        'latest_jatim': latest_jatim,
        'previous_jatim': previous_jatim,
        'change': change,
        'jatim_change': jatim_change,
        'page_title': 'IPM - Indeks Pendidikan',
    }
    
    return render(request, 'dashboard/indikator/ipm_indeks_pendidikan.html', context)

def pdrb_pengeluaran(request):
    """Merender halaman PDRB Pengeluaran dengan visualisasi data."""
    # ========== TAHUNAN DATA ==========
    # ADHB (Annual) - TAMPILKAN SEMUA KATEGORI (tidak hanya yang mengandung PDRB)
    all_adhb_data = list(PDRBPengeluaranADHB.objects.all())
    adhb_data = all_adhb_data  # Tampilkan semua kategori pengeluaran
    adhb_data.sort(key=lambda x: x.year)
    
    # ADHK (Annual) - TAMPILKAN SEMUA KATEGORI
    all_adhk_data = list(PDRBPengeluaranADHK.objects.all())
    adhk_data = all_adhk_data  # Tampilkan semua kategori pengeluaran
    adhk_data.sort(key=lambda x: x.year)
    
    # Distribusi (Annual) - TAMPILKAN SEMUA KATEGORI
    all_distribusi_data = list(PDRBPengeluaranDistribusi.objects.all())
    distribusi_data = all_distribusi_data  # Tampilkan semua kategori pengeluaran
    distribusi_data.sort(key=lambda x: (x.year, x.expenditure_category))
    
    # Laju PDRB (Annual) - TAMPILKAN SEMUA KATEGORI
    all_laju_data = list(PDRBPengeluaranLajuPDRB.objects.all())
    laju_data = all_laju_data  # Tampilkan semua kategori pengeluaran
    laju_data.sort(key=lambda x: x.year)
    
    # ========== TRWULANAN DATA ==========
    # ADHB Triwulanan - TAMPILKAN SEMUA KATEGORI
    all_adhb_triwulanan = list(PDRBPengeluaranADHBTriwulanan.objects.all())
    adhb_triwulanan = all_adhb_triwulanan  # Tampilkan semua kategori pengeluaran
    adhb_triwulanan.sort(key=lambda x: (x.year, x.quarter))
    
    # ADHK Triwulanan - TAMPILKAN SEMUA KATEGORI
    all_adhk_triwulanan = list(PDRBPengeluaranADHKTriwulanan.objects.all())
    adhk_triwulanan = all_adhk_triwulanan  # Tampilkan semua kategori pengeluaran
    adhk_triwulanan.sort(key=lambda x: (x.year, x.quarter))
    
    # Distribusi Triwulanan - TAMPILKAN SEMUA KATEGORI
    all_distribusi_triwulanan = list(PDRBPengeluaranDistribusiTriwulanan.objects.all())
    distribusi_triwulanan = all_distribusi_triwulanan  # Tampilkan semua kategori pengeluaran
    distribusi_triwulanan.sort(key=lambda x: (x.year, x.quarter, x.expenditure_category))
    
    # Laju Q-to-Q - TAMPILKAN SEMUA KATEGORI
    all_laju_qtoq = list(PDRBPengeluaranLajuQtoQ.objects.all())
    laju_qtoq = all_laju_qtoq  # Tampilkan semua kategori pengeluaran
    laju_qtoq.sort(key=lambda x: (x.year, x.quarter))
    
    # Laju Y-to-Y - TAMPILKAN SEMUA KATEGORI
    all_laju_ytoy = list(PDRBPengeluaranLajuYtoY.objects.all())
    laju_ytoy = all_laju_ytoy  # Tampilkan semua kategori pengeluaran
    laju_ytoy.sort(key=lambda x: (x.year, x.quarter))
    
    # Laju C-to-C - TAMPILKAN SEMUA KATEGORI
    all_laju_ctoc = list(PDRBPengeluaranLajuCtoC.objects.all())
    laju_ctoc = all_laju_ctoc  # Tampilkan semua kategori pengeluaran
    laju_ctoc.sort(key=lambda x: (x.year, x.quarter))
    
    # Get all years from ADHB data
    all_years = sorted(set([d.year for d in adhb_data]))
    latest_year = max(all_years) if all_years else None
    
    # Helper function to group data by category
    def group_by_category(data_list, is_quarterly=False):
        result = {}
        for data in data_list:
            if data.value is not None:
                if data.expenditure_category not in result:
                    result[data.expenditure_category] = []
                item = {
                    'year': data.year,
                    'value': float(data.value),
                    'preliminary_flag': data.preliminary_flag
                }
                if is_quarterly:
                    item['quarter'] = data.quarter
                result[data.expenditure_category].append(item)
        
        # Sort each category's data
        for category in result:
            if is_quarterly:
                result[category].sort(key=lambda x: (x['year'], x['quarter']))
            else:
                result[category].sort(key=lambda x: x['year'])
        return result
    
    # Group all data by category
    adhb_by_category = group_by_category(adhb_data)
    adhk_by_category = group_by_category(adhk_data)
    distribusi_by_category = group_by_category(distribusi_data)
    laju_by_category = group_by_category(laju_data)
    
    # Triwulanan data grouped by category
    adhb_triwulanan_by_category = group_by_category(adhb_triwulanan, is_quarterly=True)
    adhk_triwulanan_by_category = group_by_category(adhk_triwulanan, is_quarterly=True)
    distribusi_triwulanan_by_category = group_by_category(distribusi_triwulanan, is_quarterly=True)
    laju_qtoq_by_category = group_by_category(laju_qtoq, is_quarterly=True)
    laju_ytoy_by_category = group_by_category(laju_ytoy, is_quarterly=True)
    laju_ctoc_by_category = group_by_category(laju_ctoc, is_quarterly=True)
    
    # Get latest year distribusi data (ALL categories)
    latest_distribusi = []
    if latest_year:
        for data in distribusi_data:
            if data.year == latest_year and data.value is not None:
                latest_distribusi.append({
                    'name': data.expenditure_category,
                    'value': float(data.value)
                })
    
    # Get latest data from each sheet for carousel (last row/category from each sheet)
    latest_by_sheet = {}
    
    # ADHB - Get last category's latest year data
    if adhb_by_category:
        categories = list(adhb_by_category.keys())
        if categories:
            last_category = categories[-1]
            if adhb_by_category[last_category]:
                latest_by_sheet['ADHB'] = {
                    'category': last_category,
                    'data': adhb_by_category[last_category][-1] if adhb_by_category[last_category] else None,
                    'all_data': adhb_by_category[last_category]
                }
    
    # ADHK - Get last category's latest year data
    if adhk_by_category:
        categories = list(adhk_by_category.keys())
        if categories:
            last_category = categories[-1]
            if adhk_by_category[last_category]:
                latest_by_sheet['ADHK'] = {
                    'category': last_category,
                    'data': adhk_by_category[last_category][-1] if adhk_by_category[last_category] else None,
                    'all_data': adhk_by_category[last_category]
                }
    
    # Distribusi - Get last category's latest year data
    if distribusi_by_category:
        categories = list(distribusi_by_category.keys())
        if categories:
            last_category = categories[-1]
            if distribusi_by_category[last_category]:
                latest_by_sheet['Distribusi'] = {
                    'category': last_category,
                    'data': distribusi_by_category[last_category][-1] if distribusi_by_category[last_category] else None,
                    'all_data': distribusi_by_category[last_category]
                }
    
    # Laju PDRB - Get last category's latest year data
    if laju_by_category:
        categories = list(laju_by_category.keys())
        if categories:
            last_category = categories[-1]
            if laju_by_category[last_category]:
                latest_by_sheet['Laju PDRB'] = {
                    'category': last_category,
                    'data': laju_by_category[last_category][-1] if laju_by_category[last_category] else None,
                    'all_data': laju_by_category[last_category]
                }
    
    # ADHB Triwulanan - Get last category's latest data
    if adhb_triwulanan_by_category:
        categories = list(adhb_triwulanan_by_category.keys())
        if categories:
            last_category = categories[-1]
            if adhb_triwulanan_by_category[last_category]:
                latest_by_sheet['ADHB Triwulanan'] = {
                    'category': last_category,
                    'data': adhb_triwulanan_by_category[last_category][-1] if adhb_triwulanan_by_category[last_category] else None,
                    'all_data': adhb_triwulanan_by_category[last_category]
                }
    
    # ADHK Triwulanan - Get last category's latest data
    if adhk_triwulanan_by_category:
        categories = list(adhk_triwulanan_by_category.keys())
        if categories:
            last_category = categories[-1]
            if adhk_triwulanan_by_category[last_category]:
                latest_by_sheet['ADHK Triwulanan'] = {
                    'category': last_category,
                    'data': adhk_triwulanan_by_category[last_category][-1] if adhk_triwulanan_by_category[last_category] else None,
                    'all_data': adhk_triwulanan_by_category[last_category]
                }
    
    # Distribusi Triwulanan - Get last category's latest data
    if distribusi_triwulanan_by_category:
        categories = list(distribusi_triwulanan_by_category.keys())
        if categories:
            last_category = categories[-1]
            if distribusi_triwulanan_by_category[last_category]:
                latest_by_sheet['Distribusi Triwulanan'] = {
                    'category': last_category,
                    'data': distribusi_triwulanan_by_category[last_category][-1] if distribusi_triwulanan_by_category[last_category] else None,
                    'all_data': distribusi_triwulanan_by_category[last_category]
                }
    
    # Laju Q-to-Q - Get last category's latest data
    if laju_qtoq_by_category:
        categories = list(laju_qtoq_by_category.keys())
        if categories:
            last_category = categories[-1]
            if laju_qtoq_by_category[last_category]:
                latest_by_sheet['Laju Q-to-Q'] = {
                    'category': last_category,
                    'data': laju_qtoq_by_category[last_category][-1] if laju_qtoq_by_category[last_category] else None,
                    'all_data': laju_qtoq_by_category[last_category]
                }
    
    # Laju Y-to-Y - Get last category's latest data
    if laju_ytoy_by_category:
        categories = list(laju_ytoy_by_category.keys())
        if categories:
            last_category = categories[-1]
            if laju_ytoy_by_category[last_category]:
                latest_by_sheet['Laju Y-to-Y'] = {
                    'category': last_category,
                    'data': laju_ytoy_by_category[last_category][-1] if laju_ytoy_by_category[last_category] else None,
                    'all_data': laju_ytoy_by_category[last_category]
                }
    
    # Laju C-to-C - Get last category's latest data
    if laju_ctoc_by_category:
        categories = list(laju_ctoc_by_category.keys())
        if categories:
            last_category = categories[-1]
            if laju_ctoc_by_category[last_category]:
                latest_by_sheet['Laju C-to-C'] = {
                    'category': last_category,
                    'data': laju_ctoc_by_category[last_category][-1] if laju_ctoc_by_category[last_category] else None,
                    'all_data': laju_ctoc_by_category[last_category]
                }
    
    context = {
        # Annual data
        'adhb_data': adhb_data,
        'adhk_data': adhk_data,
        'distribusi_data': distribusi_data,
        'laju_data': laju_data,
        'adhb_by_category': adhb_by_category,
        'adhk_by_category': adhk_by_category,
        'distribusi_by_category': distribusi_by_category,
        'laju_by_category': laju_by_category,
        
        # Triwulanan data
        'adhb_triwulanan': adhb_triwulanan,
        'adhk_triwulanan': adhk_triwulanan,
        'distribusi_triwulanan': distribusi_triwulanan,
        'laju_qtoq': laju_qtoq,
        'laju_ytoy': laju_ytoy,
        'laju_ctoc': laju_ctoc,
        'adhb_triwulanan_by_category': adhb_triwulanan_by_category,
        'adhk_triwulanan_by_category': adhk_triwulanan_by_category,
        'distribusi_triwulanan_by_category': distribusi_triwulanan_by_category,
        'laju_qtoq_by_category': laju_qtoq_by_category,
        'laju_ytoy_by_category': laju_ytoy_by_category,
        'laju_ctoc_by_category': laju_ctoc_by_category,
        
        # Latest data for carousel
        'latest_by_sheet': latest_by_sheet,
        'latest_distribusi': latest_distribusi,
        'all_years': all_years,
        'latest_year': latest_year,
        'page_title': 'PDRB Pengeluaran',
    }
    
    return render(request, 'dashboard/indikator/pdrb_pengeluaran.html', context)

def pdrb_lapangan_usaha(request):
    """Merender halaman PDRB Lapangan Usaha dengan visualisasi data."""
    # ========== TAHUNAN DATA ==========
    # ADHB (Annual) - TAMPILKAN SEMUA KATEGORI
    all_adhb_data = list(PDRBLapanganUsahaADHB.objects.all())
    adhb_data = all_adhb_data
    adhb_data.sort(key=lambda x: x.year)
    
    # ADHK (Annual) - TAMPILKAN SEMUA KATEGORI
    all_adhk_data = list(PDRBLapanganUsahaADHK.objects.all())
    adhk_data = all_adhk_data
    adhk_data.sort(key=lambda x: x.year)
    
    # Distribusi (Annual) - TAMPILKAN SEMUA KATEGORI
    all_distribusi_data = list(PDRBLapanganUsahaDistribusi.objects.all())
    distribusi_data = all_distribusi_data
    distribusi_data.sort(key=lambda x: (x.year, x.industry_category))
    
    # Laju PDRB (Annual) - TAMPILKAN SEMUA KATEGORI
    all_laju_data = list(PDRBLapanganUsahaLajuPDRB.objects.all())
    laju_data = all_laju_data
    laju_data.sort(key=lambda x: x.year)
    
    # Laju Implisit (Annual) - TAMPILKAN SEMUA KATEGORI
    all_laju_implisit_data = list(PDRBLapanganUsahaLajuImplisit.objects.all())
    laju_implisit_data = all_laju_implisit_data
    laju_implisit_data.sort(key=lambda x: x.year)
    
    # ========== TRWULANAN DATA ==========
    # ADHB Triwulanan - TAMPILKAN SEMUA KATEGORI
    all_adhb_triwulanan = list(PDRBLapanganUsahaADHBTriwulanan.objects.all())
    adhb_triwulanan = all_adhb_triwulanan
    adhb_triwulanan.sort(key=lambda x: (x.year, x.quarter))
    
    # ADHK Triwulanan - TAMPILKAN SEMUA KATEGORI
    all_adhk_triwulanan = list(PDRBLapanganUsahaADHKTriwulanan.objects.all())
    adhk_triwulanan = all_adhk_triwulanan
    adhk_triwulanan.sort(key=lambda x: (x.year, x.quarter))
    
    # Distribusi Triwulanan - TAMPILKAN SEMUA KATEGORI
    all_distribusi_triwulanan = list(PDRBLapanganUsahaDistribusiTriwulanan.objects.all())
    distribusi_triwulanan = all_distribusi_triwulanan
    distribusi_triwulanan.sort(key=lambda x: (x.year, x.quarter, x.industry_category))
    
    # Laju Q-to-Q - TAMPILKAN SEMUA KATEGORI
    all_laju_qtoq = list(PDRBLapanganUsahaLajuQtoQ.objects.all())
    laju_qtoq = all_laju_qtoq
    laju_qtoq.sort(key=lambda x: (x.year, x.quarter))
    
    # Laju Y-to-Y - TAMPILKAN SEMUA KATEGORI
    all_laju_ytoy = list(PDRBLapanganUsahaLajuYtoY.objects.all())
    laju_ytoy = all_laju_ytoy
    laju_ytoy.sort(key=lambda x: (x.year, x.quarter))
    
    # Laju C-to-C - TAMPILKAN SEMUA KATEGORI
    all_laju_ctoc = list(PDRBLapanganUsahaLajuCtoC.objects.all())
    laju_ctoc = all_laju_ctoc
    laju_ctoc.sort(key=lambda x: (x.year, x.quarter))
    
    # Get all years from ADHB data
    all_years = sorted(set([d.year for d in adhb_data]))
    latest_year = max(all_years) if all_years else None
    
    # Helper function to group data by category
    def group_by_category(data_list, is_quarterly=False):
        result = {}
        for data in data_list:
            if data.value is not None:
                category = data.industry_category
                if category not in result:
                    result[category] = []
                item = {
                    'year': data.year,
                    'value': float(data.value),
                    'preliminary_flag': data.preliminary_flag
                }
                if is_quarterly:
                    item['quarter'] = data.quarter
                result[category].append(item)
        
        # Sort each category's data
        for category in result:
            if is_quarterly:
                result[category].sort(key=lambda x: (x['year'], x['quarter']))
            else:
                result[category].sort(key=lambda x: x['year'])
        return result
    
    # Group all data by category
    adhb_by_category = group_by_category(adhb_data)
    adhk_by_category = group_by_category(adhk_data)
    distribusi_by_category = group_by_category(distribusi_data)
    laju_by_category = group_by_category(laju_data)
    laju_implisit_by_category = group_by_category(laju_implisit_data)
    
    # Triwulanan data grouped by category
    adhb_triwulanan_by_category = group_by_category(adhb_triwulanan, is_quarterly=True)
    adhk_triwulanan_by_category = group_by_category(adhk_triwulanan, is_quarterly=True)
    distribusi_triwulanan_by_category = group_by_category(distribusi_triwulanan, is_quarterly=True)
    laju_qtoq_by_category = group_by_category(laju_qtoq, is_quarterly=True)
    laju_ytoy_by_category = group_by_category(laju_ytoy, is_quarterly=True)
    laju_ctoc_by_category = group_by_category(laju_ctoc, is_quarterly=True)
    
    # Get latest year distribusi data (ALL categories)
    latest_distribusi = []
    if latest_year:
        for data in distribusi_data:
            if data.year == latest_year and data.value is not None:
                latest_distribusi.append({
                    'name': data.industry_category,
                    'value': float(data.value)
                })
    
    # Get latest data from each sheet for carousel
    latest_by_sheet = {}
    
    # ADHB - Get last category's latest year data
    if adhb_by_category:
        categories = list(adhb_by_category.keys())
        if categories:
            last_category = categories[-1]
            if adhb_by_category[last_category]:
                latest_by_sheet['ADHB'] = {
                    'category': last_category,
                    'data': adhb_by_category[last_category][-1] if adhb_by_category[last_category] else None,
                    'all_data': adhb_by_category[last_category]
                }
    
    # ADHK - Get last category's latest year data
    if adhk_by_category:
        categories = list(adhk_by_category.keys())
        if categories:
            last_category = categories[-1]
            if adhk_by_category[last_category]:
                latest_by_sheet['ADHK'] = {
                    'category': last_category,
                    'data': adhk_by_category[last_category][-1] if adhk_by_category[last_category] else None,
                    'all_data': adhk_by_category[last_category]
                }
    
    # Distribusi - Get last category's latest year data
    if distribusi_by_category:
        categories = list(distribusi_by_category.keys())
        if categories:
            last_category = categories[-1]
            if distribusi_by_category[last_category]:
                latest_by_sheet['Distribusi'] = {
                    'category': last_category,
                    'data': distribusi_by_category[last_category][-1] if distribusi_by_category[last_category] else None,
                    'all_data': distribusi_by_category[last_category]
                }
    
    # Laju PDRB - Get last category's latest year data
    if laju_by_category:
        categories = list(laju_by_category.keys())
        if categories:
            last_category = categories[-1]
            if laju_by_category[last_category]:
                latest_by_sheet['Laju PDRB'] = {
                    'category': last_category,
                    'data': laju_by_category[last_category][-1] if laju_by_category[last_category] else None,
                    'all_data': laju_by_category[last_category]
                }
    
    # Laju Implisit - Get last category's latest year data
    if laju_implisit_by_category:
        categories = list(laju_implisit_by_category.keys())
        if categories:
            last_category = categories[-1]
            if laju_implisit_by_category[last_category]:
                latest_by_sheet['Laju Implisit'] = {
                    'category': last_category,
                    'data': laju_implisit_by_category[last_category][-1] if laju_implisit_by_category[last_category] else None,
                    'all_data': laju_implisit_by_category[last_category]
                }
    
    # ADHB Triwulanan - Get last category's latest data
    if adhb_triwulanan_by_category:
        categories = list(adhb_triwulanan_by_category.keys())
        if categories:
            last_category = categories[-1]
            if adhb_triwulanan_by_category[last_category]:
                latest_by_sheet['ADHB Triwulanan'] = {
                    'category': last_category,
                    'data': adhb_triwulanan_by_category[last_category][-1] if adhb_triwulanan_by_category[last_category] else None,
                    'all_data': adhb_triwulanan_by_category[last_category]
                }
    
    # ADHK Triwulanan - Get last category's latest data
    if adhk_triwulanan_by_category:
        categories = list(adhk_triwulanan_by_category.keys())
        if categories:
            last_category = categories[-1]
            if adhk_triwulanan_by_category[last_category]:
                latest_by_sheet['ADHK Triwulanan'] = {
                    'category': last_category,
                    'data': adhk_triwulanan_by_category[last_category][-1] if adhk_triwulanan_by_category[last_category] else None,
                    'all_data': adhk_triwulanan_by_category[last_category]
                }
    
    # Distribusi Triwulanan - Get last category's latest data
    if distribusi_triwulanan_by_category:
        categories = list(distribusi_triwulanan_by_category.keys())
        if categories:
            last_category = categories[-1]
            if distribusi_triwulanan_by_category[last_category]:
                latest_by_sheet['Distribusi Triwulanan'] = {
                    'category': last_category,
                    'data': distribusi_triwulanan_by_category[last_category][-1] if distribusi_triwulanan_by_category[last_category] else None,
                    'all_data': distribusi_triwulanan_by_category[last_category]
                }
    
    # Laju Q-to-Q - Get last category's latest data
    if laju_qtoq_by_category:
        categories = list(laju_qtoq_by_category.keys())
        if categories:
            last_category = categories[-1]
            if laju_qtoq_by_category[last_category]:
                latest_by_sheet['Laju Q-to-Q'] = {
                    'category': last_category,
                    'data': laju_qtoq_by_category[last_category][-1] if laju_qtoq_by_category[last_category] else None,
                    'all_data': laju_qtoq_by_category[last_category]
                }
    
    # Laju Y-to-Y - Get last category's latest data
    if laju_ytoy_by_category:
        categories = list(laju_ytoy_by_category.keys())
        if categories:
            last_category = categories[-1]
            if laju_ytoy_by_category[last_category]:
                latest_by_sheet['Laju Y-to-Y'] = {
                    'category': last_category,
                    'data': laju_ytoy_by_category[last_category][-1] if laju_ytoy_by_category[last_category] else None,
                    'all_data': laju_ytoy_by_category[last_category]
                }
    
    # Laju C-to-C - Get last category's latest data
    if laju_ctoc_by_category:
        categories = list(laju_ctoc_by_category.keys())
        if categories:
            last_category = categories[-1]
            if laju_ctoc_by_category[last_category]:
                latest_by_sheet['Laju C-to-C'] = {
                    'category': last_category,
                    'data': laju_ctoc_by_category[last_category][-1] if laju_ctoc_by_category[last_category] else None,
                    'all_data': laju_ctoc_by_category[last_category]
                }
    
    context = {
        # Annual data
        'adhb_data': adhb_data,
        'adhk_data': adhk_data,
        'distribusi_data': distribusi_data,
        'laju_data': laju_data,
        'laju_implisit_data': laju_implisit_data,
        'adhb_by_category': adhb_by_category,
        'adhk_by_category': adhk_by_category,
        'distribusi_by_category': distribusi_by_category,
        'laju_by_category': laju_by_category,
        'laju_implisit_by_category': laju_implisit_by_category,
        
        # Triwulanan data
        'adhb_triwulanan': adhb_triwulanan,
        'adhk_triwulanan': adhk_triwulanan,
        'distribusi_triwulanan': distribusi_triwulanan,
        'laju_qtoq': laju_qtoq,
        'laju_ytoy': laju_ytoy,
        'laju_ctoc': laju_ctoc,
        'adhb_triwulanan_by_category': adhb_triwulanan_by_category,
        'adhk_triwulanan_by_category': adhk_triwulanan_by_category,
        'distribusi_triwulanan_by_category': distribusi_triwulanan_by_category,
        'laju_qtoq_by_category': laju_qtoq_by_category,
        'laju_ytoy_by_category': laju_ytoy_by_category,
        'laju_ctoc_by_category': laju_ctoc_by_category,
        
        # Latest data for carousel
        'latest_by_sheet': latest_by_sheet,
        'latest_distribusi': latest_distribusi,
        'all_years': all_years,
        'latest_year': latest_year,
        'page_title': 'PDRB Lapangan Usaha',
    }
    
    return render(request, 'dashboard/indikator/pdrb_lapangan_usaha.html', context)

def inflasi(request):
    """Merender halaman Inflasi dengan visualisasi data."""
    from django.db.models import Max, Min
    
    # Get all inflasi data (general inflation) - sorted chronologically
    all_inflasi_data = list(Inflasi.objects.annotate(month_order=get_month_order()).order_by('year', 'month_order'))
    
    # Get distinct years (sorted descending for filter)
    all_years = sorted(set([d.year for d in all_inflasi_data]), reverse=True)
    
    # Get latest month data for summary cards - use explicit query to get the latest
    latest_inflasi = None
    previous_month_inflasi = None
    previous_year_inflasi = None
    
    # Get the latest data explicitly by ordering descending
    latest_data_query = Inflasi.objects.annotate(month_order=get_month_order()).order_by('-year', '-month_order')
    if latest_data_query.exists():
        latest_inflasi = latest_data_query.first()
        latest_year = latest_inflasi.year
        
        # Get previous month (same year or previous year)
        # If latest is not January, get previous month in same year
        if latest_inflasi.month != 'JANUARI':
            # Get previous month in same year
            month_order_map = {
                'JANUARI': 1, 'FEBRUARI': 2, 'MARET': 3, 'APRIL': 4,
                'MEI': 5, 'JUNI': 6, 'JULI': 7, 'AGUSTUS': 8,
                'SEPTEMBER': 9, 'OKTOBER': 10, 'NOPEMBER': 11, 'DESEMBER': 12
            }
            current_month_order = month_order_map.get(latest_inflasi.month, 0)
            previous_month_order = current_month_order - 1
            previous_month_name = [k for k, v in month_order_map.items() if v == previous_month_order]
            
            if previous_month_name:
                previous_month_inflasi = Inflasi.objects.filter(
                    year=latest_year,
                    month=previous_month_name[0]
                ).first()
        else:
            # Latest is January, previous month is December of previous year
            previous_month_inflasi = Inflasi.objects.filter(
                year=latest_year - 1,
                month='DESEMBER'
            ).first()
        
        # Get same month previous year (for YoY comparison)
        previous_year_inflasi = Inflasi.objects.filter(
            year=latest_year - 1,
            month=latest_inflasi.month
        ).first()
    else:
        latest_year = None
    
    # Get komoditas umum (flag = 1) - untuk filter
    komoditas_umum = InflasiPerKomoditas.objects.filter(
        flag='1'
    ).values('commodity_code', 'commodity_name').distinct().order_by('commodity_name')
    
    # Get available years for komoditas filter
    komoditas_years = sorted(InflasiPerKomoditas.objects.values_list('year', flat=True).distinct())
    
    # Prepare data for summary cards
    # Calculate month-to-month change
    m_to_m_change = None
    if latest_inflasi and previous_month_inflasi:
        if latest_inflasi.bulanan is not None and previous_month_inflasi.bulanan is not None:
            m_to_m_change = float(latest_inflasi.bulanan) - float(previous_month_inflasi.bulanan)
    
    # Calculate year-over-year change
    y_on_y_change = None
    if latest_inflasi and previous_year_inflasi:
        if latest_inflasi.yoy is not None and previous_year_inflasi.yoy is not None:
            y_on_y_change = float(latest_inflasi.yoy) - float(previous_year_inflasi.yoy)
    
    context = {
        'all_inflasi_data': all_inflasi_data,
        'latest_inflasi': latest_inflasi,
        'previous_month_inflasi': previous_month_inflasi,
        'previous_year_inflasi': previous_year_inflasi,
        'm_to_m_change': m_to_m_change,
        'y_on_y_change': y_on_y_change,
        'all_years': all_years,
        'latest_year': latest_year,
        'komoditas_umum': komoditas_umum,
        'komoditas_years': komoditas_years,
        'page_title': 'Inflasi',
    }
    
    return render(request, 'dashboard/indikator/inflasi.html', context)
