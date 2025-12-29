from django.urls import path
from . import views
# from rest_framework.routers import DefaultRouter
from apps.views import NewsViewSet, InpographicViewSet, PublicationViewSet,HumanDevelopmentIndexViewSet 
# router = DefaultRouter()
# router.register(r'news', NewsViewSet, basename='news')
# router.register(r'infographics', InpographicViewSet, basename='infographic')
# router.register(r'publications', PublicationViewSet, basename='publication')
# router.register(r'human-development-index', HumanDevelopmentIndexViewSet, basename='human-development-index')

urlpatterns = [
    
    # path('api/v1/', (router.urls)),
    # Page rendering views
    path('', views.apps, name='index'),
    path('signup/', views.signup_page, name='signup'),
    path('login/', views.login_page, name='login'),
    path('login/form/', views.user_login_form, name='login-form'),
    path('dashboard/', views.dashboard, name='dashboard'),
    path('logout/', views.logout_view, name='logout'),
    path('accounts/google/login/', views.google_login_redirect, name='google_login'),
    path('api/google/signin-callback/', views.google_signin_callback, name='google-signin-callback'),
    path('infographics/', views.infographics, name='infographics'),
    path('infographics/download/<int:infographic_id>/', views.download_infographic, name='download-infographic'),
    path('publications/', views.publications, name='publications'),
    path('publications/download/<str:pub_id>/', views.download_publication, name='download-publication'),
    path('news/', views.news, name='news'),
    path('ipm/', views.ipm, name='ipm'),
    path('indeks-pembangunan-manusia/', views.indeks_pembangunan_manusia, name='indeks-pembangunan-manusia'),
    path('hotel-occupancy/', views.hotel_occupancy, name='hotel-occupancy'),
    path('gini-ratio/', views.gini_ratio, name='gini-ratio'),
    path('kemiskinan/', views.kemiskinan, name='kemiskinan'),
    path('kependudukan/', views.kependudukan, name='kependudukan'),
    path('ketenagakerjaan/', views.ketenagakerjaan, name='ketenagakerjaan'),
    path('ketenagakerjaan-tpt/', views.ketenagakerjaan_tpt, name='ketenagakerjaan-tpt'),
    path('ketenagakerjaan-tpak/', views.ketenagakerjaan_tpak, name='ketenagakerjaan-tpak'),

    # IPM Individual Indicator Pages
    path('ipm-uhh-sp/', views.ipm_uhh_sp, name='ipm-uhh-sp'),
    path('ipm-hls/', views.ipm_hls, name='ipm-hls'),
    path('ipm-rls/', views.ipm_rls, name='ipm-rls'),
    path('ipm-pengeluaran-per-kapita/', views.ipm_pengeluaran_per_kapita, name='ipm-pengeluaran-per-kapita'),
    path('ipm-indeks-kesehatan/', views.ipm_indeks_kesehatan, name='ipm-indeks-kesehatan'),
    path('ipm-indeks-hidup-layak/', views.ipm_indeks_hidup_layak, name='ipm-indeks-hidup-layak'),
    path('ipm-indeks-pendidikan/', views.ipm_indeks_pendidikan, name='ipm-indeks-pendidikan'),
    path('pdrb-pengeluaran/', views.pdrb_pengeluaran, name='pdrb-pengeluaran'),
    path('pdrb-lapangan-usaha/', views.pdrb_lapangan_usaha, name='pdrb-lapangan-usaha'),
    path('inflasi/', views.inflasi, name='inflasi'),

    # URL for contact form submission
    path('contact-us/', views.contact_us, name='contact_us'),

    # API endpoints for authentication
    path('api/register/', views.register_user, name='api-register'),
    path('api/login/', views.user_login, name='api-login'),
    path('api/logout/', views.user_logout, name='api-logout'),

    # API endpoints for BPS data synchronization
    path('api/sync/news/', views.sync_bps_news, name='sync-bps-news'),
    path('api/sync/infographics/', views.sync_bps_infographic, name='sync-bps-infographics'),
    path('api/sync/publications/', views.sync_bps_publication, name='sync-bps-publications'),
    
    # API endpoints for Spreadsheet data synchronization
    path('api/sync/human-development-index/', views.sync_human_development_index, name='sync-human-development-index'),
    path('api/sync/hotel-occupancy-combined/', views.sync_hotel_occupancy_combined, name='sync-hotel-occupancy-combined'),
    path('api/sync/hotel-occupancy-yearly/', views.sync_hotel_occupancy_yearly, name='sync-hotel-occupancy-yearly'),
    path('api/sync/gini-ratio/', views.sync_gini_ratio, name='sync-gini-ratio'),
    
    # API endpoints for IPM sub-categories synchronization
    path('api/sync/ipm-uhh-sp/', views.sync_ipm_uhh_sp, name='sync-ipm-uhh-sp'),
    path('api/sync/ipm-hls/', views.sync_ipm_hls, name='sync-ipm-hls'),
    path('api/sync/ipm-rls/', views.sync_ipm_rls, name='sync-ipm-rls'),
    path('api/sync/ipm-pengeluaran-per-kapita/', views.sync_ipm_pengeluaran_per_kapita, name='sync-ipm-pengeluaran-per-kapita'),
    path('api/sync/ipm-indeks-kesehatan/', views.sync_ipm_indeks_kesehatan, name='sync-ipm-indeks-kesehatan'),
    path('api/sync/ipm-indeks-hidup-layak/', views.sync_ipm_indeks_hidup_layak, name='sync-ipm-indeks-hidup-layak'),
    path('api/sync/ipm-indeks-pendidikan/', views.sync_ipm_indeks_pendidikan, name='sync-ipm-indeks-pendidikan'),

    # API endpoints for Kependudukan synchronization
    path('api/sync/kependudukan/', views.sync_kependudukan, name='sync-kependudukan'),
    
    # API endpoints for PDRB synchronization
    path('api/sync/pdrb-pengeluaran/', views.sync_pdrb_pengeluaran, name='sync-pdrb-pengeluaran'),
    path('api/sync/pdrb-lapangan-usaha/', views.sync_pdrb_lapangan_usaha, name='sync-pdrb-lapangan-usaha'),

        # API endpoints for Kemiskinan synchronization
    path('api/sync/kemiskinan-surabaya/', views.sync_kemiskinan_surabaya, name='sync-kemiskinan-surabaya'),
    path('api/sync/kemiskinan-jawa-timur/', views.sync_kemiskinan_jawa_timur, name='sync-kemiskinan-jawa-timur'),
    
    # API endpoints for Inflasi synchronization
    path('api/sync/inflasi/', views.sync_inflasi, name='sync-inflasi'),
    

    # API endpoints for generic data
    path('api/data/', views.view_data, name='view-data'),
    path('api/data/add/', views.add_data, name='add-data'),
    path('api/data/update/<int:pk>/', views.update_data, name='update-data'),
    path('api/data/delete/<int:pk>/', views.delete_data, name='delete-data'),

    # API endpoints for bookmarks
    path('api/bookmarks/', views.view_bookmarks, name='view-bookmarks'),
    path('api/bookmarks/add/', views.add_bookmark, name='add-bookmark'),
    path('api/bookmarks/delete/<int:pk>/', views.delete_bookmark, name='delete-bookmark'),
    
    # API endpoints for hotel occupancy data
    path('api/hotel-occupancy/', views.get_hotel_occupancy_data, name='api-hotel-occupancy'),
    
    # API endpoints for Gini Ratio data
    path('api/gini-ratio/', views.get_gini_ratio_data, name='api-gini-ratio'),

    # API endpoints for Inflasi data
    path('api/inflasi/', views.get_inflasi_data, name='api-inflasi'),
    path('api/inflasi-perkomoditas/', views.get_inflasi_perkomoditas_data, name='api-inflasi-perkomoditas'),
    path('api/komoditas-by-flag/', views.get_komoditas_by_flag, name='api-komoditas-by-flag'),
]   
