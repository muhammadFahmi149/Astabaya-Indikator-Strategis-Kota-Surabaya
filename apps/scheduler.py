"""
Scheduler untuk menjalankan sinkronisasi data secara otomatis setiap hari.
Menggunakan APScheduler untuk menjalankan task setiap hari jam 02:00 AM.
"""
import logging
import sys
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from django_apscheduler.jobstores import DjangoJobStore, register_events
from django.conf import settings
from django.core.management import call_command

logger = logging.getLogger(__name__)


def sync_all_data():
    """
    Fungsi untuk menjalankan sinkronisasi semua data.
    Memanggil management command sync_data dengan type 'all'.
    """
    try:
        logger.info("[SCHEDULER] Memulai sinkronisasi data otomatis...")
        print("[SCHEDULER] Memulai sinkronisasi data otomatis...")
        
        # Memanggil management command sync_data dengan type 'all'
        call_command('sync_data', '--type', 'all')
        
        logger.info("[SCHEDULER] Sinkronisasi data selesai!")
        print("[SCHEDULER] Sinkronisasi data selesai!")
    except Exception as e:
        logger.error(f"[SCHEDULER] Error saat sinkronisasi data: {str(e)}")
        print(f"[SCHEDULER] Error saat sinkronisasi data: {str(e)}")
        # Jangan raise exception agar scheduler tetap berjalan


def start_scheduler():
    """
    Memulai scheduler untuk menjalankan sinkronisasi otomatis.
    Task akan berjalan setiap hari jam 02:00 AM (timezone sesuai settings).
    """
    scheduler = BackgroundScheduler(timezone=settings.TIME_ZONE)
    
    # Menggunakan DjangoJobStore untuk menyimpan job di database
    scheduler.add_jobstore(DjangoJobStore(), "default")
    
    # Menambahkan job untuk sinkronisasi data setiap hari jam 02:00 AM
    scheduler.add_job(
        sync_all_data,
        trigger=CronTrigger(hour=2, minute=0),  # Setiap hari jam 02:00 AM
        id='sync_all_data_daily',
        name='Sinkronisasi Data Harian',
        replace_existing=True,
        max_instances=1,  # Hanya satu instance yang berjalan pada satu waktu
    )
    
    # Register events untuk logging
    register_events(scheduler)
    
    try:
        logger.info("[SCHEDULER] Memulai scheduler...")
        print("[SCHEDULER] Memulai scheduler...")
        scheduler.start()
        logger.info("[SCHEDULER] Scheduler berhasil dimulai. Task akan berjalan setiap hari jam 02:00 AM.")
        print("[SCHEDULER] Scheduler berhasil dimulai. Task akan berjalan setiap hari jam 02:00 AM.")
    except Exception as e:
        logger.error(f"[SCHEDULER] Error saat memulai scheduler: {str(e)}")
        print(f"[SCHEDULER] Error saat memulai scheduler: {str(e)}")
        # Shutdown scheduler jika ada error
        scheduler.shutdown()
        sys.exit(1)

