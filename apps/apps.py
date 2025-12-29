from django.apps import AppConfig
import os
import sys


class AppsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps'
    
    def ready(self):
        """
        Memulai scheduler saat Django app siap.
        Hanya berjalan di production atau saat tidak dalam mode test.
        """
        # Jangan jalankan scheduler saat running migrations atau dalam mode test
        if os.environ.get('RUN_MAIN') != 'true':
            return
        
        # Jangan jalankan scheduler saat dalam mode test atau saat menjalankan management commands tertentu
        if 'test' in sys.argv or 'migrate' in sys.argv or 'makemigrations' in sys.argv:
            return
        
        # Import dan start scheduler
        try:
            from .scheduler import start_scheduler
            start_scheduler()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Tidak dapat memulai scheduler: {str(e)}")
            print(f"[WARNING] Tidak dapat memulai scheduler: {str(e)}")