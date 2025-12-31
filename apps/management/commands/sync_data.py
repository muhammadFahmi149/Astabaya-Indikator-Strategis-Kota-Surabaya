import time
from django.core.management.base import BaseCommand
from apps.services.API_service import (
    IPMService, 
    BPSNewsService, 
    BPSPublicationService, 
    BPSInfographicService,
    HotelOccupancyCombinedService,
    HotelOccupancyYearlyService,
    GiniRatioService,
    IPM_UHH_SPService,
    IPM_HLSService,
    IPM_RLSService,
    IPM_PengeluaranPerKapitaService,
    IPM_IndeksKesehatanService,
    IPM_IndeksHidupLayakService,
    IPM_IndeksPendidikanService,
    KetenagakerjaanTPTService,
    KetenagakerjaanTPAKService,
    KemiskinanSurabayaService,
    KemiskinanJawaTimurService,
    KependudukanService,
    PDRBPengeluaranService,
    PDRBLapanganUsahaService,
    InflasiService
)


class Command(BaseCommand):
    help = 'Sinkronisasi data dari spreadsheet dan API BPS ke database'

    def delay(self, seconds=2):
        """Delay sederhana untuk menghindari Google Sheets API quota"""
        time.sleep(seconds)

    def add_arguments(self, parser):
        parser.add_argument(
            '--type',
            type=str,
            choices=[
                'all', 'ipm', 'ipm-all', 'ipm-uhh-sp', 'ipm-hls', 'ipm-rls', 
                'ipm-pengeluaran-per-kapita', 'ipm-indeks-kesehatan', 
                'ipm-indeks-hidup-layak', 'ipm-indeks-pendidikan', 'gini-ratio',
                'news', 'publications', 'infographics', 
                'hotel-occupancy-combined', 'hotel-occupancy-yearly', 'hotel-occupancy',
                'ketenagakerjaan-tpt', 'ketenagakerjaan-tpak', 'ketenagakerjaan',
                'kemiskinan-surabaya', 'kemiskinan-jawa-timur', 'kemiskinan',
                'kependudukan', 'pdrb-pengeluaran', 'pdrb-lapangan-usaha', 'pdrb',
                'inflasi'
            ],
            default='all',
            help='Jenis data yang akan di-sync (default: all)'
        )

    def handle(self, *args, **options):
        sync_type = options['type']
        
        self.stdout.write(self.style.SUCCESS('[INFO] Memulai sinkronisasi data...'))
        self.stdout.write('')
        
        if sync_type == 'all' or sync_type == 'ipm':
            self.stdout.write('[INFO] Sinkronisasi data IPM dari spreadsheet...')
            try:
                created, updated = IPMService.sync_ipm()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] IPM: {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync IPM: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        # Sync IPM sub-categories
        ipm_sub_categories = [
            'ipm-uhh-sp', 'ipm-hls', 'ipm-rls', 'ipm-pengeluaran-per-kapita',
            'ipm-indeks-kesehatan', 'ipm-indeks-hidup-layak', 'ipm-indeks-pendidikan'
        ]
        
        if sync_type == 'all' or sync_type == 'ipm-all' or sync_type in ipm_sub_categories:
            if sync_type == 'all' or sync_type == 'ipm-all':
                self.stdout.write('[INFO] Sinkronisasi semua data IPM sub-kategori dari spreadsheet...')
                self.stdout.write('')
            
            # IPM UHH SP
            if sync_type == 'all' or sync_type == 'ipm-all' or sync_type == 'ipm-uhh-sp':
                self.stdout.write('[INFO] Sinkronisasi data IPM UHH SP...')
                try:
                    created, updated = IPM_UHH_SPService.sync_ipm_uhh_sp()
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'   [OK] IPM UHH SP: {created} data baru, {updated} data diperbarui'
                        )
                    )
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f'   [ERROR] Error sync IPM UHH SP: {str(e)}')
                    )
                self.delay(3)
                self.stdout.write('')
            
            # IPM HLS
            if sync_type == 'all' or sync_type == 'ipm-all' or sync_type == 'ipm-hls':
                self.stdout.write('[INFO] Sinkronisasi data IPM HLS...')
                try:
                    created, updated = IPM_HLSService.sync_ipm_hls()
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'   [OK] IPM HLS: {created} data baru, {updated} data diperbarui'
                        )
                    )
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f'   [ERROR] Error sync IPM HLS: {str(e)}')
                    )
                self.delay(3)
                self.stdout.write('')
            
            # IPM RLS
            if sync_type == 'all' or sync_type == 'ipm-all' or sync_type == 'ipm-rls':
                self.stdout.write('[INFO] Sinkronisasi data IPM RLS...')
                try:
                    created, updated = IPM_RLSService.sync_ipm_rls()
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'   [OK] IPM RLS: {created} data baru, {updated} data diperbarui'
                        )
                    )
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f'   [ERROR] Error sync IPM RLS: {str(e)}')
                    )
                self.delay(3)
                self.stdout.write('')
            
            # IPM Pengeluaran per Kapita
            if sync_type == 'all' or sync_type == 'ipm-all' or sync_type == 'ipm-pengeluaran-per-kapita':
                self.stdout.write('[INFO] Sinkronisasi data IPM Pengeluaran per Kapita...')
                try:
                    created, updated = IPM_PengeluaranPerKapitaService.sync_ipm_pengeluaran_per_kapita()
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'   [OK] IPM Pengeluaran per Kapita: {created} data baru, {updated} data diperbarui'
                        )
                    )
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f'   [ERROR] Error sync IPM Pengeluaran per Kapita: {str(e)}')
                    )
                self.delay(3)
                self.stdout.write('')
            
            # IPM Indeks Kesehatan
            if sync_type == 'all' or sync_type == 'ipm-all' or sync_type == 'ipm-indeks-kesehatan':
                self.stdout.write('[INFO] Sinkronisasi data IPM Indeks Kesehatan...')
                try:
                    created, updated = IPM_IndeksKesehatanService.sync_ipm_indeks_kesehatan()
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'   [OK] IPM Indeks Kesehatan: {created} data baru, {updated} data diperbarui'
                        )
                    )
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f'   [ERROR] Error sync IPM Indeks Kesehatan: {str(e)}')
                    )
                self.delay(3)
                self.stdout.write('')
            
            # IPM Indeks Hidup Layak
            if sync_type == 'all' or sync_type == 'ipm-all' or sync_type == 'ipm-indeks-hidup-layak':
                self.stdout.write('[INFO] Sinkronisasi data IPM Indeks Hidup Layak...')
                try:
                    created, updated = IPM_IndeksHidupLayakService.sync_ipm_indeks_hidup_layak()
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'   [OK] IPM Indeks Hidup Layak: {created} data baru, {updated} data diperbarui'
                        )
                    )
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f'   [ERROR] Error sync IPM Indeks Hidup Layak: {str(e)}')
                    )
                self.delay(3)
                self.stdout.write('')
            
            # IPM Indeks Pendidikan
            if sync_type == 'all' or sync_type == 'ipm-all' or sync_type == 'ipm-indeks-pendidikan':
                self.stdout.write('[INFO] Sinkronisasi data IPM Indeks Pendidikan...')
                try:
                    created, updated = IPM_IndeksPendidikanService.sync_ipm_indeks_pendidikan()
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'   [OK] IPM Indeks Pendidikan: {created} data baru, {updated} data diperbarui'
                        )
                    )
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f'   [ERROR] Error sync IPM Indeks Pendidikan: {str(e)}')
                    )
                self.delay(3)
                self.stdout.write('')
        
        if sync_type == 'all' or sync_type == 'gini-ratio':
            self.stdout.write('[INFO] Sinkronisasi data Gini Ratio dari spreadsheet...')
            try:
                created, updated = GiniRatioService.sync_gini_ratio()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] Gini Ratio: {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync Gini Ratio: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        if sync_type == 'all' or sync_type == 'news':
            self.stdout.write('[INFO] Sinkronisasi data News dari API BPS...')
            try:
                created, updated = BPSNewsService.sync_news()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] News: {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync News: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        if sync_type == 'all' or sync_type == 'publications':
            self.stdout.write('[INFO] Sinkronisasi data Publications dari API BPS...')
            try:
                created, updated = BPSPublicationService.sync_publication()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] Publications: {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync Publications: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        if sync_type == 'all' or sync_type == 'infographics':
            self.stdout.write('[INFO] Sinkronisasi data Infographics dari API BPS...')
            try:
                created, updated = BPSInfographicService.sync_infographic()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] Infographics: {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync Infographics: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        if sync_type == 'all' or sync_type == 'hotel-occupancy-combined' or sync_type == 'hotel-occupancy':
            self.stdout.write('[INFO] Sinkronisasi data Hotel Occupancy (Gabung Semua) dari spreadsheet...')
            try:
                created, updated = HotelOccupancyCombinedService.sync_hotel_occupancy_combined()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] Hotel Occupancy (Gabung Semua): {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync Hotel Occupancy (Gabung Semua): {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        if sync_type == 'all' or sync_type == 'hotel-occupancy-yearly' or sync_type == 'hotel-occupancy':
            self.stdout.write('[INFO] Sinkronisasi data Hotel Occupancy (Year-to-Year) dari spreadsheet...')
            try:
                created, updated = HotelOccupancyYearlyService.sync_hotel_occupancy_yearly()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] Hotel Occupancy (Year-to-Year): {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync Hotel Occupancy (Year-to-Year): {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        # Ketenagakerjaan sync
        if sync_type == 'all' or sync_type == 'ketenagakerjaan-tpt' or sync_type == 'ketenagakerjaan':
            self.stdout.write('[INFO] Sinkronisasi data Ketenagakerjaan TPT dari spreadsheet...')
            try:
                created, updated = KetenagakerjaanTPTService.sync_ketenagakerjaan_tpt()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] Ketenagakerjaan TPT: {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync Ketenagakerjaan TPT: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        if sync_type == 'all' or sync_type == 'ketenagakerjaan-tpak' or sync_type == 'ketenagakerjaan':
            self.stdout.write('[INFO] Sinkronisasi data Ketenagakerjaan TPAK dari spreadsheet...')
            try:
                created, updated = KetenagakerjaanTPAKService.sync_ketenagakerjaan_tpak()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] Ketenagakerjaan TPAK: {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync Ketenagakerjaan TPAK: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        # Kemiskinan sync
        if sync_type == 'all' or sync_type == 'kemiskinan-surabaya' or sync_type == 'kemiskinan':
            self.stdout.write('[INFO] Sinkronisasi data Kemiskinan Surabaya dari spreadsheet...')
            try:
                created, updated = KemiskinanSurabayaService.sync_kemiskinan_surabaya()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] Kemiskinan Surabaya: {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync Kemiskinan Surabaya: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        if sync_type == 'all' or sync_type == 'kemiskinan-jawa-timur' or sync_type == 'kemiskinan':
            self.stdout.write('[INFO] Sinkronisasi data Kemiskinan Jawa Timur dari spreadsheet...')
            try:
                created, updated = KemiskinanJawaTimurService.sync_kemiskinan_jawa_timur()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] Kemiskinan Jawa Timur: {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync Kemiskinan Jawa Timur: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        # Kependudukan sync
        if sync_type == 'all' or sync_type == 'kependudukan':
            self.stdout.write('[INFO] Sinkronisasi data Kependudukan dari spreadsheet...')
            try:
                created, updated = KependudukanService.sync_kependudukan()
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] Kependudukan: {created} data baru, {updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync Kependudukan: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        # PDRB Pengeluaran sync
        if sync_type == 'all' or sync_type == 'pdrb-pengeluaran' or sync_type == 'pdrb':
            self.stdout.write('[INFO] Sinkronisasi data PDRB Pengeluaran dari spreadsheet...')
            try:
                results = PDRBPengeluaranService.sync_all_pdrb_pengeluaran()
                total_created = sum(r['created'] for r in results.values())
                total_updated = sum(r['updated'] for r in results.values())
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] PDRB Pengeluaran: {total_created} data baru, {total_updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync PDRB Pengeluaran: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        # PDRB Lapangan Usaha sync
        if sync_type == 'all' or sync_type == 'pdrb-lapangan-usaha' or sync_type == 'pdrb':
            self.stdout.write('[INFO] Sinkronisasi data PDRB Lapangan Usaha dari spreadsheet...')
            try:
                results = PDRBLapanganUsahaService.sync_all_pdrb_lapangan_usaha()
                total_created = sum(r['created'] for r in results.values())
                total_updated = sum(r['updated'] for r in results.values())
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] PDRB Lapangan Usaha: {total_created} data baru, {total_updated} data diperbarui'
                    )
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync PDRB Lapangan Usaha: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        # Inflasi sync
        if sync_type == 'all' or sync_type == 'inflasi':
            self.stdout.write('[INFO] Sinkronisasi data Inflasi dari spreadsheet...')
            try:
                results = InflasiService.sync_all_inflasi()
                total_created = sum(r['created'] for r in results.values())
                total_updated = sum(r['updated'] for r in results.values())
                self.stdout.write(
                    self.style.SUCCESS(
                        f'   [OK] Inflasi: {total_created} data baru, {total_updated} data diperbarui'
                    )
                )
                # Print per-sheet summary
                for sheet_name, counts in results.items():
                    self.stdout.write(
                        f'      - {sheet_name}: {counts["created"]} created, {counts["updated"]} updated'
                    )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'   [ERROR] Error sync Inflasi: {str(e)}')
                )
            self.delay(3)
            self.stdout.write('')
        
        self.stdout.write(self.style.SUCCESS('[OK] Sinkronisasi selesai!'))

