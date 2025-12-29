from django.contrib.auth.models import AbstractUser
from django.db import models
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType

class User(AbstractUser):
    """
    Custom user model that inherits from Django's AbstractUser.
    This provides all the necessary fields and methods for authentication.
    """
    
    pass

class Data(models.Model):
    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    data_name = models.CharField(max_length=255)
    data_description = models.TextField()
    data_image = models.ImageField(upload_to="data")
    data_view_count = models.PositiveIntegerField(default=1)
    data_created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return self.data_name
    
class News(models.Model):
    news_id = models.AutoField(primary_key=True)
    title = models.CharField(max_length=255,blank=True, null=True)
    content = models.TextField(blank=True, null=True)
    category_id = models.CharField(max_length=255, blank=True, null=True)
    category_name = models.CharField(max_length=255, blank=True, null=True)
    release_date = models.DateField(blank=True, null=True)
    picture_url = models.URLField(max_length=500, blank=True, null=True)

    def __str__(self):
        return self.title
        
class Infographic(models.Model):
    title = models.CharField(max_length=255, blank=True, null=True)
    image = models.URLField(max_length=500, blank=True, null=True)
    dl = models.URLField(max_length=500, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)

    class Meta:
        ordering = ['-created_at', '-id']

    def __str__(self):
        return self.title

class Publication(models.Model):
    pub_id = models.CharField(max_length=255, unique=True)
    title = models.CharField(max_length=255, blank=True,null=True )
    image = models.URLField(max_length=500, blank=True, null=True)
    dl = models.URLField(max_length=500, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    abstract = models.TextField(blank=True, null=True)
    size = models.CharField(max_length=50, blank=True, null=True)

    def __str__(self):
        return self.title

class Bookmark(models.Model):
    """
    Model untuk menyimpan bookmark pengguna.
    Menggunakan GenericForeignKey untuk bisa berelasi dengan
    model Infographic, Publication, atau News.
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='bookmarks')

    # Generic Relation Setup
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    content_object = GenericForeignKey('content_type', 'object_id')

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        # Pastikan setiap user hanya bisa bookmark satu item yang sama sekali.
        unique_together = ('user', 'content_type', 'object_id')
        ordering = ['-created_at']
        verbose_name = "Bookmark"
        verbose_name_plural = "Bookmarks"

    def __str__(self):
        return f'{self.user.username} bookmarked {self.content_object}'

class HumanDevelopmentIndex(models.Model):
    """
    Stores the Human Development Index (IPM) for a specific location and year.
    This model is designed to hold the transposed data.
    """
    class LocationType(models.TextChoices):
        REGENCY = 'REGENCY', 'Kabupaten'
        MUNICIPALITY = 'MUNICIPALITY', 'Kota'

    location_name = models.CharField(max_length=255, verbose_name="Nama Lokasi")
    location_type = models.CharField(
        max_length=20,
        choices=LocationType.choices,
        verbose_name="Tipe Lokasi"
    )
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    ipm_value = models.DecimalField(max_digits=5, decimal_places=2, verbose_name="Nilai IPM")

    def __str__(self):
        return f"{self.location_name} ({self.year}) - {self.ipm_value}"
    
    class Meta:
        unique_together = ('location_name', 'year')
        verbose_name = "Indeks Pembangunan Manusia"
        verbose_name_plural = "Indeks Pembangunan Manusia"

class HotelOccupancyCombined(models.Model):
    """
    Stores monthly hotel occupancy data from "Tingkat Hunian Hotel (bu tanti) gabung semua" sheet.
    Contains data with year and month.
    """
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    month = models.CharField(max_length=20, verbose_name="Bulan")
    mktj = models.DecimalField(max_digits=15, decimal_places=2, verbose_name="MKTJ", null=True, blank=True)
    tpk = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="TPK", null=True, blank=True)
    rlmta = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="RLMTA", null=True, blank=True)
    rlmtnus = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="RLMTNUS", null=True, blank=True)
    rlmtgab = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="RLMTGAB", null=True, blank=True)
    gpr = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="GPR", null=True, blank=True)

    def __str__(self):
        return f"{self.year} - {self.month}"

    class Meta:
        unique_together = ('year', 'month')
        verbose_name = "Tingkat Hunian Hotel (Gabung Semua)"
        verbose_name_plural = "Tingkat Hunian Hotel (Gabung Semua)"

class HotelOccupancyYearly(models.Model):
    """
    Stores yearly hotel occupancy data from "Tingkat Hunian Hotel (bu tanti) y-to-y" sheet.
    Contains data aggregated by year only.
    """
    year = models.PositiveSmallIntegerField(verbose_name="Tahun", unique=True)
    mktj = models.DecimalField(max_digits=15, decimal_places=2, verbose_name="MKTJ", null=True, blank=True)
    tpk = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="TPK", null=True, blank=True)
    rlmta = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="RLMTA", null=True, blank=True)
    rlmtnus = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="RLMTNUS", null=True, blank=True)
    rlmtgab = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="RLMTGAB", null=True, blank=True)
    gpr = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="GPR", null=True, blank=True)

    def __str__(self):
        return f"{self.year}"

    class Meta:
        verbose_name = "Tingkat Hunian Hotel (Year-to-Year)"
        verbose_name_plural = "Tingkat Hunian Hotel (Year-to-Year)"

class GiniRatio(models.Model):
    """
    Stores the Gini Ratio for a specific location and year.
    Data is fetched from "Gini Ratio (bu septa)_Y-to-Y" sheet in Google Sheets.
    """
    class LocationType(models.TextChoices):
        REGENCY = 'REGENCY', 'Kabupaten'
        MUNICIPALITY = 'MUNICIPALITY', 'Kota'

    location_name = models.CharField(max_length=255, verbose_name="Nama Lokasi")
    location_type = models.CharField(
        max_length=20,
        choices=LocationType.choices,
        verbose_name="Tipe Lokasi"
    )
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    gini_ratio_value = models.DecimalField(max_digits=5, decimal_places=3, verbose_name="Nilai Gini Ratio")

    def __str__(self):
        return f"{self.location_name} ({self.year}) - {self.gini_ratio_value}"
    
    class Meta:
        unique_together = ('location_name', 'year')
        verbose_name = "Gini Ratio"
        verbose_name_plural = "Gini Ratio"

# IPM Sub-Categories Models
class IPM_UHH_SP(models.Model):
    """
    Stores IPM Usia Harapan Hidup saat Lahir (SP) for a specific location and year.
    Data is fetched from "IPM_UHH SP_Y-to-Y " sheet in Google Sheets.
    """
    class LocationType(models.TextChoices):
        REGENCY = 'REGENCY', 'Kabupaten'
        MUNICIPALITY = 'MUNICIPALITY', 'Kota'

    location_name = models.CharField(max_length=255, verbose_name="Nama Lokasi")
    location_type = models.CharField(
        max_length=20,
        choices=LocationType.choices,
        verbose_name="Tipe Lokasi"
    )
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Nilai UHH SP")

    def __str__(self):
        return f"{self.location_name} ({self.year}) - {self.value}"
    
    class Meta:
        unique_together = ('location_name', 'year')
        verbose_name = "IPM Usia Harapan Hidup saat Lahir (SP)"
        verbose_name_plural = "IPM Usia Harapan Hidup saat Lahir (SP)"

class IPM_HLS(models.Model):
    """
    Stores IPM Harapan Lama Sekolah for a specific location and year.
    Data is fetched from "IPM_HLS_Y-to-Y" sheet in Google Sheets.
    """
    class LocationType(models.TextChoices):
        REGENCY = 'REGENCY', 'Kabupaten'
        MUNICIPALITY = 'MUNICIPALITY', 'Kota'

    location_name = models.CharField(max_length=255, verbose_name="Nama Lokasi")
    location_type = models.CharField(
        max_length=20,
        choices=LocationType.choices,
        verbose_name="Tipe Lokasi"
    )
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Nilai HLS")

    def __str__(self):
        return f"{self.location_name} ({self.year}) - {self.value}"
    
    class Meta:
        unique_together = ('location_name', 'year')
        verbose_name = "IPM Harapan Lama Sekolah"
        verbose_name_plural = "IPM Harapan Lama Sekolah"

class IPM_RLS(models.Model):
    """
    Stores IPM Rata-rata Lama Sekolah for a specific location and year.
    Data is fetched from "IPM_RLS_Y-to-Y" sheet in Google Sheets.
    """
    class LocationType(models.TextChoices):
        REGENCY = 'REGENCY', 'Kabupaten'
        MUNICIPALITY = 'MUNICIPALITY', 'Kota'

    location_name = models.CharField(max_length=255, verbose_name="Nama Lokasi")
    location_type = models.CharField(
        max_length=20,
        choices=LocationType.choices,
        verbose_name="Tipe Lokasi"
    )
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Nilai RLS")

    def __str__(self):
        return f"{self.location_name} ({self.year}) - {self.value}"
    
    class Meta:
        unique_together = ('location_name', 'year')
        verbose_name = "IPM Rata-rata Lama Sekolah"
        verbose_name_plural = "IPM Rata-rata Lama Sekolah"

class IPM_PengeluaranPerKapita(models.Model):
    """
    Stores IPM Pengeluaran per Kapita for a specific location and year.
    Data is fetched from "IPM_Pengeluaran per kapita_Y-to-Y" sheet in Google Sheets.
    """
    class LocationType(models.TextChoices):
        REGENCY = 'REGENCY', 'Kabupaten'
        MUNICIPALITY = 'MUNICIPALITY', 'Kota'

    location_name = models.CharField(max_length=255, verbose_name="Nama Lokasi")
    location_type = models.CharField(
        max_length=20,
        choices=LocationType.choices,
        verbose_name="Tipe Lokasi"
    )
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    value = models.DecimalField(max_digits=15, decimal_places=2, verbose_name="Nilai Pengeluaran per Kapita")

    def __str__(self):
        return f"{self.location_name} ({self.year}) - {self.value}"
    
    class Meta:
        unique_together = ('location_name', 'year')
        verbose_name = "IPM Pengeluaran per Kapita"
        verbose_name_plural = "IPM Pengeluaran per Kapita"

class IPM_IndeksKesehatan(models.Model):
    """
    Stores IPM Indeks Kesehatan for a specific location and year.
    Data is fetched from "IPM_Indeks Kesehatan_Y-to-Y" sheet in Google Sheets.
    """
    class LocationType(models.TextChoices):
        REGENCY = 'REGENCY', 'Kabupaten'
        MUNICIPALITY = 'MUNICIPALITY', 'Kota'

    location_name = models.CharField(max_length=255, verbose_name="Nama Lokasi")
    location_type = models.CharField(
        max_length=20,
        choices=LocationType.choices,
        verbose_name="Tipe Lokasi"
    )
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Nilai Indeks Kesehatan")

    def __str__(self):
        return f"{self.location_name} ({self.year}) - {self.value}"
    
    class Meta:
        unique_together = ('location_name', 'year')
        verbose_name = "IPM Indeks Kesehatan"
        verbose_name_plural = "IPM Indeks Kesehatan"

class IPM_IndeksHidupLayak(models.Model):
    """
    Stores IPM Indeks Hidup Layak for a specific location and year.
    Data is fetched from "IPM_Indeks Hidup Layak_Y-to-Y" sheet in Google Sheets.
    """
    class LocationType(models.TextChoices):
        REGENCY = 'REGENCY', 'Kabupaten'
        MUNICIPALITY = 'MUNICIPALITY', 'Kota'

    location_name = models.CharField(max_length=255, verbose_name="Nama Lokasi")
    location_type = models.CharField(
        max_length=20,
        choices=LocationType.choices,
        verbose_name="Tipe Lokasi"
    )
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Nilai Indeks Hidup Layak")

    def __str__(self):
        return f"{self.location_name} ({self.year}) - {self.value}"
    
    class Meta:
        unique_together = ('location_name', 'year')
        verbose_name = "IPM Indeks Hidup Layak"
        verbose_name_plural = "IPM Indeks Hidup Layak"

class IPM_IndeksPendidikan(models.Model):
    """
    Stores IPM Indeks Pendidikan for a specific location and year.
    Data is fetched from "IPM_Indeks Pendidikan_Y-to-Y" sheet in Google Sheets.
    """
    class LocationType(models.TextChoices):
        REGENCY = 'REGENCY', 'Kabupaten'
        MUNICIPALITY = 'MUNICIPALITY', 'Kota'

    location_name = models.CharField(max_length=255, verbose_name="Nama Lokasi")
    location_type = models.CharField(
        max_length=20,
        choices=LocationType.choices,
        verbose_name="Tipe Lokasi"
    )
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Nilai Indeks Pendidikan")

    def __str__(self):
        return f"{self.location_name} ({self.year}) - {self.value}"
    
    class Meta:
        unique_together = ('location_name', 'year')
        verbose_name = "IPM Indeks Pendidikan"
        verbose_name_plural = "IPM Indeks Pendidikan"

# Kemiskinan Models
class KemiskinanSurabaya(models.Model):
    """
    Stores poverty indicators for Surabaya City.
    Data is fetched from "Kemiskinan(Surabaya)_YtoY" sheet in Google Sheets.
    """
    year = models.PositiveSmallIntegerField(verbose_name="Tahun", unique=True)
    jumlah_penduduk_miskin = models.DecimalField(max_digits=15, decimal_places=3, verbose_name="Jumlah Penduduk Miskin (dalam 000)", null=True, blank=True)
    persentase_penduduk_miskin = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Persentase Penduduk Miskin", null=True, blank=True)
    indeks_kedalaman_kemiskinan_p1 = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Indeks Kedalaman Kemiskinan (P1)", null=True, blank=True)
    indeks_keparahan_kemiskinan_p2 = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Indeks Keparahan Kemiskinan (P2)", null=True, blank=True)
    garis_kemiskinan = models.DecimalField(max_digits=15, decimal_places=2, verbose_name="Garis Kemiskinan (Rp/Kapita/Bulan)", null=True, blank=True)

    def __str__(self):
        return f"Surabaya ({self.year})"

    class Meta:
        verbose_name = "Kemiskinan Surabaya"
        verbose_name_plural = "Kemiskinan Surabaya"

class KemiskinanJawaTimur(models.Model):
    """
    Stores poverty indicators for Jawa Timur Province.
    Data is fetched from "Kemiskinan(JawaTimur)_YtoY_" sheet in Google Sheets.
    """
    year = models.PositiveSmallIntegerField(verbose_name="Tahun", unique=True)
    jumlah_penduduk_miskin = models.DecimalField(max_digits=15, decimal_places=3, verbose_name="Jumlah Penduduk Miskin (dalam 000)", null=True, blank=True)
    persentase_penduduk_miskin = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Persentase Penduduk Miskin", null=True, blank=True)
    indeks_kedalaman_kemiskinan_p1 = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Indeks Kedalaman Kemiskinan (P1)", null=True, blank=True)
    indeks_keparahan_kemiskinan_p2 = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Indeks Keparahan Kemiskinan (P2)", null=True, blank=True)
    garis_kemiskinan = models.DecimalField(max_digits=15, decimal_places=2, verbose_name="Garis Kemiskinan (Rp/Kapita/Bulan)", null=True, blank=True)

    def __str__(self):
        return f"Jawa Timur ({self.year})"

    class Meta:
        verbose_name = "Kemiskinan Jawa Timur"
        verbose_name_plural = "Kemiskinan Jawa Timur"

# Kependudukan Models
class Kependudukan(models.Model):
    """
    Stores combined population data by age group, year, and gender.
    Data is fetched from "Kependudukan_gabungan" sheet in Google Sheets.
    This sheet contains LK, PR, and Total in one sheet with multi-row headers.
    """
    class GenderType(models.TextChoices):
        LAKI_LAKI = 'LK', 'Laki-Laki'
        PEREMPUAN = 'PR', 'Perempuan'
        TOTAL = 'TOTAL', 'Total'

    age_group = models.CharField(max_length=20, verbose_name="Kelompok Umur")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    gender = models.CharField(
        max_length=10,
        choices=GenderType.choices,
        verbose_name="Jenis Kelamin"
    )
    population = models.IntegerField(verbose_name="Jumlah Penduduk", null=True, blank=True)

    def __str__(self):
        gender_display = dict(self.GenderType.choices).get(self.gender, self.gender)
        return f"{gender_display} - {self.age_group} ({self.year})"

    class Meta:
        unique_together = ('age_group', 'year', 'gender')
        verbose_name = "Kependudukan"
        verbose_name_plural = "Kependudukan"

# Ketenagakerjaan Models
class KetenagakerjaanTPT(models.Model):
    """
    Stores Tingkat Pengangguran Terbuka (TPT) data.
    Data is fetched from "Ketenagakerjaan_TPT" sheet in Google Sheets.
    Format: Tahun, Laki-Laki, Perempuan, Total
    """
    year = models.PositiveSmallIntegerField(verbose_name="Tahun", unique=True)
    laki_laki = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Laki-Laki", null=True, blank=True)
    perempuan = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Perempuan", null=True, blank=True)
    total = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Total", null=True, blank=True)

    def __str__(self):
        return f"{self.year} - Total: {self.total}"

    class Meta:
        verbose_name = "Ketenagakerjaan TPT"
        verbose_name_plural = "Ketenagakerjaan TPT"

class KetenagakerjaanTPAK(models.Model):
    """
    Stores Tingkat Partisipasi Angkatan Kerja (TPAK) data.
    Data is fetched from "Ketenagakerjaan_TPAK" sheet in Google Sheets.
    Format: Tahun, Laki-Laki, Perempuan, Total
    """
    year = models.PositiveSmallIntegerField(verbose_name="Tahun", unique=True)
    laki_laki = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Laki-Laki", null=True, blank=True)
    perempuan = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Perempuan", null=True, blank=True)
    total = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Total", null=True, blank=True)

    def __str__(self):
        return f"{self.year} - Total: {self.total}"

    class Meta:
        verbose_name = "Ketenagakerjaan TPAK"
        verbose_name_plural = "Ketenagakerjaan TPAK"

# PDRB Pengeluaran Models
class PDRBPengeluaranADHB(models.Model):
    """
    Stores PDRB Pengeluaran data at Current Market Prices (ADHB).
    Data from "PDRB Pengeluaran_ADHB" sheet.
    """
    expenditure_category = models.CharField(max_length=255, verbose_name="Jenis Pengeluaran")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=20, decimal_places=2, verbose_name="Nilai", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.expenditure_category} - {self.year}{flag_display}: {self.value}"

    class Meta:
        unique_together = ('expenditure_category', 'year')
        verbose_name = "PDRB Pengeluaran ADHB"
        verbose_name_plural = "PDRB Pengeluaran ADHB"

class PDRBPengeluaranADHK(models.Model):
    """
    Stores PDRB Pengeluaran data at Constant 2010 Prices (ADHK).
    Data from "PDRB Pengeluaran_ADHK" sheet.
    """
    expenditure_category = models.CharField(max_length=255, verbose_name="Jenis Pengeluaran")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=20, decimal_places=2, verbose_name="Nilai", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.expenditure_category} - {self.year}{flag_display}: {self.value}"

    class Meta:
        unique_together = ('expenditure_category', 'year')
        verbose_name = "PDRB Pengeluaran ADHK"
        verbose_name_plural = "PDRB Pengeluaran ADHK"

class PDRBPengeluaranDistribusi(models.Model):
    """
    Stores PDRB Pengeluaran distribution/percentage data.
    Data from "PDRB Pengeluaran_Distribusi" sheet.
    """
    expenditure_category = models.CharField(max_length=255, verbose_name="Jenis Pengeluaran")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Nilai (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.expenditure_category} - {self.year}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('expenditure_category', 'year')
        verbose_name = "PDRB Pengeluaran Distribusi"
        verbose_name_plural = "PDRB Pengeluaran Distribusi"

class PDRBPengeluaranLajuPDRB(models.Model):
    """
    Stores PDRB Pengeluaran growth rate data.
    Data from "PDRB Pengeluaran_Laju PDRB" sheet.
    """
    expenditure_category = models.CharField(max_length=255, verbose_name="Jenis Pengeluaran")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Laju Pertumbuhan (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.expenditure_category} - {self.year}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('expenditure_category', 'year')
        verbose_name = "PDRB Pengeluaran Laju PDRB"
        verbose_name_plural = "PDRB Pengeluaran Laju PDRB"

class PDRBPengeluaranADHBTriwulanan(models.Model):
    """
    Stores quarterly PDRB Pengeluaran data at Current Market Prices (ADHB).
    Data from "PDRB Pengeluaran_ADHB_Triwulanan" sheet.
    """
    expenditure_category = models.CharField(max_length=255, verbose_name="Jenis Pengeluaran")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=20, decimal_places=2, verbose_name="Nilai", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.expenditure_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}"

    class Meta:
        unique_together = ('expenditure_category', 'year', 'quarter')
        verbose_name = "PDRB Pengeluaran ADHB Triwulanan"
        verbose_name_plural = "PDRB Pengeluaran ADHB Triwulanan"

class PDRBPengeluaranADHKTriwulanan(models.Model):
    """
    Stores quarterly PDRB Pengeluaran data at Constant 2010 Prices (ADHK).
    Data from "PDRB Pengeluaran_ADHK_Triwulanan" sheet.
    """
    expenditure_category = models.CharField(max_length=255, verbose_name="Jenis Pengeluaran")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=20, decimal_places=2, verbose_name="Nilai", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.expenditure_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}"

    class Meta:
        unique_together = ('expenditure_category', 'year', 'quarter')
        verbose_name = "PDRB Pengeluaran ADHK Triwulanan"
        verbose_name_plural = "PDRB Pengeluaran ADHK Triwulanan"

class PDRBPengeluaranDistribusiTriwulanan(models.Model):
    """
    Stores quarterly PDRB Pengeluaran distribution/percentage data.
    Data from "PDRB Pengeluaran_Distribusi_Triwulanan" sheet.
    """
    expenditure_category = models.CharField(max_length=255, verbose_name="Jenis Pengeluaran")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Nilai (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.expenditure_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('expenditure_category', 'year', 'quarter')
        verbose_name = "PDRB Pengeluaran Distribusi Triwulanan"
        verbose_name_plural = "PDRB Pengeluaran Distribusi Triwulanan"

class PDRBPengeluaranLajuQtoQ(models.Model):
    """
    Stores quarter-to-quarter growth rate for PDRB Pengeluaran.
    Data from "Laju Pertumbuhan_q-to-q_PDRB Pengeluaran_ Triwulan" sheet.
    """
    expenditure_category = models.CharField(max_length=255, verbose_name="Jenis Pengeluaran")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Laju Pertumbuhan Q-to-Q (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.expenditure_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('expenditure_category', 'year', 'quarter')
        verbose_name = "PDRB Pengeluaran Laju Q-to-Q"
        verbose_name_plural = "PDRB Pengeluaran Laju Q-to-Q"

class PDRBPengeluaranLajuYtoY(models.Model):
    """
    Stores year-to-year growth rate for PDRB Pengeluaran.
    Data from "Laju Pertumbuhan_y-to-y_PDRB Pengeluaran_ Triwulan" sheet.
    """
    expenditure_category = models.CharField(max_length=255, verbose_name="Jenis Pengeluaran")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Laju Pertumbuhan Y-to-Y (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.expenditure_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('expenditure_category', 'year', 'quarter')
        verbose_name = "PDRB Pengeluaran Laju Y-to-Y"
        verbose_name_plural = "PDRB Pengeluaran Laju Y-to-Y"

class PDRBPengeluaranLajuCtoC(models.Model):
    """
    Stores cumulative-to-cumulative growth rate for PDRB Pengeluaran.
    Data from "Laju Pertumbuhan_c-to-c_PDRB Pengeluaran_ Triwulan" sheet.
    """
    expenditure_category = models.CharField(max_length=255, verbose_name="Jenis Pengeluaran")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Laju Pertumbuhan C-to-C (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.expenditure_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('expenditure_category', 'year', 'quarter')
        verbose_name = "PDRB Pengeluaran Laju C-to-C"
        verbose_name_plural = "PDRB Pengeluaran Laju C-to-C"

# PDRB Lapangan Usaha Models
class PDRBLapanganUsahaADHB(models.Model):
    """
    Stores PDRB Lapangan Usaha data at Current Market Prices (ADHB).
    Data from "PDRB Lapus_ADHB" sheet.
    """
    industry_category = models.CharField(max_length=255, verbose_name="Lapangan Usaha")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=20, decimal_places=2, verbose_name="Nilai", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.industry_category} - {self.year}{flag_display}: {self.value}"

    class Meta:
        unique_together = ('industry_category', 'year')
        verbose_name = "PDRB Lapangan Usaha ADHB"
        verbose_name_plural = "PDRB Lapangan Usaha ADHB"

class PDRBLapanganUsahaADHK(models.Model):
    """
    Stores PDRB Lapangan Usaha data at Constant 2010 Prices (ADHK).
    Data from "PDRB Lapus_ADHK" sheet.
    """
    industry_category = models.CharField(max_length=255, verbose_name="Lapangan Usaha")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=20, decimal_places=2, verbose_name="Nilai", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.industry_category} - {self.year}{flag_display}: {self.value}"

    class Meta:
        unique_together = ('industry_category', 'year')
        verbose_name = "PDRB Lapangan Usaha ADHK"
        verbose_name_plural = "PDRB Lapangan Usaha ADHK"

class PDRBLapanganUsahaDistribusi(models.Model):
    """
    Stores PDRB Lapangan Usaha distribution/percentage data.
    Data from "PDRB Lapus_Distribusi" sheet.
    """
    industry_category = models.CharField(max_length=255, verbose_name="Lapangan Usaha")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Nilai (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.industry_category} - {self.year}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('industry_category', 'year')
        verbose_name = "PDRB Lapangan Usaha Distribusi"
        verbose_name_plural = "PDRB Lapangan Usaha Distribusi"

class PDRBLapanganUsahaLajuPDRB(models.Model):
    """
    Stores PDRB Lapangan Usaha growth rate data.
    Data from "PDRB Lapus_Laju PDRB" sheet.
    """
    industry_category = models.CharField(max_length=255, verbose_name="Lapangan Usaha")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Laju Pertumbuhan (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.industry_category} - {self.year}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('industry_category', 'year')
        verbose_name = "PDRB Lapangan Usaha Laju PDRB"
        verbose_name_plural = "PDRB Lapangan Usaha Laju PDRB"

class PDRBLapanganUsahaLajuImplisit(models.Model):
    """
    Stores PDRB Lapangan Usaha implicit growth rate data.
    Data from "PDRB Lapus_Laju Implisit" sheet.
    """
    industry_category = models.CharField(max_length=255, verbose_name="Lapangan Usaha")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Laju Implisit (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.industry_category} - {self.year}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('industry_category', 'year')
        verbose_name = "PDRB Lapangan Usaha Laju Implisit"
        verbose_name_plural = "PDRB Lapangan Usaha Laju Implisit"

class PDRBLapanganUsahaADHBTriwulanan(models.Model):
    """
    Stores quarterly PDRB Lapangan Usaha data at Current Market Prices (ADHB).
    Data from "PDRB Lapus_ADHB_Triwulanan" sheet.
    """
    industry_category = models.CharField(max_length=255, verbose_name="Lapangan Usaha")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=20, decimal_places=2, verbose_name="Nilai", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.industry_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}"

    class Meta:
        unique_together = ('industry_category', 'year', 'quarter')
        verbose_name = "PDRB Lapangan Usaha ADHB Triwulanan"
        verbose_name_plural = "PDRB Lapangan Usaha ADHB Triwulanan"

class PDRBLapanganUsahaADHKTriwulanan(models.Model):
    """
    Stores quarterly PDRB Lapangan Usaha data at Constant 2010 Prices (ADHK).
    Data from "PDRB Lapus_ADHK_Triwulanan" sheet.
    """
    industry_category = models.CharField(max_length=255, verbose_name="Lapangan Usaha")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=20, decimal_places=2, verbose_name="Nilai", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.industry_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}"

    class Meta:
        unique_together = ('industry_category', 'year', 'quarter')
        verbose_name = "PDRB Lapangan Usaha ADHK Triwulanan"
        verbose_name_plural = "PDRB Lapangan Usaha ADHK Triwulanan"

class PDRBLapanganUsahaDistribusiTriwulanan(models.Model):
    """
    Stores quarterly PDRB Lapangan Usaha distribution/percentage data.
    Data from "PDRB Lapus_Distribusi_Triwulanan" sheet.
    """
    industry_category = models.CharField(max_length=255, verbose_name="Lapangan Usaha")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Nilai (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.industry_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('industry_category', 'year', 'quarter')
        verbose_name = "PDRB Lapangan Usaha Distribusi Triwulanan"
        verbose_name_plural = "PDRB Lapangan Usaha Distribusi Triwulanan"

class PDRBLapanganUsahaLajuQtoQ(models.Model):
    """
    Stores quarter-to-quarter growth rate for PDRB Lapangan Usaha.
    Data from "Laju Pertumbuhan_q-to-q_PDRB Lapus_ Triwulan" sheet.
    """
    industry_category = models.CharField(max_length=255, verbose_name="Lapangan Usaha")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Laju Pertumbuhan Q-to-Q (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.industry_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('industry_category', 'year', 'quarter')
        verbose_name = "PDRB Lapangan Usaha Laju Q-to-Q"
        verbose_name_plural = "PDRB Lapangan Usaha Laju Q-to-Q"

class PDRBLapanganUsahaLajuYtoY(models.Model):
    """
    Stores year-to-year growth rate for PDRB Lapangan Usaha.
    Data from "Laju Pertumbuhan_y-to-y_PDRB Lapus_ Triwulan" sheet.
    """
    industry_category = models.CharField(max_length=255, verbose_name="Lapangan Usaha")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Laju Pertumbuhan Y-to-Y (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.industry_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('industry_category', 'year', 'quarter')
        verbose_name = "PDRB Lapangan Usaha Laju Y-to-Y"
        verbose_name_plural = "PDRB Lapangan Usaha Laju Y-to-Y"

class PDRBLapanganUsahaLajuCtoC(models.Model):
    """
    Stores cumulative-to-cumulative growth rate for PDRB Lapangan Usaha.
    Data from "Laju Pertumbuhan_c-to-c_PDRB Lapus_ Triwulan" sheet.
    """
    industry_category = models.CharField(max_length=255, verbose_name="Lapangan Usaha")
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    quarter = models.CharField(
        max_length=5,
        choices=[('I', 'Triwulan I'), ('II', 'Triwulan II'), ('III', 'Triwulan III'), ('IV', 'Triwulan IV'), ('TOTAL', 'Jumlah/Total')],
        verbose_name="Triwulan"
    )
    preliminary_flag = models.CharField(
        max_length=3,
        choices=[('', 'Final'), ('*', 'Preliminary'), ('**', 'Very Preliminary'), ('***', 'Very Very Preliminary')],
        default='',
        blank=True,
        verbose_name="Status Data"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Laju Pertumbuhan C-to-C (%)", null=True, blank=True)

    def __str__(self):
        flag_display = self.preliminary_flag if self.preliminary_flag else ''
        return f"{self.industry_category} - {self.year} Q{self.quarter}{flag_display}: {self.value}%"

    class Meta:
        unique_together = ('industry_category', 'year', 'quarter')
        verbose_name = "PDRB Lapangan Usaha Laju C-to-C"
        verbose_name_plural = "PDRB Lapangan Usaha Laju C-to-C"

# Inflasi Models
class Inflasi(models.Model):
    """
    Stores general inflation data by month and year.
    Data from "Inflasi" sheet.
    Contains Bulanan (Monthly), Kumulatif (Cumulative), and YoY (Year-over-Year) values.
    """
    class MonthType(models.TextChoices):
        JANUARI = 'JANUARI', 'Januari'
        FEBRUARI = 'FEBRUARI', 'Februari'
        MARET = 'MARET', 'Maret'
        APRIL = 'APRIL', 'April'
        MEI = 'MEI', 'Mei'
        JUNI = 'JUNI', 'Juni'
        JULI = 'JULI', 'Juli'
        AGUSTUS = 'AGUSTUS', 'Agustus'
        SEPTEMBER = 'SEPTEMBER', 'September'
        OKTOBER = 'OKTOBER', 'Oktober'
        NOVEMBER = 'NOPEMBER', 'November'
        DESEMBER = 'DESEMBER', 'Desember'

    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    month = models.CharField(
        max_length=20,
        choices=MonthType.choices,
        verbose_name="Bulan"
    )
    bulanan = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Bulanan (%)", null=True, blank=True)
    kumulatif = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Kumulatif (%)", null=True, blank=True)
    yoy = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="YoY (%)", null=True, blank=True)

    def __str__(self):
        return f"{self.year} - {self.get_month_display()}: Bulanan={self.bulanan}, Kumulatif={self.kumulatif}, YoY={self.yoy}"

    class Meta:
        unique_together = ('year', 'month')
        verbose_name = "Inflasi"
        verbose_name_plural = "Inflasi"

class InflasiPerKomoditas(models.Model):
    """
    Stores inflation data per commodity by year and month.
    Data from "Inflasi_perkom_YYYY" sheets (e.g., Inflasi_perkom_2022, Inflasi_perkom_2023, etc.).
    
    This flexible model handles varying commodity counts across years:
    - If a commodity exists in 2023 but not in 2024, it simply won't have records for 2024
    - Each commodity-year-month combination is stored as a separate record
    - This approach automatically scales to any number of commodities and years
    """
    class MonthType(models.TextChoices):
        JANUARI = 'JANUARI', 'Januari'
        FEBRUARI = 'FEBRUARI', 'Februari'
        MARET = 'MARET', 'Maret'
        APRIL = 'APRIL', 'April'
        MEI = 'MEI', 'Mei'
        JUNI = 'JUNI', 'Juni'
        JULI = 'JULI', 'Juli'
        AGUSTUS = 'AGUSTUS', 'Agustus'
        SEPTEMBER = 'SEPTEMBER', 'September'
        OKTOBER = 'OKTOBER', 'Oktober'
        NOVEMBER = 'NOPEMBER', 'November'
        DESEMBER = 'DESEMBER', 'Desember'

    commodity_code = models.CharField(max_length=50, verbose_name="Kode Komoditas")
    commodity_name = models.CharField(max_length=255, verbose_name="Nama Komoditas")
    flag = models.CharField(max_length=10, verbose_name="Flag", null=True, blank=True)
    year = models.PositiveSmallIntegerField(verbose_name="Tahun")
    month = models.CharField(
        max_length=20,
        choices=MonthType.choices,
        verbose_name="Bulan"
    )
    value = models.DecimalField(max_digits=10, decimal_places=2, verbose_name="Nilai (%)", null=True, blank=True)

    def __str__(self):
        return f"{self.commodity_name} ({self.commodity_code}) - {self.year} {self.get_month_display()}: {self.value}%"

    class Meta:
        # IMPORTANT: Include 'flag' in unique_together because same commodity_code 
        # can exist with different flags (e.g., code "11" can be Flag 1 or Flag 2)
        unique_together = ('commodity_code', 'flag', 'year', 'month')
        verbose_name = "Inflasi Per Komoditas"
        verbose_name_plural = "Inflasi Per Komoditas"
        indexes = [
            models.Index(fields=['commodity_code', 'year']),
            models.Index(fields=['year', 'month']),
            models.Index(fields=['commodity_code', 'flag', 'year']),
        ]
        