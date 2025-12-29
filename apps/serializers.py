from rest_framework import serializers
from .models import (
    HumanDevelopmentIndex, User, Data, News, Infographic, Publication, 
    HotelOccupancyCombined, HotelOccupancyYearly, GiniRatio,
    IPM_UHH_SP, IPM_HLS, IPM_RLS, IPM_PengeluaranPerKapita,
    IPM_IndeksKesehatan, IPM_IndeksHidupLayak, IPM_IndeksPendidikan,
    KetenagakerjaanTPT, KetenagakerjaanTPAK, KemiskinanSurabaya, KemiskinanJawaTimur,
    Kependudukan,
    PDRBPengeluaranADHB, PDRBPengeluaranADHK, PDRBPengeluaranDistribusi, PDRBPengeluaranLajuPDRB,
    PDRBPengeluaranADHBTriwulanan, PDRBPengeluaranADHKTriwulanan, PDRBPengeluaranDistribusiTriwulanan,
    PDRBPengeluaranLajuQtoQ, PDRBPengeluaranLajuYtoY, PDRBPengeluaranLajuCtoC,
    PDRBLapanganUsahaADHB, PDRBLapanganUsahaADHK, PDRBLapanganUsahaDistribusi,
    PDRBLapanganUsahaLajuPDRB, PDRBLapanganUsahaLajuImplisit,
    PDRBLapanganUsahaADHBTriwulanan, PDRBLapanganUsahaADHKTriwulanan,
    PDRBLapanganUsahaDistribusiTriwulanan, PDRBLapanganUsahaLajuQtoQ,
    PDRBLapanganUsahaLajuYtoY, PDRBLapanganUsahaLajuCtoC,
    Inflasi, InflasiPerKomoditas, Bookmark
)
from django.db.models import fields
from django.contrib.contenttypes.models import ContentType

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['username', 'email', 'password']
        extra_kwargs = {'password': {'write_only': True}}

    def create(self, validated_data):
        user = User.objects.create_user(
            username=validated_data['username'],
            email=validated_data['email'],
            password=validated_data['password'],
        )
        return user
    
class DataSerializers(serializers.ModelSerializer):
    class Meta:
        model = Data
        fields = ('data_name', 'data_description', 'data_image', 'data_view_count', 'data_created_at')
        
class NewsSerializer(serializers.ModelSerializer):
    class Meta:
        model = News
        fields = ('news_id','title','content','category_id','category_name','release_date','picture_url')

class InfographicSerializer(serializers.ModelSerializer):
    class Meta:
        model = Infographic
        fields = ('id','title','image','dl')
        

class PublicationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Publication
        fields = ('pub_id','title','abstract','image','dl','date','size')

class BookmarkSerializer(serializers.ModelSerializer):
    """
    Serializer untuk model Bookmark.
    Ini secara dinamis menserialisasi `content_object` berdasarkan tipenya.
    """
    content_object = serializers.SerializerMethodField()
    # `content_type_name` digunakan untuk membuat bookmark baru (write-only)
    content_type_name = serializers.CharField(write_only=True, help_text="Model name: 'news', 'infographic', or 'publication'")
    # `content_type_model` digunakan untuk menampilkan nama model (read-only)
    content_type_model = serializers.CharField(source='content_type.model', read_only=True)

    class Meta:
        model = Bookmark
        fields = ['id', 'user', 'content_type_model', 'content_type_name', 'object_id', 'content_object', 'created_at']
        read_only_fields = ['user', 'created_at', 'content_object', 'content_type_model']

    def get_content_object(self, obj):
        """
        Menggunakan serializer yang sesuai berdasarkan instance dari content_object.
        """
        if isinstance(obj.content_object, News):
            return NewsSerializer(obj.content_object, context=self.context).data
        if isinstance(obj.content_object, Infographic):
            return InfographicSerializer(obj.content_object, context=self.context).data
        if isinstance(obj.content_object, Publication):
            return PublicationSerializer(obj.content_object, context=self.context).data
        return None

    def validate(self, data):
        """
        Validasi kustom untuk memastikan content_type dan object_id valid.
        """
        content_type_name = data.get('content_type_name').lower()
        object_id = data.get('object_id')
        
        # Handle array case (shouldn't happen, but just in case)
        if isinstance(object_id, list):
            if len(object_id) > 0:
                object_id = object_id[0]
            else:
                raise serializers.ValidationError("Object ID tidak boleh kosong.")
        
        # Mendapatkan model dari ContentType
        try:
            content_type = ContentType.objects.get(app_label='apps', model=content_type_name)
        except ContentType.DoesNotExist:
            raise serializers.ValidationError(f"Tipe konten '{content_type_name}' tidak valid.")

        # Memeriksa apakah objek dengan ID tersebut ada
        model_class = content_type.model_class()
        
        # For Publication model, pub_id is CharField, but Bookmark.object_id is PositiveIntegerField
        # So we need to use the Publication's primary key (id) instead of pub_id
        if content_type_name == 'publication':
            # First, try to find publication by pub_id (string)
            pub_obj = model_class.objects.filter(pub_id=str(object_id)).first()
            if not pub_obj:
                # If not found by pub_id, try by primary key (in case object_id is already the pk)
                try:
                    pub_obj = model_class.objects.filter(pk=int(object_id)).first()
                except (ValueError, TypeError):
                    pass
            
            if not pub_obj:
                raise serializers.ValidationError(f"Publikasi dengan ID '{object_id}' tidak ditemukan.")
            
            # Use the primary key (id) for Bookmark.object_id
            object_id = pub_obj.pk
        else:
            # For News and Infographic, convert to integer
            try:
                object_id = int(object_id)
            except (ValueError, TypeError):
                raise serializers.ValidationError(f"Object ID '{object_id}' tidak valid. Harus berupa angka.")
            
            if not model_class.objects.filter(pk=object_id).exists():
                raise serializers.ValidationError(f"Objek dengan ID {object_id} untuk model '{content_type_name}' tidak ditemukan.")
        
        # Menyimpan instance ContentType dan object_id yang sudah dikonversi untuk digunakan di method create
        data['content_type'] = content_type
        data['object_id'] = object_id
        return data

    def create(self, validated_data):
        # Menghapus 'content_type_name' karena tidak ada di model Bookmark
        validated_data.pop('content_type_name', None)
        return Bookmark.objects.create(**validated_data)

class HumanDevelopmentIndexSerializer(serializers.ModelSerializer):
    class Meta:
        model = HumanDevelopmentIndex
        fields = ['id', 'location_name', 'location_type', 'year', 'ipm_value']

class HotelOccupancyCombinedSerializer(serializers.ModelSerializer):
    class Meta:
        model = HotelOccupancyCombined
        fields = ['id', 'year', 'month', 'mktj', 'tpk', 'rlmta', 'rlmtnus', 'rlmtgab', 'gpr']

class HotelOccupancyYearlySerializer(serializers.ModelSerializer):
    class Meta:
        model = HotelOccupancyYearly
        fields = ['id', 'year', 'mktj', 'tpk', 'rlmta', 'rlmtnus', 'rlmtgab', 'gpr']

class GiniRatioSerializer(serializers.ModelSerializer):
    class Meta:
        model = GiniRatio
        fields = ['id', 'location_name', 'location_type', 'year', 'gini_ratio_value']

class IPM_UHH_SPSerializer(serializers.ModelSerializer):
    class Meta:
        model = IPM_UHH_SP
        fields = ['id', 'location_name', 'location_type', 'year', 'value']

class IPM_HLSSerializer(serializers.ModelSerializer):
    class Meta:
        model = IPM_HLS
        fields = ['id', 'location_name', 'location_type', 'year', 'value']

class IPM_RLSSerializer(serializers.ModelSerializer):
    class Meta:
        model = IPM_RLS
        fields = ['id', 'location_name', 'location_type', 'year', 'value']

class IPM_PengeluaranPerKapitaSerializer(serializers.ModelSerializer):
    class Meta:
        model = IPM_PengeluaranPerKapita
        fields = ['id', 'location_name', 'location_type', 'year', 'value']

class IPM_IndeksKesehatanSerializer(serializers.ModelSerializer):
    class Meta:
        model = IPM_IndeksKesehatan
        fields = ['id', 'location_name', 'location_type', 'year', 'value']

class IPM_IndeksHidupLayakSerializer(serializers.ModelSerializer):
    class Meta:
        model = IPM_IndeksHidupLayak
        fields = ['id', 'location_name', 'location_type', 'year', 'value']

class IPM_IndeksPendidikanSerializer(serializers.ModelSerializer):
    class Meta:
        model = IPM_IndeksPendidikan
        fields = ['id', 'location_name', 'location_type', 'year', 'value']

class KetenagakerjaanTPTSerializer(serializers.ModelSerializer):
    class Meta:
        model = KetenagakerjaanTPT
        fields = ['id', 'year', 'laki_laki', 'perempuan', 'total']

class KetenagakerjaanTPAKSerializer(serializers.ModelSerializer):
    class Meta:
        model = KetenagakerjaanTPAK
        fields = ['id', 'year', 'laki_laki', 'perempuan', 'total']

class KemiskinanSurabayaSerializer(serializers.ModelSerializer):
    class Meta:
        model = KemiskinanSurabaya
        fields = ['id', 'year', 'jumlah_penduduk_miskin', 'persentase_penduduk_miskin', 
                  'indeks_kedalaman_kemiskinan_p1', 'indeks_keparahan_kemiskinan_p2', 'garis_kemiskinan']

class KemiskinanJawaTimurSerializer(serializers.ModelSerializer):
    class Meta:
        model = KemiskinanJawaTimur
        fields = ['id', 'year', 'jumlah_penduduk_miskin', 'persentase_penduduk_miskin', 
                  'indeks_kedalaman_kemiskinan_p1', 'indeks_keparahan_kemiskinan_p2', 'garis_kemiskinan']

class KependudukanSerializer(serializers.ModelSerializer):
    class Meta:
        model = Kependudukan
        fields = ['id', 'age_group', 'year', 'gender', 'population']

# PDRB Pengeluaran Serializers
class PDRBPengeluaranADHBSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBPengeluaranADHB
        fields = ['id', 'expenditure_category', 'year', 'preliminary_flag', 'value']

class PDRBPengeluaranADHKSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBPengeluaranADHK
        fields = ['id', 'expenditure_category', 'year', 'preliminary_flag', 'value']

class PDRBPengeluaranDistribusiSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBPengeluaranDistribusi
        fields = ['id', 'expenditure_category', 'year', 'preliminary_flag', 'value']

class PDRBPengeluaranLajuPDRBSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBPengeluaranLajuPDRB
        fields = ['id', 'expenditure_category', 'year', 'preliminary_flag', 'value']

class PDRBPengeluaranADHBTriwulananSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBPengeluaranADHBTriwulanan
        fields = ['id', 'expenditure_category', 'year', 'quarter', 'preliminary_flag', 'value']

class PDRBPengeluaranADHKTriwulananSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBPengeluaranADHKTriwulanan
        fields = ['id', 'expenditure_category', 'year', 'quarter', 'preliminary_flag', 'value']

class PDRBPengeluaranDistribusiTriwulananSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBPengeluaranDistribusiTriwulanan
        fields = ['id', 'expenditure_category', 'year', 'quarter', 'preliminary_flag', 'value']

class PDRBPengeluaranLajuQtoQSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBPengeluaranLajuQtoQ
        fields = ['id', 'expenditure_category', 'year', 'quarter', 'preliminary_flag', 'value']

class PDRBPengeluaranLajuYtoYSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBPengeluaranLajuYtoY
        fields = ['id', 'expenditure_category', 'year', 'quarter', 'preliminary_flag', 'value']

class PDRBPengeluaranLajuCtoCSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBPengeluaranLajuCtoC
        fields = ['id', 'expenditure_category', 'year', 'quarter', 'preliminary_flag', 'value']

# PDRB Lapangan Usaha Serializers
class PDRBLapanganUsahaADHBSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBLapanganUsahaADHB
        fields = ['id', 'industry_category', 'year', 'preliminary_flag', 'value']

class PDRBLapanganUsahaADHKSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBLapanganUsahaADHK
        fields = ['id', 'industry_category', 'year', 'preliminary_flag', 'value']

class PDRBLapanganUsahaDistribusiSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBLapanganUsahaDistribusi
        fields = ['id', 'industry_category', 'year', 'preliminary_flag', 'value']

class PDRBLapanganUsahaLajuPDRBSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBLapanganUsahaLajuPDRB
        fields = ['id', 'industry_category', 'year', 'preliminary_flag', 'value']

class PDRBLapanganUsahaLajuImplisitSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBLapanganUsahaLajuImplisit
        fields = ['id', 'industry_category', 'year', 'preliminary_flag', 'value']

class PDRBLapanganUsahaADHBTriwulananSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBLapanganUsahaADHBTriwulanan
        fields = ['id', 'industry_category', 'year', 'quarter', 'preliminary_flag', 'value']

class PDRBLapanganUsahaADHKTriwulananSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBLapanganUsahaADHKTriwulanan
        fields = ['id', 'industry_category', 'year', 'quarter', 'preliminary_flag', 'value']

class PDRBLapanganUsahaDistribusiTriwulananSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBLapanganUsahaDistribusiTriwulanan
        fields = ['id', 'industry_category', 'year', 'quarter', 'preliminary_flag', 'value']

class PDRBLapanganUsahaLajuQtoQSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBLapanganUsahaLajuQtoQ
        fields = ['id', 'industry_category', 'year', 'quarter', 'preliminary_flag', 'value']

class PDRBLapanganUsahaLajuYtoYSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBLapanganUsahaLajuYtoY
        fields = ['id', 'industry_category', 'year', 'quarter', 'preliminary_flag', 'value']

class PDRBLapanganUsahaLajuCtoCSerializer(serializers.ModelSerializer):
    class Meta:
        model = PDRBLapanganUsahaLajuCtoC
        fields = ['id', 'industry_category', 'year', 'quarter', 'preliminary_flag', 'value']

# Inflasi Serializers
class InflasiSerializer(serializers.ModelSerializer):
    class Meta:
        model = Inflasi
        fields = ['id', 'year', 'month', 'bulanan', 'kumulatif', 'yoy']

class InflasiPerKomoditasSerializer(serializers.ModelSerializer):
    class Meta:
        model = InflasiPerKomoditas
        fields = ['id', 'commodity_code', 'commodity_name', 'flag', 'year', 'month', 'value']
