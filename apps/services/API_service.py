import json
import requests
import logging
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import re
import html
from bs4 import BeautifulSoup
from django.conf import settings
import time
import math
import os
import json
from apps.models import (
    HumanDevelopmentIndex, Publication, Infographic, News, 
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
    Inflasi, InflasiPerKomoditas
)
from apps.serializers import (
    HumanDevelopmentIndexSerializer, PublicationSerializer, InfographicSerializer, 
    NewsSerializer, HotelOccupancyCombinedSerializer, HotelOccupancyYearlySerializer, 
    GiniRatioSerializer, IPM_UHH_SPSerializer, IPM_HLSSerializer, IPM_RLSSerializer,
    IPM_PengeluaranPerKapitaSerializer, IPM_IndeksKesehatanSerializer,
    IPM_IndeksHidupLayakSerializer, IPM_IndeksPendidikanSerializer,
    KetenagakerjaanTPTSerializer, KetenagakerjaanTPAKSerializer, KemiskinanSurabayaSerializer,
    KemiskinanJawaTimurSerializer, KependudukanSerializer,
    PDRBPengeluaranADHBSerializer, PDRBPengeluaranADHKSerializer, PDRBPengeluaranDistribusiSerializer,
    PDRBPengeluaranLajuPDRBSerializer, PDRBPengeluaranADHBTriwulananSerializer,
    PDRBPengeluaranADHKTriwulananSerializer, PDRBPengeluaranDistribusiTriwulananSerializer,
    PDRBPengeluaranLajuQtoQSerializer, PDRBPengeluaranLajuYtoYSerializer, PDRBPengeluaranLajuCtoCSerializer,
    PDRBLapanganUsahaADHBSerializer, PDRBLapanganUsahaADHKSerializer, PDRBLapanganUsahaDistribusiSerializer,
    PDRBLapanganUsahaLajuPDRBSerializer, PDRBLapanganUsahaLajuImplisitSerializer,
    PDRBLapanganUsahaADHBTriwulananSerializer, PDRBLapanganUsahaADHKTriwulananSerializer,
    PDRBLapanganUsahaDistribusiTriwulananSerializer, PDRBLapanganUsahaLajuQtoQSerializer,
    PDRBLapanganUsahaLajuYtoYSerializer, PDRBLapanganUsahaLajuCtoCSerializer,
    InflasiSerializer, InflasiPerKomoditasSerializer
)

logger = logging.getLogger(__name__)


class IPMService:
    @staticmethod
    def fetch_ipm_data():
        """Fetches and processes IPM data from Google Sheets into a long-format DataFrame."""
        print("[INFO] Fetching IPM data from Google Sheets...")
        try:
            # Scope akses Google Sheets dan Google Drive
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            # Autentikasi
            
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            # ID Google Sheet dari link
            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("Indeks Pembangunan Manusia Menu_Y-to-Y")

            # Ambil semua data dan buat DataFrame
            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # --- Data Cleaning and Transformation ---
            # Rename column to standard name - handle various column name formats
            if 'Kabupaten/Kota\nRegency/Municipality' in df.columns:
                df = df.rename(columns={'Kabupaten/Kota\nRegency/Municipality': 'Kabupaten/Kota'})
            elif 'Kabupaten/Kot' in df.columns:
                df = df.rename(columns={'Kabupaten/Kot': 'Kabupaten/Kota'})
            elif 'Provinsi' in df.columns:
                df = df.rename(columns={'Provinsi': 'Kabupaten/Kota'})
                print(f"[INFO] Found and renamed location column: 'Provinsi' -> 'Kabupaten/Kota'")
            elif 'Kabupaten/Kota' not in df.columns:
                # Try to find the location column
                location_col_found = False
                for col in df.columns:
                    col_lower = str(col).lower()
                    if 'kabupaten' in col_lower or 'kota' in col_lower or 'kot' in col_lower or 'provinsi' in col_lower:
                        df = df.rename(columns={col: 'Kabupaten/Kota'})
                        location_col_found = True
                        print(f"[INFO] Found and renamed location column: '{col}' -> 'Kabupaten/Kota'")
                        break
                
                if not location_col_found:
                    print(f"[ERROR] Location column not found. Available columns: {df.columns.tolist()}")
                    return pd.DataFrame()
            
            # Remove empty columns
            empty_columns = [col for col in df.columns if col == '']
            if empty_columns:
                df = df.drop(columns=empty_columns)

            # Find year columns
            year_columns = [col for col in df.columns if col.isdigit() and len(col) == 4]
            if not year_columns:
                # Try to find year columns that might be strings
                for col in df.columns:
                    if col != 'Kabupaten/Kota' and str(col).strip().isdigit():
                        year_columns.append(col)
            
            if not year_columns:
                print("[ERROR] No year columns found")
                return pd.DataFrame()

            # Melt DataFrame to long format
            try:
                df_melted = pd.melt(df, id_vars=['Kabupaten/Kota'], value_vars=year_columns,
                                    var_name='Tahun', value_name='Value')
            except KeyError as e:
                print(f"[ERROR] Error during melt operation: {e}")
                print(f"[DEBUG] Available columns: {df.columns.tolist()}")
                return pd.DataFrame()

            # Convert data types
            df_melted['Tahun'] = pd.to_numeric(df_melted['Tahun'], errors='coerce')
            
            # Convert value to numeric, handling both comma and dot as decimal separator
            # Handle both formats: "12.34" (dot) and "12,34" (comma)
            df_melted['Value'] = df_melted['Value'].astype(str)
            # Replace comma with dot for decimal separator
            df_melted['Value'] = df_melted['Value'].str.replace(',', '.', regex=False)
            # Convert to numeric
            df_melted['Value'] = pd.to_numeric(df_melted['Value'], errors='coerce')

            # Hapus baris dengan nilai IPM yang tidak valid/kosong
            df_melted.dropna(subset=['Value'], inplace=True)
            df_melted.dropna(subset=['Tahun'], inplace=True)

            print(f"[OK] Data processed. Total valid records: {len(df_melted)}")
            return df_melted

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'Indeks Pembangunan Manusia Menu' not found.")
            print(f"[ERROR] Worksheet 'Indeks Pembangunan Manusia Menu' not found.")
        except KeyError as e:
            logger.error(f"KeyError while processing IPM data: {e}")
            print(f"[ERROR] KeyError: {e}")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing IPM data: {e}")
            print(f"[ERROR] Unexpected error: {e}")
        return pd.DataFrame() # Return empty DataFrame on error

    @staticmethod
    def save_ipm_to_db(ipm_df):
        """Saves the processed IPM DataFrame to the database using a serializer."""
        if ipm_df.empty:
            print("[WARNING] No IPM data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in ipm_df.iterrows():
            location_name = str(row['Kabupaten/Kota']).strip()

            # Skip any residual non-data rows
            if not location_name or "Sumber/Source" in location_name:
                continue

            # Determine location type
            location_type = HumanDevelopmentIndex.LocationType.MUNICIPALITY if location_name.startswith("KOTA") else HumanDevelopmentIndex.LocationType.REGENCY

            data_to_serialize = {
                'location_name': location_name,
                'location_type': location_type.value,
                'year': row['Tahun'],
                'ipm_value': row['Value']
            }

            # Coba dapatkan instance yang ada terlebih dahulu
            try:
                instance = HumanDevelopmentIndex.objects.get(location_name=location_name, year=row['Tahun'])
            except HumanDevelopmentIndex.DoesNotExist:
                instance = None

            # Berikan instance ke serializer jika ada (untuk mode update)
            serializer = HumanDevelopmentIndexSerializer(instance=instance, data=data_to_serialize)

            if serializer.is_valid():
                # Gunakan serializer.save() yang akan menangani create atau update secara otomatis
                obj = serializer.save()
                created = instance is None # Jika instance sebelumnya tidak ada, berarti ini adalah create
                if created:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan IPM untuk {location_name} tahun {row['Tahun']}: {serializer.errors}")

        print(f"[INFO] Total IPM records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_ipm(cls):
        """Fungsi utama untuk sinkronisasi data API -> database."""
        ipm_df = cls.fetch_ipm_data()
        created_count, updated_count = cls.save_ipm_to_db(ipm_df)
        return created_count, updated_count

class HotelOccupancyCombinedService:
    @staticmethod
    def fetch_hotel_occupancy_combined_data():
        """Fetches and processes hotel occupancy combined data from Google Sheets."""
        print("[INFO] Fetching Hotel Occupancy Combined data from Google Sheets...")
        try:
            # Scope akses Google Sheets dan Google Drive
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            # Autentikasi
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            # ID Google Sheet dari link (sama seperti HumanDevelopmentIndex)
            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("Tingkat Hunian Hotel (bu tanti)_M-to-M")

            # Ambil semua data dan buat DataFrame
            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # --- Data Cleaning and Transformation ---
            # Rename columns to match model fields (case-insensitive)
            column_mapping = {}
            for col in df.columns:
                col_upper = col.upper().strip()
                if 'TAHUN' in col_upper or col_upper == 'TAHUN':
                    column_mapping[col] = 'Tahun'
                elif 'BULAN' in col_upper or col_upper == 'BULAN':
                    column_mapping[col] = 'Bulan'
                elif 'MKTJ' in col_upper:
                    column_mapping[col] = 'MKTJ'
                elif 'TPK' in col_upper:
                    column_mapping[col] = 'TPK'
                elif 'RLMTA' in col_upper:
                    column_mapping[col] = 'RLMTA'
                elif 'RLMTNUS' in col_upper:
                    column_mapping[col] = 'RLMTNUS'
                elif 'RLMTGAB' in col_upper:
                    column_mapping[col] = 'RLMTGAB'
                elif 'GPR' in col_upper:
                    column_mapping[col] = 'GPR'
            
            df = df.rename(columns=column_mapping)
            
            # Ensure required columns exist
            required_cols = ['Tahun', 'Bulan']
            if not all(col in df.columns for col in required_cols):
                print(f"[ERROR] Missing required columns. Found: {df.columns.tolist()}")
                return pd.DataFrame()
            
            # Convert data types
            df['Tahun'] = pd.to_numeric(df['Tahun'], errors='coerce')
            
            # Convert numeric columns - same logic as IPM/GiniRatio
            numeric_cols = ['MKTJ', 'TPK', 'RLMTA', 'RLMTNUS', 'RLMTGAB', 'GPR']
            for col in numeric_cols:
                if col in df.columns:
                    # Convert value to numeric, handling both comma and dot as decimal separator
                    # Handle both formats: "12.34" (dot) and "12,34" (comma)
                    df[col] = df[col].astype(str)
                    # Replace comma with dot for decimal separator
                    df[col] = df[col].str.replace(',', '.', regex=False)
                    # Convert to numeric
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Hapus baris dengan tahun yang tidak valid
            df = df.dropna(subset=['Tahun'])
            
            # Clean month names (normalize to standard format)
            if 'Bulan' in df.columns:
                df['Bulan'] = df['Bulan'].astype(str).str.strip()
            
            print(f"[OK] Data processed. Total valid records: {len(df)}")
            return df

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'Tingkat Hunian Hotel (bu tanti) gabung semua' not found.")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing hotel occupancy combined data: {e}")
            print(f"[ERROR] {e}")
        return pd.DataFrame() # Return empty DataFrame on error

    @staticmethod
    def save_hotel_occupancy_combined_to_db(df):
        """Saves the processed hotel occupancy combined DataFrame to the database."""
        if df.empty:
            print("[WARNING] No hotel occupancy combined data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        # Helper function to round decimal values to 2 decimal places
        def round_decimal(value, decimal_places=2):
            """Round a value to specified decimal places, handling None and NaN."""
            if value is None or pd.isna(value):
                return None
            try:
                return round(float(value), decimal_places)
            except (ValueError, TypeError):
                return None

        for index, row in df.iterrows():
            year = int(row['Tahun']) if pd.notna(row['Tahun']) else None
            month = str(row['Bulan']).strip() if pd.notna(row['Bulan']) else None

            if not year or not month:
                continue

            data_to_serialize = {
                'year': year,
                'month': month,
                'mktj': round_decimal(row.get('MKTJ'), 2),
                'tpk': round_decimal(row.get('TPK'), 2),
                'rlmta': round_decimal(row.get('RLMTA'), 2),
                'rlmtnus': round_decimal(row.get('RLMTNUS'), 2),
                'rlmtgab': round_decimal(row.get('RLMTGAB'), 2),
                'gpr': round_decimal(row.get('GPR'), 2),
            }

            # Coba dapatkan instance yang ada terlebih dahulu
            try:
                instance = HotelOccupancyCombined.objects.get(year=year, month=month)
            except HotelOccupancyCombined.DoesNotExist:
                instance = None

            # Berikan instance ke serializer jika ada (untuk mode update)
            serializer = HotelOccupancyCombinedSerializer(instance=instance, data=data_to_serialize)

            if serializer.is_valid():
                obj = serializer.save()
                created = instance is None
                if created:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan hotel occupancy combined untuk {year}-{month}: {serializer.errors}")

        print(f"[INFO] Total hotel occupancy combined records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_hotel_occupancy_combined(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_hotel_occupancy_combined_data()
        created_count, updated_count = cls.save_hotel_occupancy_combined_to_db(df)
        return created_count, updated_count

class HotelOccupancyYearlyService:
    @staticmethod
    def fetch_hotel_occupancy_yearly_data():
        """Fetches and processes hotel occupancy yearly data from Google Sheets."""
        print("[INFO] Fetching Hotel Occupancy Yearly data from Google Sheets...")
        try:
            # Scope akses Google Sheets dan Google Drive
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            # Autentikasi
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            # ID Google Sheet dari link (sama seperti HumanDevelopmentIndex)
            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("Tingkat Hunian Hotel (bu tanti)_Y-to-Y")

            # Ambil semua data dan buat DataFrame
            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # --- Data Cleaning and Transformation ---
            # Rename columns to match model fields (case-insensitive)
            column_mapping = {}
            for col in df.columns:
                col_upper = col.upper().strip()
                if 'TAHUN' in col_upper or col_upper == 'TAHUN':
                    column_mapping[col] = 'Tahun'
                elif 'MKTJ' in col_upper:
                    column_mapping[col] = 'MKTJ'
                elif 'TPK' in col_upper:
                    column_mapping[col] = 'TPK'
                elif 'RLMTA' in col_upper:
                    column_mapping[col] = 'RLMTA'
                elif 'RLMTNUS' in col_upper:
                    column_mapping[col] = 'RLMTNUS'
                elif 'RLMTGAB' in col_upper:
                    column_mapping[col] = 'RLMTGAB'
                elif 'GPR' in col_upper:
                    column_mapping[col] = 'GPR'
            
            df = df.rename(columns=column_mapping)
            
            # Ensure required columns exist
            if 'Tahun' not in df.columns:
                print(f"[ERROR] Missing required column 'Tahun'. Found: {df.columns.tolist()}")
                return pd.DataFrame()
            
            # Convert data types
            df['Tahun'] = pd.to_numeric(df['Tahun'], errors='coerce')
            
            # Convert numeric columns - same logic as IPM/GiniRatio
            numeric_cols = ['MKTJ', 'TPK', 'RLMTA', 'RLMTNUS', 'RLMTGAB', 'GPR']
            for col in numeric_cols:
                if col in df.columns:
                    # Convert value to numeric, handling both comma and dot as decimal separator
                    # Handle both formats: "12.34" (dot) and "12,34" (comma)
                    df[col] = df[col].astype(str)
                    # Replace comma with dot for decimal separator
                    df[col] = df[col].str.replace(',', '.', regex=False)
                    # Convert to numeric
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Hapus baris dengan tahun yang tidak valid
            df = df.dropna(subset=['Tahun'])
            
            print(f"[OK] Data processed. Total valid records: {len(df)}")
            return df

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'Tingkat Hunian Hotel (bu tanti) y-to-y' not found.")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing hotel occupancy yearly data: {e}")
            print(f"[ERROR] {e}")
        return pd.DataFrame() # Return empty DataFrame on error

    @staticmethod
    def save_hotel_occupancy_yearly_to_db(df):
        """Saves the processed hotel occupancy yearly DataFrame to the database."""
        if df.empty:
            print("[WARNING] No hotel occupancy yearly data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        # Helper function to round decimal values to 2 decimal places
        def round_decimal(value, decimal_places=2):
            """Round a value to specified decimal places, handling None and NaN."""
            if value is None or pd.isna(value):
                return None
            try:
                return round(float(value), decimal_places)
            except (ValueError, TypeError):
                return None

        for index, row in df.iterrows():
            year = int(row['Tahun']) if pd.notna(row['Tahun']) else None

            if not year:
                continue

            data_to_serialize = {
                'year': year,
                'mktj': round_decimal(row.get('MKTJ'), 2),
                'tpk': round_decimal(row.get('TPK'), 2),
                'rlmta': round_decimal(row.get('RLMTA'), 2),
                'rlmtnus': round_decimal(row.get('RLMTNUS'), 2),
                'rlmtgab': round_decimal(row.get('RLMTGAB'), 2),
                'gpr': round_decimal(row.get('GPR'), 2),
            }

            # Coba dapatkan instance yang ada terlebih dahulu
            try:
                instance = HotelOccupancyYearly.objects.get(year=year)
            except HotelOccupancyYearly.DoesNotExist:
                instance = None

            # Berikan instance ke serializer jika ada (untuk mode update)
            serializer = HotelOccupancyYearlySerializer(instance=instance, data=data_to_serialize)

            if serializer.is_valid():
                obj = serializer.save()
                created = instance is None
                if created:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan hotel occupancy yearly untuk {year}: {serializer.errors}")

        print(f"[INFO] Total hotel occupancy yearly records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_hotel_occupancy_yearly(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_hotel_occupancy_yearly_data()
        created_count, updated_count = cls.save_hotel_occupancy_yearly_to_db(df)
        return created_count, updated_count

class GiniRatioService:
    @staticmethod
    def fetch_gini_ratio_data():
        """Fetches and processes Gini Ratio data from Google Sheets into a long-format DataFrame."""
        print("[INFO] Fetching Gini Ratio data from Google Sheets...")
        try:
            # Scope akses Google Sheets dan Google Drive
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            # Autentikasi
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            # ID Google Sheet dari link
            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("Gini Ratio (bu septa)_Y-to-Y")

            # Ambil semua data dan buat DataFrame
            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # --- Data Cleaning and Transformation ---
            # Rename column A to standard name
            if 'Kabupaten/Kot' in df.columns:
                df = df.rename(columns={'Kabupaten/Kot': 'Kabupaten/Kota'})
            elif 'Kabupaten/Kota' not in df.columns:
                # Try to find the location column
                for col in df.columns:
                    if 'kabupaten' in col.lower() or 'kota' in col.lower() or 'kot' in col.lower():
                        df = df.rename(columns={col: 'Kabupaten/Kota'})
                        break
            
            # Remove empty columns
            empty_columns = [col for col in df.columns if col == '']
            if empty_columns:
                df = df.drop(columns=empty_columns)

            # Melt DataFrame to long format
            year_columns = [col for col in df.columns if col.isdigit() and len(col) == 4]
            if not year_columns:
                # Try to find year columns that might be strings
                for col in df.columns:
                    if col != 'Kabupaten/Kota' and str(col).strip().isdigit():
                        year_columns.append(col)
            
            if not year_columns:
                print("[ERROR] No year columns found")
                return pd.DataFrame()

            df_melted = pd.melt(df, id_vars=['Kabupaten/Kota'], value_vars=year_columns,
                                var_name='Tahun', value_name='Value')

            # Convert data types
            df_melted['Tahun'] = pd.to_numeric(df_melted['Tahun'], errors='coerce')
            # Convert value to numeric, handling both comma and dot as decimal separator
            df_melted['Value'] = df_melted['Value'].astype(str).str.replace(',', '.', regex=False)
            df_melted['Value'] = pd.to_numeric(df_melted['Value'], errors='coerce')

            # Hapus baris dengan nilai Gini Ratio yang tidak valid/kosong
            df_melted.dropna(subset=['Value'], inplace=True)
            
            # Hapus baris dengan tahun yang tidak valid
            df_melted.dropna(subset=['Tahun'], inplace=True)

            print(f"[OK] Data processed. Total valid records: {len(df_melted)}")
            return df_melted

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'Gini Ratio (bu septa)_Y-to-Y' not found.")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing Gini Ratio data: {e}")
            print(f"[ERROR] {e}")
        return pd.DataFrame() # Return empty DataFrame on error

    @staticmethod
    def save_gini_ratio_to_db(gini_df):
        """Saves the processed Gini Ratio DataFrame to the database using a serializer."""
        if gini_df.empty:
            print("[WARNING] No Gini Ratio data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in gini_df.iterrows():
            location_name = str(row['Kabupaten/Kota']).strip()

            # Skip any residual non-data rows
            if not location_name or "Sumber/Source" in location_name or location_name == '':
                continue

            # Determine location type
            location_type = GiniRatio.LocationType.MUNICIPALITY if location_name.startswith("KOTA") else GiniRatio.LocationType.REGENCY

            data_to_serialize = {
                'location_name': location_name,
                'location_type': location_type.value,
                'year': int(row['Tahun']),
                'gini_ratio_value': row['Value']
            }

            # Coba dapatkan instance yang ada terlebih dahulu
            try:
                instance = GiniRatio.objects.get(location_name=location_name, year=int(row['Tahun']))
            except GiniRatio.DoesNotExist:
                instance = None

            # Berikan instance ke serializer jika ada (untuk mode update)
            serializer = GiniRatioSerializer(instance=instance, data=data_to_serialize)

            if serializer.is_valid():
                # Gunakan serializer.save() yang akan menangani create atau update secara otomatis
                obj = serializer.save()
                created = instance is None # Jika instance sebelumnya tidak ada, berarti ini adalah create
                if created:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan Gini Ratio untuk {location_name} tahun {row['Tahun']}: {serializer.errors}")

        print(f"[INFO] Total Gini Ratio records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_gini_ratio(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        gini_df = cls.fetch_gini_ratio_data()
        created_count, updated_count = cls.save_gini_ratio_to_db(gini_df)
        return created_count, updated_count
        
class BPSNewsService:
    """
    Service untuk fetch dan sync data News dari BPS API.
    Menggunakan fungsi cleaning untuk membersihkan HTML dan memvalidasi data.
    """
    
    @staticmethod
    def clean_html_content(text):
        """
        Membersihkan HTML tags dan entities dari teks.
        Menggunakan BeautifulSoup jika tersedia, jika tidak menggunakan regex fallback.
        Juga membersihkan karakter escape Unicode seperti \u003C, \u003E, dll.
        """
        if not text or not isinstance(text, str):
            return ""
        
        # First, handle literal Unicode escape sequences like \u003C (which is <), \u003E (which is >), etc.
        # These are Unicode escape sequences stored as literal strings
        text = re.sub(r'\\u003C', '', text)
        text = re.sub(r'\\u003E', '', text)
        text = re.sub(r'\\u0022', '', text)  # "
        text = re.sub(r'\\u0027', '', text)  # '
        text = re.sub(r'\\u0020', ' ', text)  # space
        
        # Handle other common escape sequences
        text = re.sub(r'\\u000D\\u000A', ' ', text)
        text = re.sub(r'\\u000D', ' ', text)
        text = re.sub(r'\\u000A', ' ', text)
        text = re.sub(r'\\u0009', ' ', text)  # tab
        text = re.sub(r'\\u000B', ' ', text)  # vertical tab
        text = re.sub(r'\\u000C', ' ', text)  # form feed
        text = re.sub(r'\\r\\n', ' ', text)
        text = re.sub(r'\\n', ' ', text)
        text = re.sub(r'\\r', ' ', text)
        text = re.sub(r'\\t', ' ', text)
        
        # Try to decode Unicode escape sequences if they exist as literal strings
        try:
            # Replace literal \uXXXX patterns with actual characters
            text = re.sub(r'\\u([0-9a-fA-F]{4})', 
                lambda m: chr(int(m.group(1), 16)), text)
        except Exception as e:
            logger.warning(f"Error decoding Unicode escapes: {e}")
        
        # Decode HTML entities terlebih dahulu
        text = html.unescape(text)
        
        # Remove style attributes and their content first
        text = re.sub(r'style\s*=\s*["\'][^"\']*["\']', '', text, flags=re.IGNORECASE)
        text = re.sub(r'style\s*=\s*[^\s>]*', '', text, flags=re.IGNORECASE)
        
        # Gunakan BeautifulSoup jika tersedia
        try:
            # Parse dengan BeautifulSoup untuk menghapus semua HTML tags
            soup = BeautifulSoup(text, 'html.parser')
            
            # Ambil hanya teks, tanpa tag HTML
            cleaned_text = soup.get_text(separator=' ', strip=False)
            
            # Normalisasi whitespace: hapus multiple spaces, tabs, newlines berlebihan
            cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
            
            # Hapus leading/trailing whitespace
            cleaned_text = cleaned_text.strip()
            
            return cleaned_text
        except Exception as e:
            logger.warning(f"Error menggunakan BeautifulSoup: {e}. Menggunakan fallback method.")
            
            # Fallback method: hapus HTML tags dengan regex
            # Hapus script dan style tags beserta isinya
            text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
            text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
            
            # Hapus semua HTML tags
            text = re.sub(r'<[^>]+>', '', text)
            
            # Normalisasi whitespace
            text = re.sub(r'\s+', ' ', text)
            
            # Hapus leading/trailing whitespace
            text = text.strip()
            
            return text

    @staticmethod
    def clean_text_field(text, max_length=None):
        """
        Membersihkan field teks biasa (title, category_name, dll).
        """
        if not text or not isinstance(text, str):
            return ""
        
        # Decode HTML entities
        cleaned = html.unescape(text)
        
        # Hapus HTML tags jika ada
        cleaned = re.sub(r'<[^>]+>', '', cleaned)
        
        # Normalisasi whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        cleaned = cleaned.strip()
        
        # Truncate jika melebihi max_length
        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]
        
        return cleaned

    @staticmethod
    def validate_and_clean_news_id(news_id):
        """
        Validasi dan cleaning untuk news_id.
        Pastikan news_id adalah numeric dan valid.
        """
        if news_id is None:
            return None
        
        # Convert ke string dulu
        news_id_str = str(news_id).strip()
        
        # Hapus karakter non-numeric kecuali tanda minus di awal
        news_id_str = re.sub(r'[^\d-]', '', news_id_str)
        
        # Coba convert ke integer
        try:
            news_id_int = int(news_id_str)
            # Pastikan positif
            if news_id_int > 0:
                return news_id_int
            else:
                logger.warning(f"Invalid news_id (non-positive): {news_id}")
                return None
        except (ValueError, TypeError):
            logger.warning(f"Invalid news_id (non-numeric): {news_id}")
            return None

    @staticmethod
    def clean_url(url):
        """
        Membersihkan dan validasi URL.
        """
        if not url or not isinstance(url, str):
            return None
        url = url.strip()
        if url.startswith(('http://', 'https://')):
            return url[:500]  # Limit panjang URL
        return None

    @staticmethod
    def fetch_news_data():
        base_url = f"https://webapi.bps.go.id/v1/api/list/model/news/lang/ind/domain/3578/key/{settings.API_KEY}/"
        first_page = requests.get(f"{base_url}?page=1").json()
        total_pages = first_page["data"][0]["pages"]
        all_news = first_page["data"][1]
        for page in range(2, total_pages + 1):
            print(f"📡 Fetching page {page} ...")    
            response = requests.get(f"{base_url}?page={page}")
            response.raise_for_status()
            data = response.json()
            if "data" in data and len(data["data"]) > 1:
                all_news.extend(data["data"][1])
        print(f"✅ Total berita diambil: {len(all_news)}")
        return all_news
    @staticmethod
    def save_news_to_db(news_list):
        """Simpan hasil fetch ke database menggunakan serializer dengan cleaning data."""
        created_count = 0
        updated_count = 0
        skipped_count = 0
        
        for item in news_list:
            # Validasi dan clean news_id
            news_id = BPSNewsService.validate_and_clean_news_id(item.get('news_id'))
            if not news_id:
                skipped_count += 1
                continue
            
            # Clean semua field menggunakan fungsi cleaning
            cleaned_title = BPSNewsService.clean_text_field(item.get('title'), max_length=255)
            cleaned_content = BPSNewsService.clean_html_content(item.get('news'))
            cleaned_category_id = BPSNewsService.clean_text_field(item.get('newscat_id'), max_length=255)
            cleaned_category_name = BPSNewsService.clean_text_field(item.get('newscat_name'), max_length=255)
            cleaned_picture_url = BPSNewsService.clean_url(item.get('picture'))
            
            # Convert release_date ke date (bukan datetime)
            release_date = item.get('rl_date')
            if release_date:
                try:
                    # Konversi ke datetime dulu, lalu ambil date-nya
                    dt = pd.to_datetime(release_date, errors='coerce')
                    if pd.isna(dt):
                        release_date = None
                    else:
                        # Konversi pandas Timestamp ke Python date object
                        # pd.Timestamp memiliki method .date() yang mengembalikan date object
                        release_date = dt.date()
                except Exception:
                    release_date = None
            
            # Map API fields to model fields dengan data yang sudah dibersihkan
            data_to_serialize = {
                'news_id': news_id,
                'title': cleaned_title,
                'content': cleaned_content,
                'category_id': cleaned_category_id,
                'category_name': cleaned_category_name,
                'release_date': release_date,
                'picture_url': cleaned_picture_url
            }
            
            serializer = NewsSerializer(data=data_to_serialize)
            if serializer.is_valid():
                obj, created = News.objects.update_or_create(
                    news_id=news_id,
                    defaults=serializer.validated_data
                )
                if created:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                logger.error(f"Error saat menyimpan news_id {news_id}: {serializer.errors}")
                skipped_count += 1
        
        print(f"💾 Total berita dibuat: {created_count}, diperbarui: {updated_count}, di-skip: {skipped_count}")
        return created_count, updated_count

    @classmethod
    def sync_news(cls):
        """Fungsi utama untuk sinkronisasi data API -> database."""
        news_list = cls.fetch_news_data()
        created_count, updated_count = cls.save_news_to_db(news_list)
        return created_count, updated_count
class BPSPublicationService:
    @staticmethod
    def fetch_publication_data():
        base_url = f"https://webapi.bps.go.id/v1/api/list/model/publication/lang/ind/domain/3578/key/{settings.API_KEY}/"
        first_page = requests.get(f"{base_url}?page=1").json()
        total_pages = first_page["data"][0]["pages"]
        all_publication = first_page["data"][1]
        for page in range(2, total_pages + 1):
            print(f"📡 Fetching page {page} ...")    
            response = requests.get(f"{base_url}?page={page}")
            response.raise_for_status()
            data = response.json()
            if "data" in data and len(data["data"]) > 1:
                all_publication.extend(data["data"][1])
        print(f"✅ Total publikasi diambil: {len(all_publication)}")
        return all_publication
    @staticmethod
    def save_publication_to_db(publication_list):
        """Simpan hasil fetch ke database menggunakan serializer."""
        created_count = 0
        updated_count = 0
        for item in publication_list:
            pub_id = item.get('pub_id')
            if not pub_id:
                continue
                
            # Map API fields to model fields
            # Truncate URL fields to max 500 characters to avoid validation error
            dl_value = item.get('pdf', '')
            if dl_value and len(dl_value) > 500:
                dl_value = dl_value[:500]
            
            image_value = item.get('cover', '')
            if image_value and len(image_value) > 500:
                image_value = image_value[:500]
            
            # Clean abstract from special characters
            abstract_value = item.get('abstract', '') or ''
            if abstract_value:
                # Handle literal escape sequences like "\u000D\u000A"
                abstract_value = re.sub(r'\\u000D\\u000A', ' ', abstract_value, flags=re.IGNORECASE)
                abstract_value = re.sub(r'\\u000D', ' ', abstract_value, flags=re.IGNORECASE)
                abstract_value = re.sub(r'\\u000A', ' ', abstract_value, flags=re.IGNORECASE)
                abstract_value = re.sub(r'\\u0009', ' ', abstract_value, flags=re.IGNORECASE)  # tab
                abstract_value = re.sub(r'\\u000B', ' ', abstract_value, flags=re.IGNORECASE)  # vertical tab
                abstract_value = re.sub(r'\\u000C', ' ', abstract_value, flags=re.IGNORECASE)  # form feed
                
                # Handle other common escape sequences
                abstract_value = re.sub(r'\\r\\n', ' ', abstract_value, flags=re.IGNORECASE)
                abstract_value = re.sub(r'\\n', ' ', abstract_value, flags=re.IGNORECASE)
                abstract_value = re.sub(r'\\r', ' ', abstract_value, flags=re.IGNORECASE)
                abstract_value = re.sub(r'\\t', ' ', abstract_value, flags=re.IGNORECASE)
                
                # Try to decode Unicode escape sequences
                try:
                    abstract_value = re.sub(r'\\u([0-9a-fA-F]{4})', 
                        lambda m: chr(int(m.group(1), 16)), abstract_value)
                except:
                    pass
                
                # Remove actual control characters
                abstract_value = re.sub(r'[\r\n]+', ' ', abstract_value)
                abstract_value = re.sub(r'[\u0000-\u001F\u007F-\u009F]', ' ', abstract_value)
                
                # Replace multiple spaces with single space
                abstract_value = re.sub(r'[\s\t]+', ' ', abstract_value).strip()
            
            data_to_serialize = {
                'pub_id': pub_id,
                'title': item.get('title'),
                'abstract': abstract_value,
                'image': image_value,
                'dl': dl_value,
                'date': item.get('rl_date'),
                'size': item.get('size')
            }
            
            # Get existing instance if it exists
            existing_obj = None
            try:
                existing_obj = Publication.objects.get(pub_id=pub_id)
                # Use existing instance for update
                serializer = PublicationSerializer(existing_obj, data=data_to_serialize, partial=True)
            except Publication.DoesNotExist:
                # New instance, use data only
                serializer = PublicationSerializer(data=data_to_serialize)
            
            if serializer.is_valid():
                serializer.save()
                if existing_obj:
                    updated_count += 1
                else:
                    created_count += 1
            else:
                print(f"❌ Error saat menyimpan pub_id {pub_id}: {serializer.errors}")
        print(f"💾 Total publikasi dibuat: {created_count}, diperbarui: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_publication(cls):
        """Fungsi utama untuk sinkronisasi data API -> database."""
        publication_list = cls.fetch_publication_data()
        created_count, updated_count = cls.save_publication_to_db(publication_list)
        return created_count, updated_count

class BPSInfographicService:
    @staticmethod
    def fetch_infographic_data():
        base_url = f"https://webapi.bps.go.id/v1/api/list/model/infographic/lang/ind/domain/3578/key/{settings.API_KEY}/"
        first_page = requests.get(f"{base_url}?page=1").json()
        total_pages = first_page["data"][0]["pages"]
        all_infographic = first_page["data"][1]
        for page in range(2, total_pages + 1):
            print(f"📡 Fetching page {page} ...")    
            response = requests.get(f"{base_url}?page={page}")
            response.raise_for_status()
            data = response.json()
            if "data" in data and len(data["data"]) > 1:
                all_infographic.extend(data["data"][1])
        print(f"✅ Total infografis diambil: {len(all_infographic)}")
        return all_infographic
    @staticmethod
    def save_infographic_to_db(infographic_list):
        """Simpan hasil fetch ke database menggunakan serializer."""
        created_count = 0
        updated_count = 0
        for item in infographic_list:
            # Map API fields to model fields
            data_to_serialize = {
                'title': item.get('title'),
                'image': item.get('img'),
                'dl': item.get('dl')
            }
            serializer = InfographicSerializer(data=data_to_serialize)
            if serializer.is_valid():
                obj, created = Infographic.objects.update_or_create(
                    title=item.get("title"), # Assuming title is unique for infographics
                    defaults=serializer.validated_data
                )
                if created:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"❌ Error saat menyimpan infografis {item.get('title')}: {serializer.errors}")
        print(f"💾 Total infografis dibuat: {created_count}, diperbarui: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_infographic(cls):
        """Fungsi utama untuk sinkronisasi data API -> database."""
        infographic_list = cls.fetch_infographic_data()
        created_count, updated_count = cls.save_infographic_to_db(infographic_list)
        return created_count, updated_count
# def _fetch_bps_data(model: str):
#     """
#     Fetches data from the BPS Web API for a given model, handling pagination.
#     """
#     base_url = f"https://webapi.bps.go.id/v1/api/list/model/{model}/lang/ind/domain/3578/key/{settings.API_KEY}/"
#     all_data = []

#     try:
#         # First request to get total pages
#         initial_response = requests.get(f"{base_url}page/1", timeout=10)
#         initial_response.raise_for_status()
#         initial_data = initial_response.json()
#         total_pages = int(initial_data["data"][0]["pages"])
#         all_data.extend(initial_data["data"][1])

#         # Loop through the rest of the pages
#         for page in range(2, total_pages + 1):
#             paginated_url = f"{base_url}page/{page}"
#             response = requests.get(paginated_url, timeout=10)
#             response.raise_for_status()
#             page_data = response.json()
            
#             if model == "publication":
#                 serializer = PublicationSerializer(data=page_data, many=True)
#                 for res in page_data:
#                     data = json.dumps(res)
#                     pub_id = res["pub_id"]
#                     title = res["title"]
#                     abstract = res["abstract"]
#                     image = res["cover"]
#                     dl = res["pdf"]
#                     date = res["rl_date"]
#                     size = res["size"]
#                     # publication = Publication.objects.create(
#                     # pub_id = pub_id,
#                     # title = title,
#                     # abstract = abstract,
#                     # image = image,
#                     # dl = dl,
#                     # date = date,
#                     # size = size,
#                     # )
#             if model == "infographic":
#                 for res in page_data:
#                     data = json.dumps(res)
#                     title = res["title"]
#                     image = res["img"]
#                     dl = res["dl"]
#                     infographic =Inpographic.objects.create(
#                     title = title,
#                     image = image,
#                     dl = dl,
#                     )
#             if model == "news":
#                 for res in page_data:
#                     data = json.dumps(res)
#                     title = res["title"]
#                     content = res.get()
#                     category_id = res.get()
#                     infographic =Inpographic.objects.create(
#                     title = title,
#                     image = image,
#                     dl = dl,
#                     )
            
#             all_data.extend(page_data["data"][1])
#             time.sleep(0.2) # Be respectful to the API server

#         return all_data

#     except requests.exceptions.RequestException as e:
#         logger.error(f"Failed to retrieve BPS data for model '{model}': {e}")
#     except Exception as e:
#         logger.error(f"Unexpected data structure from BPS API for model '{model}': {e}")
#     return []

# def get_news_data():
#     return _fetch_bps_data("news")

# def get_publication_data():
#     return _fetch_bps_data("publication")

# def get_inpographic_data():
#     return _fetch_bps_data("infographic")

# ========== IPM Sub-Categories Services ==========

class IPM_UHH_SPService:
    @staticmethod
    def fetch_ipm_uhh_sp_data():
        """Fetches and processes IPM UHH SP data from Google Sheets into a long-format DataFrame."""
        print("[INFO] Fetching IPM UHH SP data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("IPM_UHH SP_Y-to-Y ")

            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # --- Data Cleaning and Transformation ---
            # Rename column to standard name - handle various column name formats
            if 'Kabupaten/Kota\nRegency/Municipality' in df.columns:
                df = df.rename(columns={'Kabupaten/Kota\nRegency/Municipality': 'Kabupaten/Kota'})
            elif 'Kabupaten/Kot' in df.columns:
                df = df.rename(columns={'Kabupaten/Kot': 'Kabupaten/Kota'})
            elif 'Provinsi' in df.columns:
                df = df.rename(columns={'Provinsi': 'Kabupaten/Kota'})
                print(f"[INFO] Found and renamed location column: 'Provinsi' -> 'Kabupaten/Kota'")
            elif 'Kabupaten/Kota' not in df.columns:
                # Try to find the location column
                location_col_found = False
                for col in df.columns:
                    col_lower = str(col).lower()
                    if 'kabupaten' in col_lower or 'kota' in col_lower or 'kot' in col_lower or 'provinsi' in col_lower:
                        df = df.rename(columns={col: 'Kabupaten/Kota'})
                        location_col_found = True
                        print(f"[INFO] Found and renamed location column: '{col}' -> 'Kabupaten/Kota'")
                        break
                
                if not location_col_found:
                    print(f"[ERROR] Location column not found. Available columns: {df.columns.tolist()}")
                    return pd.DataFrame()
            
            # Remove empty columns
            empty_columns = [col for col in df.columns if col == '']
            if empty_columns:
                df = df.drop(columns=empty_columns)

            # Find year columns
            year_columns = [col for col in df.columns if col.isdigit() and len(col) == 4]
            if not year_columns:
                # Try to find year columns that might be strings
                for col in df.columns:
                    if col != 'Kabupaten/Kota' and str(col).strip().isdigit():
                        year_columns.append(col)
            
            if not year_columns:
                print("[ERROR] No year columns found")
                return pd.DataFrame()

            # Melt DataFrame to long format
            try:
                df_melted = pd.melt(df, id_vars=['Kabupaten/Kota'], value_vars=year_columns,
                                    var_name='Tahun', value_name='Value')
            except KeyError as e:
                print(f"[ERROR] Error during melt operation: {e}")
                print(f"[DEBUG] Available columns: {df.columns.tolist()}")
                return pd.DataFrame()

            # Convert data types
            df_melted['Tahun'] = pd.to_numeric(df_melted['Tahun'], errors='coerce')
            
            # Convert value to numeric, handling both comma and dot as decimal separator
            # Handle both formats: "12.34" (dot) and "12,34" (comma)
            df_melted['Value'] = df_melted['Value'].astype(str)
            # Replace comma with dot for decimal separator
            df_melted['Value'] = df_melted['Value'].str.replace(',', '.', regex=False)
            # Convert to numeric
            df_melted['Value'] = pd.to_numeric(df_melted['Value'], errors='coerce')
            
            # Remove rows with invalid/empty values
            df_melted.dropna(subset=['Value'], inplace=True)
            df_melted.dropna(subset=['Tahun'], inplace=True)

            print(f"[OK] Data processed. Total valid records: {len(df_melted)}")
            return df_melted

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'IPM_UHH SP_Y-to-Y ' not found.")
            print(f"[ERROR] Worksheet 'IPM_UHH SP_Y-to-Y ' not found.")
        except KeyError as e:
            logger.error(f"KeyError while processing IPM UHH SP data: {e}")
            print(f"[ERROR] KeyError: {e}")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing IPM UHH SP data: {e}")
            print(f"[ERROR] Unexpected error: {e}")
        return pd.DataFrame()

    @staticmethod
    def save_ipm_uhh_sp_to_db(df):
        """Saves the processed IPM UHH SP DataFrame to the database."""
        if df.empty:
            print("[WARNING] No IPM UHH SP data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            location_name = str(row['Kabupaten/Kota']).strip()
            if not location_name or "Sumber/Source" in location_name:
                continue

            location_type = IPM_UHH_SP.LocationType.MUNICIPALITY if location_name.startswith("KOTA") else IPM_UHH_SP.LocationType.REGENCY

            data_to_serialize = {
                'location_name': location_name,
                'location_type': location_type.value,
                'year': row['Tahun'],
                'value': row['Value']
            }

            try:
                instance = IPM_UHH_SP.objects.get(location_name=location_name, year=row['Tahun'])
            except IPM_UHH_SP.DoesNotExist:
                instance = None

            serializer = IPM_UHH_SPSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan IPM UHH SP untuk {location_name} tahun {row['Tahun']}: {serializer.errors}")

        print(f"[INFO] Total IPM UHH SP records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_ipm_uhh_sp(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_ipm_uhh_sp_data()
        created_count, updated_count = cls.save_ipm_uhh_sp_to_db(df)
        return created_count, updated_count

class IPM_HLSService:
    @staticmethod
    def fetch_ipm_hls_data():
        """Fetches and processes IPM HLS data from Google Sheets into a long-format DataFrame."""
        print("[INFO] Fetching IPM HLS data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("IPM_HLS_Y-to-Y")

            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # --- Data Cleaning and Transformation ---
            # Rename column to standard name - handle various column name formats
            if 'Kabupaten/Kota\nRegency/Municipality' in df.columns:
                df = df.rename(columns={'Kabupaten/Kota\nRegency/Municipality': 'Kabupaten/Kota'})
            elif 'Kabupaten/Kot' in df.columns:
                df = df.rename(columns={'Kabupaten/Kot': 'Kabupaten/Kota'})
            elif 'Provinsi' in df.columns:
                df = df.rename(columns={'Provinsi': 'Kabupaten/Kota'})
                print(f"[INFO] Found and renamed location column: 'Provinsi' -> 'Kabupaten/Kota'")
            elif 'Kabupaten/Kota' not in df.columns:
                # Try to find the location column
                location_col_found = False
                for col in df.columns:
                    col_lower = str(col).lower()
                    if 'kabupaten' in col_lower or 'kota' in col_lower or 'kot' in col_lower or 'provinsi' in col_lower:
                        df = df.rename(columns={col: 'Kabupaten/Kota'})
                        location_col_found = True
                        print(f"[INFO] Found and renamed location column: '{col}' -> 'Kabupaten/Kota'")
                        break
                
                if not location_col_found:
                    print(f"[ERROR] Location column not found. Available columns: {df.columns.tolist()}")
                    return pd.DataFrame()
            
            # Remove empty columns
            empty_columns = [col for col in df.columns if col == '']
            if empty_columns:
                df = df.drop(columns=empty_columns)

            # Find year columns
            year_columns = [col for col in df.columns if col.isdigit() and len(col) == 4]
            if not year_columns:
                # Try to find year columns that might be strings
                for col in df.columns:
                    if col != 'Kabupaten/Kota' and str(col).strip().isdigit():
                        year_columns.append(col)
            
            if not year_columns:
                print("[ERROR] No year columns found")
                return pd.DataFrame()

            # Melt DataFrame to long format
            try:
                df_melted = pd.melt(df, id_vars=['Kabupaten/Kota'], value_vars=year_columns,
                                    var_name='Tahun', value_name='Value')
            except KeyError as e:
                print(f"[ERROR] Error during melt operation: {e}")
                print(f"[DEBUG] Available columns: {df.columns.tolist()}")
                return pd.DataFrame()

            # Convert data types
            df_melted['Tahun'] = pd.to_numeric(df_melted['Tahun'], errors='coerce')
            
            # Convert value to numeric, handling both comma and dot as decimal separator
            # Handle both formats: "12.34" (dot) and "12,34" (comma)
            df_melted['Value'] = df_melted['Value'].astype(str)
            # Replace comma with dot for decimal separator
            df_melted['Value'] = df_melted['Value'].str.replace(',', '.', regex=False)
            # Convert to numeric
            df_melted['Value'] = pd.to_numeric(df_melted['Value'], errors='coerce')
            
            # Remove rows with invalid/empty values
            df_melted.dropna(subset=['Value'], inplace=True)
            df_melted.dropna(subset=['Tahun'], inplace=True)

            print(f"[OK] Data processed. Total valid records: {len(df_melted)}")
            return df_melted

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'IPM_HLS_Y-to-Y' not found.")
            print(f"[ERROR] Worksheet 'IPM_HLS_Y-to-Y' not found.")
        except KeyError as e:
            logger.error(f"KeyError while processing IPM HLS data: {e}")
            print(f"[ERROR] KeyError: {e}")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing IPM HLS data: {e}")
            print(f"[ERROR] Unexpected error: {e}")
        return pd.DataFrame()

    @staticmethod
    def save_ipm_hls_to_db(df):
        """Saves the processed IPM HLS DataFrame to the database."""
        if df.empty:
            print("[WARNING] No IPM HLS data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            location_name = str(row['Kabupaten/Kota']).strip()
            if not location_name or "Sumber/Source" in location_name:
                continue

            location_type = IPM_HLS.LocationType.MUNICIPALITY if location_name.startswith("KOTA") else IPM_HLS.LocationType.REGENCY

            data_to_serialize = {
                'location_name': location_name,
                'location_type': location_type.value,
                'year': row['Tahun'],
                'value': row['Value']
            }

            try:
                instance = IPM_HLS.objects.get(location_name=location_name, year=row['Tahun'])
            except IPM_HLS.DoesNotExist:
                instance = None

            serializer = IPM_HLSSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan IPM HLS untuk {location_name} tahun {row['Tahun']}: {serializer.errors}")

        print(f"[INFO] Total IPM HLS records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_ipm_hls(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_ipm_hls_data()
        created_count, updated_count = cls.save_ipm_hls_to_db(df)
        return created_count, updated_count

class IPM_RLSService:
    @staticmethod
    def fetch_ipm_rls_data():
        """Fetches and processes IPM RLS data from Google Sheets into a long-format DataFrame."""
        print("[INFO] Fetching IPM RLS data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("IPM_RLS_Y-to-Y")

            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # --- Data Cleaning and Transformation ---
            # Rename column to standard name - handle various column name formats
            if 'Kabupaten/Kota\nRegency/Municipality' in df.columns:
                df = df.rename(columns={'Kabupaten/Kota\nRegency/Municipality': 'Kabupaten/Kota'})
            elif 'Kabupaten/Kot' in df.columns:
                df = df.rename(columns={'Kabupaten/Kot': 'Kabupaten/Kota'})
            elif 'Provinsi' in df.columns:
                df = df.rename(columns={'Provinsi': 'Kabupaten/Kota'})
                print(f"[INFO] Found and renamed location column: 'Provinsi' -> 'Kabupaten/Kota'")
            elif 'Kabupaten/Kota' not in df.columns:
                # Try to find the location column
                location_col_found = False
                for col in df.columns:
                    col_lower = str(col).lower()
                    if 'kabupaten' in col_lower or 'kota' in col_lower or 'kot' in col_lower or 'provinsi' in col_lower:
                        df = df.rename(columns={col: 'Kabupaten/Kota'})
                        location_col_found = True
                        print(f"[INFO] Found and renamed location column: '{col}' -> 'Kabupaten/Kota'")
                        break
                
                if not location_col_found:
                    print(f"[ERROR] Location column not found. Available columns: {df.columns.tolist()}")
                    return pd.DataFrame()
            
            # Remove empty columns
            empty_columns = [col for col in df.columns if col == '']
            if empty_columns:
                df = df.drop(columns=empty_columns)

            # Find year columns
            year_columns = [col for col in df.columns if col.isdigit() and len(col) == 4]
            if not year_columns:
                # Try to find year columns that might be strings
                for col in df.columns:
                    if col != 'Kabupaten/Kota' and str(col).strip().isdigit():
                        year_columns.append(col)
            
            if not year_columns:
                print("[ERROR] No year columns found")
                return pd.DataFrame()

            # Melt DataFrame to long format
            try:
                df_melted = pd.melt(df, id_vars=['Kabupaten/Kota'], value_vars=year_columns,
                                    var_name='Tahun', value_name='Value')
            except KeyError as e:
                print(f"[ERROR] Error during melt operation: {e}")
                print(f"[DEBUG] Available columns: {df.columns.tolist()}")
                return pd.DataFrame()

            # Convert data types
            df_melted['Tahun'] = pd.to_numeric(df_melted['Tahun'], errors='coerce')
            
            # Convert value to numeric, handling both comma and dot as decimal separator
            # Handle both formats: "12.34" (dot) and "12,34" (comma)
            df_melted['Value'] = df_melted['Value'].astype(str)
            # Replace comma with dot for decimal separator
            df_melted['Value'] = df_melted['Value'].str.replace(',', '.', regex=False)
            # Convert to numeric
            df_melted['Value'] = pd.to_numeric(df_melted['Value'], errors='coerce')
            
            # Remove rows with invalid/empty values
            df_melted.dropna(subset=['Value'], inplace=True)
            df_melted.dropna(subset=['Tahun'], inplace=True)

            print(f"[OK] Data processed. Total valid records: {len(df_melted)}")
            return df_melted

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'IPM_RLS_Y-to-Y' not found.")
            print(f"[ERROR] Worksheet 'IPM_RLS_Y-to-Y' not found.")
        except KeyError as e:
            logger.error(f"KeyError while processing IPM RLS data: {e}")
            print(f"[ERROR] KeyError: {e}")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing IPM RLS data: {e}")
            print(f"[ERROR] Unexpected error: {e}")
        return pd.DataFrame()

    @staticmethod
    def save_ipm_rls_to_db(df):
        """Saves the processed IPM RLS DataFrame to the database."""
        if df.empty:
            print("[WARNING] No IPM RLS data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            location_name = str(row['Kabupaten/Kota']).strip()
            if not location_name or "Sumber/Source" in location_name:
                continue

            location_type = IPM_RLS.LocationType.MUNICIPALITY if location_name.startswith("KOTA") else IPM_RLS.LocationType.REGENCY

            data_to_serialize = {
                'location_name': location_name,
                'location_type': location_type.value,
                'year': row['Tahun'],
                'value': row['Value']
            }

            try:
                instance = IPM_RLS.objects.get(location_name=location_name, year=row['Tahun'])
            except IPM_RLS.DoesNotExist:
                instance = None

            serializer = IPM_RLSSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan IPM RLS untuk {location_name} tahun {row['Tahun']}: {serializer.errors}")

        print(f"[INFO] Total IPM RLS records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_ipm_rls(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_ipm_rls_data()
        created_count, updated_count = cls.save_ipm_rls_to_db(df)
        return created_count, updated_count

class IPM_PengeluaranPerKapitaService:
    @staticmethod
    def fetch_ipm_pengeluaran_per_kapita_data():
        """Fetches and processes IPM Pengeluaran per Kapita data from Google Sheets into a long-format DataFrame."""
        print("[INFO] Fetching IPM Pengeluaran per Kapita data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("IPM_Pengeluaran per kapita_Y-to-Y")

            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # --- Data Cleaning and Transformation ---
            # Rename column to standard name - handle various column name formats
            if 'Kabupaten/Kota\nRegency/Municipality' in df.columns:
                df = df.rename(columns={'Kabupaten/Kota\nRegency/Municipality': 'Kabupaten/Kota'})
            elif 'Kabupaten/Kot' in df.columns:
                df = df.rename(columns={'Kabupaten/Kot': 'Kabupaten/Kota'})
            elif 'Provinsi' in df.columns:
                df = df.rename(columns={'Provinsi': 'Kabupaten/Kota'})
                print(f"[INFO] Found and renamed location column: 'Provinsi' -> 'Kabupaten/Kota'")
            elif 'Kabupaten/Kota' not in df.columns:
                # Try to find the location column
                location_col_found = False
                for col in df.columns:
                    col_lower = str(col).lower()
                    if 'kabupaten' in col_lower or 'kota' in col_lower or 'kot' in col_lower or 'provinsi' in col_lower:
                        df = df.rename(columns={col: 'Kabupaten/Kota'})
                        location_col_found = True
                        print(f"[INFO] Found and renamed location column: '{col}' -> 'Kabupaten/Kota'")
                        break
                
                if not location_col_found:
                    print(f"[ERROR] Location column not found. Available columns: {df.columns.tolist()}")
                    return pd.DataFrame()
            
            # Remove empty columns
            empty_columns = [col for col in df.columns if col == '']
            if empty_columns:
                df = df.drop(columns=empty_columns)

            # Find year columns
            year_columns = [col for col in df.columns if col.isdigit() and len(col) == 4]
            if not year_columns:
                # Try to find year columns that might be strings
                for col in df.columns:
                    if col != 'Kabupaten/Kota' and str(col).strip().isdigit():
                        year_columns.append(col)
            
            if not year_columns:
                print("[ERROR] No year columns found")
                return pd.DataFrame()

            # Melt DataFrame to long format
            try:
                df_melted = pd.melt(df, id_vars=['Kabupaten/Kota'], value_vars=year_columns,
                                    var_name='Tahun', value_name='Value')
            except KeyError as e:
                print(f"[ERROR] Error during melt operation: {e}")
                print(f"[DEBUG] Available columns: {df.columns.tolist()}")
                return pd.DataFrame()

            # Convert data types
            df_melted['Tahun'] = pd.to_numeric(df_melted['Tahun'], errors='coerce')
            
            # Convert value to numeric, handling both comma and dot as decimal separator
            # Handle both formats: "12.34" (dot) and "12,34" (comma)
            df_melted['Value'] = df_melted['Value'].astype(str)
            # Replace comma with dot for decimal separator
            df_melted['Value'] = df_melted['Value'].str.replace(',', '.', regex=False)
            # Convert to numeric
            df_melted['Value'] = pd.to_numeric(df_melted['Value'], errors='coerce')
            
            # Remove rows with invalid/empty values
            df_melted.dropna(subset=['Value'], inplace=True)
            df_melted.dropna(subset=['Tahun'], inplace=True)

            print(f"[OK] Data processed. Total valid records: {len(df_melted)}")
            return df_melted

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'IPM_Pengeluaran per kapita_Y-to-Y' not found.")
            print(f"[ERROR] Worksheet 'IPM_Pengeluaran per kapita_Y-to-Y' not found.")
        except KeyError as e:
            logger.error(f"KeyError while processing IPM Pengeluaran per Kapita data: {e}")
            print(f"[ERROR] KeyError: {e}")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing IPM Pengeluaran per Kapita data: {e}")
            print(f"[ERROR] Unexpected error: {e}")
        return pd.DataFrame()

    @staticmethod
    def save_ipm_pengeluaran_per_kapita_to_db(df):
        """Saves the processed IPM Pengeluaran per Kapita DataFrame to the database."""
        if df.empty:
            print("[WARNING] No IPM Pengeluaran per Kapita data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            location_name = str(row['Kabupaten/Kota']).strip()
            if not location_name or "Sumber/Source" in location_name:
                continue

            location_type = IPM_PengeluaranPerKapita.LocationType.MUNICIPALITY if location_name.startswith("KOTA") else IPM_PengeluaranPerKapita.LocationType.REGENCY

            # Round value to 2 decimal places to match model's decimal_places=2
            value = round(float(row['Value']), 2) if pd.notna(row['Value']) else None

            data_to_serialize = {
                'location_name': location_name,
                'location_type': location_type.value,
                'year': row['Tahun'],
                'value': value
            }

            try:
                instance = IPM_PengeluaranPerKapita.objects.get(location_name=location_name, year=row['Tahun'])
            except IPM_PengeluaranPerKapita.DoesNotExist:
                instance = None

            serializer = IPM_PengeluaranPerKapitaSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan IPM Pengeluaran per Kapita untuk {location_name} tahun {row['Tahun']}: {serializer.errors}")

        print(f"[INFO] Total IPM Pengeluaran per Kapita records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_ipm_pengeluaran_per_kapita(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_ipm_pengeluaran_per_kapita_data()
        created_count, updated_count = cls.save_ipm_pengeluaran_per_kapita_to_db(df)
        return created_count, updated_count

class IPM_IndeksKesehatanService:
    @staticmethod
    def fetch_ipm_indeks_kesehatan_data():
        """Fetches and processes IPM Indeks Kesehatan data from Google Sheets into a long-format DataFrame."""
        print("[INFO] Fetching IPM Indeks Kesehatan data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("IPM_Indeks Kesehatan_Y-to-Y")

            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # --- Data Cleaning and Transformation ---
            # Rename column to standard name - handle various column name formats
            if 'Kabupaten/Kota\nRegency/Municipality' in df.columns:
                df = df.rename(columns={'Kabupaten/Kota\nRegency/Municipality': 'Kabupaten/Kota'})
            elif 'Kabupaten/Kot' in df.columns:
                df = df.rename(columns={'Kabupaten/Kot': 'Kabupaten/Kota'})
            elif 'Provinsi' in df.columns:
                df = df.rename(columns={'Provinsi': 'Kabupaten/Kota'})
                print(f"[INFO] Found and renamed location column: 'Provinsi' -> 'Kabupaten/Kota'")
            elif 'Kabupaten/Kota' not in df.columns:
                # Try to find the location column
                location_col_found = False
                for col in df.columns:
                    col_lower = str(col).lower()
                    if 'kabupaten' in col_lower or 'kota' in col_lower or 'kot' in col_lower or 'provinsi' in col_lower:
                        df = df.rename(columns={col: 'Kabupaten/Kota'})
                        location_col_found = True
                        print(f"[INFO] Found and renamed location column: '{col}' -> 'Kabupaten/Kota'")
                        break
                
                if not location_col_found:
                    print(f"[ERROR] Location column not found. Available columns: {df.columns.tolist()}")
                    return pd.DataFrame()
            
            # Remove empty columns
            empty_columns = [col for col in df.columns if col == '']
            if empty_columns:
                df = df.drop(columns=empty_columns)

            # Find year columns
            year_columns = [col for col in df.columns if col.isdigit() and len(col) == 4]
            if not year_columns:
                # Try to find year columns that might be strings
                for col in df.columns:
                    if col != 'Kabupaten/Kota' and str(col).strip().isdigit():
                        year_columns.append(col)
            
            if not year_columns:
                print("[ERROR] No year columns found")
                return pd.DataFrame()

            # Melt DataFrame to long format
            try:
                df_melted = pd.melt(df, id_vars=['Kabupaten/Kota'], value_vars=year_columns,
                                    var_name='Tahun', value_name='Value')
            except KeyError as e:
                print(f"[ERROR] Error during melt operation: {e}")
                print(f"[DEBUG] Available columns: {df.columns.tolist()}")
                return pd.DataFrame()

            # Convert data types
            df_melted['Tahun'] = pd.to_numeric(df_melted['Tahun'], errors='coerce')
            
            # Convert value to numeric, handling both comma and dot as decimal separator
            # Handle both formats: "12.34" (dot) and "12,34" (comma)
            df_melted['Value'] = df_melted['Value'].astype(str)
            # Replace comma with dot for decimal separator
            df_melted['Value'] = df_melted['Value'].str.replace(',', '.', regex=False)
            # Convert to numeric
            df_melted['Value'] = pd.to_numeric(df_melted['Value'], errors='coerce')
            
            # Remove rows with invalid/empty values
            df_melted.dropna(subset=['Value'], inplace=True)
            df_melted.dropna(subset=['Tahun'], inplace=True)

            print(f"[OK] Data processed. Total valid records: {len(df_melted)}")
            return df_melted

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'IPM_Indeks Kesehatan_Y-to-Y' not found.")
            print(f"[ERROR] Worksheet 'IPM_Indeks Kesehatan_Y-to-Y' not found.")
        except KeyError as e:
            logger.error(f"KeyError while processing IPM Indeks Kesehatan data: {e}")
            print(f"[ERROR] KeyError: {e}")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing IPM Indeks Kesehatan data: {e}")
            print(f"[ERROR] Unexpected error: {e}")
        return pd.DataFrame()

    @staticmethod
    def save_ipm_indeks_kesehatan_to_db(df):
        """Saves the processed IPM Indeks Kesehatan DataFrame to the database."""
        if df.empty:
            print("[WARNING] No IPM Indeks Kesehatan data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            location_name = str(row['Kabupaten/Kota']).strip()
            if not location_name or "Sumber/Source" in location_name:
                continue

            location_type = IPM_IndeksKesehatan.LocationType.MUNICIPALITY if location_name.startswith("KOTA") else IPM_IndeksKesehatan.LocationType.REGENCY

            data_to_serialize = {
                'location_name': location_name,
                'location_type': location_type.value,
                'year': row['Tahun'],
                'value': row['Value']
            }

            try:
                instance = IPM_IndeksKesehatan.objects.get(location_name=location_name, year=row['Tahun'])
            except IPM_IndeksKesehatan.DoesNotExist:
                instance = None

            serializer = IPM_IndeksKesehatanSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan IPM Indeks Kesehatan untuk {location_name} tahun {row['Tahun']}: {serializer.errors}")

        print(f"[INFO] Total IPM Indeks Kesehatan records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_ipm_indeks_kesehatan(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_ipm_indeks_kesehatan_data()
        created_count, updated_count = cls.save_ipm_indeks_kesehatan_to_db(df)
        return created_count, updated_count

class IPM_IndeksHidupLayakService:
    @staticmethod
    def fetch_ipm_indeks_hidup_layak_data():
        """Fetches and processes IPM Indeks Hidup Layak data from Google Sheets into a long-format DataFrame."""
        print("[INFO] Fetching IPM Indeks Hidup Layak data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("IPM_Indeks Hidup Layak_Y-to-Y")

            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # --- Data Cleaning and Transformation ---
            # Rename column to standard name - handle various column name formats
            if 'Kabupaten/Kota\nRegency/Municipality' in df.columns:
                df = df.rename(columns={'Kabupaten/Kota\nRegency/Municipality': 'Kabupaten/Kota'})
            elif 'Kabupaten/Kot' in df.columns:
                df = df.rename(columns={'Kabupaten/Kot': 'Kabupaten/Kota'})
            elif 'Provinsi' in df.columns:
                df = df.rename(columns={'Provinsi': 'Kabupaten/Kota'})
                print(f"[INFO] Found and renamed location column: 'Provinsi' -> 'Kabupaten/Kota'")
            elif 'Kabupaten/Kota' not in df.columns:
                # Try to find the location column
                location_col_found = False
                for col in df.columns:
                    col_lower = str(col).lower()
                    if 'kabupaten' in col_lower or 'kota' in col_lower or 'kot' in col_lower or 'provinsi' in col_lower:
                        df = df.rename(columns={col: 'Kabupaten/Kota'})
                        location_col_found = True
                        print(f"[INFO] Found and renamed location column: '{col}' -> 'Kabupaten/Kota'")
                        break
                
                if not location_col_found:
                    print(f"[ERROR] Location column not found. Available columns: {df.columns.tolist()}")
                    return pd.DataFrame()
            
            # Remove empty columns
            empty_columns = [col for col in df.columns if col == '']
            if empty_columns:
                df = df.drop(columns=empty_columns)

            # Find year columns
            year_columns = [col for col in df.columns if col.isdigit() and len(col) == 4]
            if not year_columns:
                # Try to find year columns that might be strings
                for col in df.columns:
                    if col != 'Kabupaten/Kota' and str(col).strip().isdigit():
                        year_columns.append(col)
            
            if not year_columns:
                print("[ERROR] No year columns found")
                return pd.DataFrame()

            # Melt DataFrame to long format
            try:
                df_melted = pd.melt(df, id_vars=['Kabupaten/Kota'], value_vars=year_columns,
                                    var_name='Tahun', value_name='Value')
            except KeyError as e:
                print(f"[ERROR] Error during melt operation: {e}")
                print(f"[DEBUG] Available columns: {df.columns.tolist()}")
                return pd.DataFrame()

            # Convert data types
            df_melted['Tahun'] = pd.to_numeric(df_melted['Tahun'], errors='coerce')
            
            # Convert value to numeric, handling both comma and dot as decimal separator
            # Handle both formats: "12.34" (dot) and "12,34" (comma)
            df_melted['Value'] = df_melted['Value'].astype(str)
            # Replace comma with dot for decimal separator
            df_melted['Value'] = df_melted['Value'].str.replace(',', '.', regex=False)
            # Convert to numeric
            df_melted['Value'] = pd.to_numeric(df_melted['Value'], errors='coerce')
            
            # Remove rows with invalid/empty values
            df_melted.dropna(subset=['Value'], inplace=True)
            df_melted.dropna(subset=['Tahun'], inplace=True)

            print(f"[OK] Data processed. Total valid records: {len(df_melted)}")
            return df_melted

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'IPM_Indeks Hidup Layak_Y-to-Y' not found.")
            print(f"[ERROR] Worksheet 'IPM_Indeks Hidup Layak_Y-to-Y' not found.")
        except KeyError as e:
            logger.error(f"KeyError while processing IPM Indeks Hidup Layak data: {e}")
            print(f"[ERROR] KeyError: {e}")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing IPM Indeks Hidup Layak data: {e}")
            print(f"[ERROR] Unexpected error: {e}")
        return pd.DataFrame()

    @staticmethod
    def save_ipm_indeks_hidup_layak_to_db(df):
        """Saves the processed IPM Indeks Hidup Layak DataFrame to the database."""
        if df.empty:
            print("[WARNING] No IPM Indeks Hidup Layak data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            location_name = str(row['Kabupaten/Kota']).strip()
            if not location_name or "Sumber/Source" in location_name:
                continue

            location_type = IPM_IndeksHidupLayak.LocationType.MUNICIPALITY if location_name.startswith("KOTA") else IPM_IndeksHidupLayak.LocationType.REGENCY

            data_to_serialize = {
                'location_name': location_name,
                'location_type': location_type.value,
                'year': row['Tahun'],
                'value': row['Value']
            }

            try:
                instance = IPM_IndeksHidupLayak.objects.get(location_name=location_name, year=row['Tahun'])
            except IPM_IndeksHidupLayak.DoesNotExist:
                instance = None

            serializer = IPM_IndeksHidupLayakSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan IPM Indeks Hidup Layak untuk {location_name} tahun {row['Tahun']}: {serializer.errors}")

        print(f"[INFO] Total IPM Indeks Hidup Layak records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_ipm_indeks_hidup_layak(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_ipm_indeks_hidup_layak_data()
        created_count, updated_count = cls.save_ipm_indeks_hidup_layak_to_db(df)
        return created_count, updated_count

class IPM_IndeksPendidikanService:
    @staticmethod
    def fetch_ipm_indeks_pendidikan_data():
        """Fetches and processes IPM Indeks Pendidikan data from Google Sheets into a long-format DataFrame."""
        print("[INFO] Fetching IPM Indeks Pendidikan data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("IPM_Indeks Pendidikan_Y-to-Y")

            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # --- Data Cleaning and Transformation ---
            # Rename column to standard name - handle various column name formats
            if 'Kabupaten/Kota\nRegency/Municipality' in df.columns:
                df = df.rename(columns={'Kabupaten/Kota\nRegency/Municipality': 'Kabupaten/Kota'})
            elif 'Kabupaten/Kot' in df.columns:
                df = df.rename(columns={'Kabupaten/Kot': 'Kabupaten/Kota'})
            elif 'Provinsi' in df.columns:
                df = df.rename(columns={'Provinsi': 'Kabupaten/Kota'})
                print(f"[INFO] Found and renamed location column: 'Provinsi' -> 'Kabupaten/Kota'")
            elif 'Kabupaten/Kota' not in df.columns:
                # Try to find the location column
                location_col_found = False
                for col in df.columns:
                    col_lower = str(col).lower()
                    if 'kabupaten' in col_lower or 'kota' in col_lower or 'kot' in col_lower or 'provinsi' in col_lower:
                        df = df.rename(columns={col: 'Kabupaten/Kota'})
                        location_col_found = True
                        print(f"[INFO] Found and renamed location column: '{col}' -> 'Kabupaten/Kota'")
                        break
                
                if not location_col_found:
                    print(f"[ERROR] Location column not found. Available columns: {df.columns.tolist()}")
                    return pd.DataFrame()
            
            # Remove empty columns
            empty_columns = [col for col in df.columns if col == '']
            if empty_columns:
                df = df.drop(columns=empty_columns)

            # Find year columns
            year_columns = [col for col in df.columns if col.isdigit() and len(col) == 4]
            if not year_columns:
                # Try to find year columns that might be strings
                for col in df.columns:
                    if col != 'Kabupaten/Kota' and str(col).strip().isdigit():
                        year_columns.append(col)
            
            if not year_columns:
                print("[ERROR] No year columns found")
                return pd.DataFrame()

            # Melt DataFrame to long format
            try:
                df_melted = pd.melt(df, id_vars=['Kabupaten/Kota'], value_vars=year_columns,
                                    var_name='Tahun', value_name='Value')
            except KeyError as e:
                print(f"[ERROR] Error during melt operation: {e}")
                print(f"[DEBUG] Available columns: {df.columns.tolist()}")
                return pd.DataFrame()

            # Convert data types
            df_melted['Tahun'] = pd.to_numeric(df_melted['Tahun'], errors='coerce')
            
            # Convert value to numeric, handling both comma and dot as decimal separator
            # Handle both formats: "12.34" (dot) and "12,34" (comma)
            df_melted['Value'] = df_melted['Value'].astype(str)
            # Replace comma with dot for decimal separator
            df_melted['Value'] = df_melted['Value'].str.replace(',', '.', regex=False)
            # Convert to numeric
            df_melted['Value'] = pd.to_numeric(df_melted['Value'], errors='coerce')
            
            # Remove rows with invalid/empty values
            df_melted.dropna(subset=['Value'], inplace=True)
            df_melted.dropna(subset=['Tahun'], inplace=True)

            print(f"[OK] Data processed. Total valid records: {len(df_melted)}")
            return df_melted

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'IPM_Indeks Pendidikan_Y-to-Y' not found.")
            print(f"[ERROR] Worksheet 'IPM_Indeks Pendidikan_Y-to-Y' not found.")
        except KeyError as e:
            logger.error(f"KeyError while processing IPM Indeks Pendidikan data: {e}")
            print(f"[ERROR] KeyError: {e}")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing IPM Indeks Pendidikan data: {e}")
            print(f"[ERROR] Unexpected error: {e}")
        return pd.DataFrame()

    @staticmethod
    def save_ipm_indeks_pendidikan_to_db(df):
        """Saves the processed IPM Indeks Pendidikan DataFrame to the database."""
        if df.empty:
            print("[WARNING] No IPM Indeks Pendidikan data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            location_name = str(row['Kabupaten/Kota']).strip()
            if not location_name or "Sumber/Source" in location_name:
                continue

            location_type = IPM_IndeksPendidikan.LocationType.MUNICIPALITY if location_name.startswith("KOTA") else IPM_IndeksPendidikan.LocationType.REGENCY

            data_to_serialize = {
                'location_name': location_name,
                'location_type': location_type.value,
                'year': row['Tahun'],
                'value': row['Value']
            }

            try:
                instance = IPM_IndeksPendidikan.objects.get(location_name=location_name, year=row['Tahun'])
            except IPM_IndeksPendidikan.DoesNotExist:
                instance = None

            serializer = IPM_IndeksPendidikanSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan IPM Indeks Pendidikan untuk {location_name} tahun {row['Tahun']}: {serializer.errors}")

        print(f"[INFO] Total IPM Indeks Pendidikan records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_ipm_indeks_pendidikan(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_ipm_indeks_pendidikan_data()
        created_count, updated_count = cls.save_ipm_indeks_pendidikan_to_db(df)
        return created_count, updated_count

def convert_value_to_numeric(value):
    """
    Convert value to numeric, handling:
    - "-" -> 0
    - Both "." and "," as decimal separators
    - "." as thousand separator (Indonesian format: 1.234.567)
    - "," as decimal separator (Indonesian format: 1.234,56)
    - Empty strings -> None
    """
    if value is None or value == '' or str(value).strip() == '':
        return None
    
    value_str = str(value).strip()
    
    # Replace "-" with 0
    if value_str == '-' or value_str == '—':
        return 0
    
    # Indonesian number format handling:
    # - Dots (.) are thousand separators: 1.234.567 = 1234567
    # - Commas (,) are decimal separators: 1.234,56 = 1234.56
    # - If both exist: 1.234.567,89 = 1234567.89
    
    # Check if comma exists (decimal separator)
    has_comma = ',' in value_str
    has_dot = '.' in value_str
    
    if has_comma and has_dot:
        # Both exist: dots are thousands, comma is decimal
        # Example: "1.234.567,89" -> "1234567.89"
        value_str = value_str.replace('.', '')  # Remove thousand separators (dots)
        value_str = value_str.replace(',', '.')  # Replace comma with dot for decimal
    elif has_comma and not has_dot:
        # Only comma: it's decimal separator
        # Example: "1234,56" -> "1234.56"
        value_str = value_str.replace(',', '.')
    elif has_dot and not has_comma:
        # Only dots: check if it's thousand separator or decimal
        # If multiple dots or dot is not at the end (last 3 digits), it's thousand separator
        dot_count = value_str.count('.')
        if dot_count > 1:
            # Multiple dots = thousand separators
            # Example: "1.234.567" -> "1234567"
            value_str = value_str.replace('.', '')
        else:
            # Single dot: check position
            # If dot is followed by exactly 3 digits at the end, might be thousand separator
            # Otherwise, assume it's decimal
            dot_pos = value_str.rfind('.')
            if dot_pos > 0 and len(value_str) - dot_pos - 1 == 3:
                # Format like "1234.567" - could be thousand or decimal
                # For population data, it's more likely thousand separator
                # But we'll be conservative and treat as decimal if only one dot
                pass  # Keep as is, will be treated as decimal
            # If dot is at position where it's clearly decimal (last 2 digits), keep it
            # Otherwise, might be thousand separator
            if dot_pos > 0 and len(value_str) - dot_pos - 1 > 3:
                # More than 3 digits after dot = likely thousand separator
                value_str = value_str.replace('.', '')
    
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return None

# ========== Kemiskinan Services ==========

class KemiskinanSurabayaService:
    @staticmethod
    def fetch_kemiskinan_surabaya_data():
        """Fetches and processes Kemiskinan Surabaya data from Google Sheets."""
        print("[INFO] Fetching Kemiskinan Surabaya data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("Kemiskinan(Surabaya)_YtoY")

            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # Find indicator row (first column should contain indicators)
            # Find year columns (columns that are 4-digit numbers)
            year_columns = [col for col in df.columns if str(col).strip().isdigit() and len(str(col).strip()) == 4]
            
            if not year_columns:
                print("[ERROR] No year columns found")
                return pd.DataFrame()
            
            # Find indicator column (first column)
            indicator_col = df.columns[0]
            
            # Melt DataFrame to long format
            df_melted = pd.melt(df, id_vars=[indicator_col], value_vars=year_columns,
                                var_name='Tahun', value_name='Value')
            
            # Convert year to numeric
            df_melted['Tahun'] = pd.to_numeric(df_melted['Tahun'], errors='coerce')
            
            # Convert value - handle "-" and both decimal formats
            df_melted['Value'] = df_melted['Value'].astype(str)
            df_melted['Value'] = df_melted['Value'].apply(convert_value_to_numeric)
            
            # Pivot to get indicators as columns
            df_pivot = df_melted.pivot_table(index='Tahun', columns=indicator_col, values='Value', aggfunc='first').reset_index()
            
            # Map indicator names to model fields
            indicator_mapping = {
                'Jumlah Penduduk Miskin (Dlm 000)': 'jumlah_penduduk_miskin',
                'Persentase Penduduk Miskin': 'persentase_penduduk_miskin',
                'Indeks Kedalaman Kemiskinan (P1)': 'indeks_kedalaman_kemiskinan_p1',
                'Indeks Keparahan Kemiskinan (P2)': 'indeks_keparahan_kemiskinan_p2',
                'Garis Kemiskinan (Rp/Kapita/Bulan)': 'garis_kemiskinan',
            }
            
            # Rename columns based on mapping
            df_final = pd.DataFrame()
            df_final['Tahun'] = df_pivot['Tahun']
            
            for indicator, field_name in indicator_mapping.items():
                # Try to find matching column (case-insensitive)
                matching_col = None
                for col in df_pivot.columns:
                    if indicator.lower() in str(col).lower() or str(col).lower() in indicator.lower():
                        matching_col = col
                        break
                
                if matching_col and matching_col in df_pivot.columns:
                    df_final[field_name] = df_pivot[matching_col]
                else:
                    df_final[field_name] = None
            
            # Remove rows with invalid year
            df_final = df_final.dropna(subset=['Tahun'])
            
            print(f"[OK] Data processed. Total valid records: {len(df_final)}")
            return df_final

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'Kemiskinan(Surabaya)_YtoY' not found.")
            print(f"[ERROR] Worksheet 'Kemiskinan(Surabaya)_YtoY' not found.")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing Kemiskinan Surabaya data: {e}")
            print(f"[ERROR] {e}")
        return pd.DataFrame()

    @staticmethod
    def save_kemiskinan_surabaya_to_db(df):
        """Saves the processed Kemiskinan Surabaya DataFrame to the database."""
        if df.empty:
            print("[WARNING] No Kemiskinan Surabaya data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            year = int(row['Tahun']) if pd.notna(row['Tahun']) else None

            if not year:
                continue

            data_to_serialize = {
                'year': year,
                'jumlah_penduduk_miskin': row.get('jumlah_penduduk_miskin') if pd.notna(row.get('jumlah_penduduk_miskin')) else None,
                'persentase_penduduk_miskin': row.get('persentase_penduduk_miskin') if pd.notna(row.get('persentase_penduduk_miskin')) else None,
                'indeks_kedalaman_kemiskinan_p1': row.get('indeks_kedalaman_kemiskinan_p1') if pd.notna(row.get('indeks_kedalaman_kemiskinan_p1')) else None,
                'indeks_keparahan_kemiskinan_p2': row.get('indeks_keparahan_kemiskinan_p2') if pd.notna(row.get('indeks_keparahan_kemiskinan_p2')) else None,
                'garis_kemiskinan': row.get('garis_kemiskinan') if pd.notna(row.get('garis_kemiskinan')) else None,
            }

            try:
                instance = KemiskinanSurabaya.objects.get(year=year)
            except KemiskinanSurabaya.DoesNotExist:
                instance = None

            serializer = KemiskinanSurabayaSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan Kemiskinan Surabaya untuk tahun {year}: {serializer.errors}")

        print(f"[INFO] Total Kemiskinan Surabaya records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_kemiskinan_surabaya(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_kemiskinan_surabaya_data()
        created_count, updated_count = cls.save_kemiskinan_surabaya_to_db(df)
        return created_count, updated_count

class KemiskinanJawaTimurService:
    @staticmethod
    def fetch_kemiskinan_jawa_timur_data():
        """Fetches and processes Kemiskinan Jawa Timur data from Google Sheets."""
        print("[INFO] Fetching Kemiskinan Jawa Timur data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("Kemiskinan(JawaTimur)_YtoY_")

            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # Find indicator row (first column should contain indicators)
            # Find year columns (columns that are 4-digit numbers)
            year_columns = [col for col in df.columns if str(col).strip().isdigit() and len(str(col).strip()) == 4]
            
            if not year_columns:
                print("[ERROR] No year columns found")
                return pd.DataFrame()
            
            # Find indicator column (first column)
            indicator_col = df.columns[0]
            
            # Melt DataFrame to long format
            df_melted = pd.melt(df, id_vars=[indicator_col], value_vars=year_columns,
                                var_name='Tahun', value_name='Value')
            
            # Convert year to numeric
            df_melted['Tahun'] = pd.to_numeric(df_melted['Tahun'], errors='coerce')
            
            # Convert value - handle "-" and both decimal formats
            df_melted['Value'] = df_melted['Value'].astype(str)
            df_melted['Value'] = df_melted['Value'].apply(convert_value_to_numeric)
            
            # Pivot to get indicators as columns
            df_pivot = df_melted.pivot_table(index='Tahun', columns=indicator_col, values='Value', aggfunc='first').reset_index()
            
            # Map indicator names to model fields
            indicator_mapping = {
                'Jumlah Penduduk Miskin (Dlm 000)': 'jumlah_penduduk_miskin',
                'Persentase Penduduk Miskin': 'persentase_penduduk_miskin',
                'Indeks Kedalaman Kemiskinan (P1)': 'indeks_kedalaman_kemiskinan_p1',
                'Indeks Keparahan Kemiskinan (P2)': 'indeks_keparahan_kemiskinan_p2',
                'Garis Kemiskinan (Rp/Kapita/Bulan)': 'garis_kemiskinan',
            }
            
            # Rename columns based on mapping
            df_final = pd.DataFrame()
            df_final['Tahun'] = df_pivot['Tahun']
            
            for indicator, field_name in indicator_mapping.items():
                # Try to find matching column (case-insensitive)
                matching_col = None
                for col in df_pivot.columns:
                    if indicator.lower() in str(col).lower() or str(col).lower() in indicator.lower():
                        matching_col = col
                        break
                
                if matching_col and matching_col in df_pivot.columns:
                    df_final[field_name] = df_pivot[matching_col]
                else:
                    df_final[field_name] = None
            
            # Remove rows with invalid year
            df_final = df_final.dropna(subset=['Tahun'])
            
            print(f"[OK] Data processed. Total valid records: {len(df_final)}")
            return df_final

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'Kemiskinan(JawaTimur)_YtoY_' not found.")
            print(f"[ERROR] Worksheet 'Kemiskinan(JawaTimur)_YtoY_' not found.")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing Kemiskinan Jawa Timur data: {e}")
            print(f"[ERROR] {e}")
        return pd.DataFrame()

    @staticmethod
    def save_kemiskinan_jawa_timur_to_db(df):
        """Saves the processed Kemiskinan Jawa Timur DataFrame to the database."""
        if df.empty:
            print("[WARNING] No Kemiskinan Jawa Timur data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            year = int(row['Tahun']) if pd.notna(row['Tahun']) else None

            if not year:
                continue

            data_to_serialize = {
                'year': year,
                'jumlah_penduduk_miskin': row.get('jumlah_penduduk_miskin') if pd.notna(row.get('jumlah_penduduk_miskin')) else None,
                'persentase_penduduk_miskin': row.get('persentase_penduduk_miskin') if pd.notna(row.get('persentase_penduduk_miskin')) else None,
                'indeks_kedalaman_kemiskinan_p1': row.get('indeks_kedalaman_kemiskinan_p1') if pd.notna(row.get('indeks_kedalaman_kemiskinan_p1')) else None,
                'indeks_keparahan_kemiskinan_p2': row.get('indeks_keparahan_kemiskinan_p2') if pd.notna(row.get('indeks_keparahan_kemiskinan_p2')) else None,
                'garis_kemiskinan': row.get('garis_kemiskinan') if pd.notna(row.get('garis_kemiskinan')) else None,
            }

            try:
                instance = KemiskinanJawaTimur.objects.get(year=year)
            except KemiskinanJawaTimur.DoesNotExist:
                instance = None

            serializer = KemiskinanJawaTimurSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan Kemiskinan Jawa Timur untuk tahun {year}: {serializer.errors}")

        print(f"[INFO] Total Kemiskinan Jawa Timur records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_kemiskinan_jawa_timur(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_kemiskinan_jawa_timur_data()
        created_count, updated_count = cls.save_kemiskinan_jawa_timur_to_db(df)
        return created_count, updated_count

# ========== Helper function untuk convert nilai kependudukan ==========
def convert_kependudukan_value(value):
    """
    Convert value to numeric for population data.
    Data kependudukan nilainya adalah ribuan tapi tampilannya menggunakan "." dan ",".
    Jadi kita hapus semua "." dan "," karena hanya formatting.
    - "-" -> 0
    - Hapus semua "." dan "," (hanya formatting, bukan decimal separator)
    - Empty strings -> None
    """
    if value is None or value == '' or str(value).strip() == '':
        return None
    
    value_str = str(value).strip()
    
    # Replace "-" with 0
    if value_str == '-' or value_str == '—':
        return 0
    
    # Hapus semua "." dan "," karena hanya formatting (data sudah dalam ribuan)
    value_str = value_str.replace('.', '')
    value_str = value_str.replace(',', '')
    
    try:
        return int(float(value_str))  # Convert to int since population is whole numbers
    except (ValueError, TypeError):
        return None

# ========== Kependudukan Services ==========

class KependudukanService:
    @staticmethod
    def fetch_kependudukan_data():
        """Fetches and processes Kependudukan data from Google Sheets."""
        print("[INFO] Fetching Kependudukan data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("Kependudukan_gabungan")

            # Get all data
            data = sheet.get_all_values()
            if not data or len(data) < 3:
                print("[WARNING] No data found in sheet or insufficient rows")
                return pd.DataFrame()
            
            print(f"[OK] Raw data fetched with {len(data)} rows")
            
            # Row 0 (index 0) = Tahun headers (2015, 2016, etc.)
            # Row 1 (index 1) = Gender headers (LK, PR, Total) for each year
            # Row 2 onwards (index 2+) = Data rows with age groups
            
            year_row = data[0]  # First row: years
            gender_row = data[1]  # Second row: gender categories
            data_rows = data[2:]  # Third row onwards: actual data
            
            # Parse year and gender structure
            # Build a mapping: column_index -> (year, gender)
            # Format: Row 1 has years (may span multiple columns), Row 2 has gender categories
            column_mapping = {}
            current_year = None
            
            # First pass: identify years from row 1
            # Years might span multiple columns (e.g., "2015" in col B spans B, C, D)
            for col_idx in range(len(year_row)):
                year_val = str(year_row[col_idx]).strip() if col_idx < len(year_row) else ""
                
                # Check if this is a year (4-digit number)
                if year_val.isdigit() and len(year_val) == 4:
                    current_year = int(year_val)
                    # Year found, will be used for subsequent columns until next year
                
                # Second pass: identify gender from row 2
                if col_idx < len(gender_row):
                    gender_val = str(gender_row[col_idx]).strip().upper()
                    
                    # Check if this is a gender category
                    if gender_val in ['LK', 'PR', 'TOTAL'] and current_year:
                        # Map gender values
                        if gender_val == 'LK':
                            gender = 'LK'
                        elif gender_val == 'PR':
                            gender = 'PR'
                        elif gender_val == 'TOTAL':
                            gender = 'TOTAL'
                        else:
                            gender = None
                        
                        if gender:
                            column_mapping[col_idx] = (current_year, gender)
            
            if not column_mapping:
                print("[ERROR] Could not parse year and gender structure from headers")
                return pd.DataFrame()
            
            print(f"[OK] Found {len(column_mapping)} data columns")
            
            # Process data rows
            records = []
            for row_idx, row in enumerate(data_rows):
                if not row or len(row) == 0:
                    continue
                
                # First column is age group
                age_group = str(row[0]).strip() if len(row) > 0 else ""
                
                # Skip if empty or total row
                if not age_group or age_group.upper() in ['JUMLAH', 'TOTAL', '']:
                    continue
                
                # Process each data column
                for col_idx, (year, gender) in column_mapping.items():
                    if col_idx < len(row):
                        value_str = str(row[col_idx]).strip()
                        
                        # Convert value using kependudukan-specific function (removes all "." and ",")
                        population = convert_kependudukan_value(value_str)
                        
                        records.append({
                            'age_group': age_group,
                            'year': year,
                            'gender': gender,
                            'population': population
                        })
            
            df = pd.DataFrame(records)
            
            if df.empty:
                print("[WARNING] No valid records found after processing")
                return pd.DataFrame()
            
            # Remove rows with invalid data
            df = df.dropna(subset=['age_group', 'year', 'gender'])
            
            print(f"[OK] Data processed. Total valid records: {len(df)}")
            return df

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'Kependudukan_gabungan' not found.")
            print(f"[ERROR] Worksheet 'Kependudukan_gabungan' not found.")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing Kependudukan data: {e}")
            print(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()
        return pd.DataFrame()

    @staticmethod
    def save_kependudukan_to_db(df):
        """Saves the processed Kependudukan DataFrame to the database."""
        if df.empty:
            print("[WARNING] No Kependudukan data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            age_group = str(row['age_group']).strip() if pd.notna(row['age_group']) else None
            year = int(row['year']) if pd.notna(row['year']) else None
            gender = str(row['gender']).strip() if pd.notna(row['gender']) else None
            population = row['population'] if pd.notna(row['population']) and row['population'] != 0 else None

            if not age_group or not year or not gender:
                continue

            data_to_serialize = {
                'age_group': age_group,
                'year': year,
                'gender': gender,
                'population': population
            }

            try:
                instance = Kependudukan.objects.get(
                    age_group=age_group,
                    year=year,
                    gender=gender
                )
            except Kependudukan.DoesNotExist:
                instance = None

            serializer = KependudukanSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan Kependudukan untuk {age_group} {year} {gender}: {serializer.errors}")

        print(f"[INFO] Total Kependudukan records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_kependudukan(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_kependudukan_data()
        created_count, updated_count = cls.save_kependudukan_to_db(df)
        return created_count, updated_count

# ========== Ketenagakerjaan Services ==========

class KetenagakerjaanTPTService:
    @staticmethod
    def fetch_ketenagakerjaan_tpt_data():
        """Fetches and processes Ketenagakerjaan TPT data from Google Sheets."""
        print("[INFO] Fetching Ketenagakerjaan TPT data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("Ketenagakerjaan_TPT")

            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # Find columns: Tahun, Laki-Laki, Perempuan, Total
            year_col = None
            laki_laki_col = None
            perempuan_col = None
            total_col = None
            
            for col in df.columns:
                col_upper = str(col).upper().strip()
                if 'TAHUN' in col_upper or col_upper == 'YEAR' or col_upper == 'TAHUN':
                    year_col = col
                elif 'LAKI' in col_upper or 'LAKI-LAKI' in col_upper or 'MALE' in col_upper:
                    laki_laki_col = col
                elif 'PEREMPUAN' in col_upper or 'FEMALE' in col_upper:
                    perempuan_col = col
                elif 'TOTAL' in col_upper:
                    total_col = col
            
            # If columns not found by name, use position (A=Tahun, B=Laki-Laki, C=Perempuan, D=Total)
            if not year_col and len(df.columns) >= 1:
                year_col = df.columns[0]
            if not laki_laki_col and len(df.columns) >= 2:
                laki_laki_col = df.columns[1]
            if not perempuan_col and len(df.columns) >= 3:
                perempuan_col = df.columns[2]
            if not total_col and len(df.columns) >= 4:
                total_col = df.columns[3]
            
            if not year_col:
                print(f"[ERROR] Cannot identify year column. Found: {df.columns.tolist()}")
                return pd.DataFrame()
            
            # Create standardized dataframe
            df_clean = pd.DataFrame()
            df_clean['Tahun'] = df[year_col]
            
            if laki_laki_col:
                df_clean['Laki_Laki'] = df[laki_laki_col]
            else:
                df_clean['Laki_Laki'] = None
                
            if perempuan_col:
                df_clean['Perempuan'] = df[perempuan_col]
            else:
                df_clean['Perempuan'] = None
                
            if total_col:
                df_clean['Total'] = df[total_col]
            else:
                df_clean['Total'] = None
            
            # Convert year to numeric
            df_clean['Tahun'] = pd.to_numeric(df_clean['Tahun'], errors='coerce')
            
            # Convert values - handle "-" and both decimal formats
            for col in ['Laki_Laki', 'Perempuan', 'Total']:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].astype(str)
                    df_clean[col] = df_clean[col].apply(convert_value_to_numeric)
            
            # Remove rows with invalid year
            df_clean = df_clean.dropna(subset=['Tahun'])
            
            print(f"[OK] Data processed. Total valid records: {len(df_clean)}")
            return df_clean

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'Ketenagakerjaan_TPT' not found.")
            print(f"[ERROR] Worksheet 'Ketenagakerjaan_TPT' not found.")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing Ketenagakerjaan TPT data: {e}")
            print(f"[ERROR] {e}")
        return pd.DataFrame()

    @staticmethod
    def save_ketenagakerjaan_tpt_to_db(df):
        """Saves the processed Ketenagakerjaan TPT DataFrame to the database."""
        if df.empty:
            print("[WARNING] No Ketenagakerjaan TPT data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            year = int(row['Tahun']) if pd.notna(row['Tahun']) else None
            laki_laki = row.get('Laki_Laki') if pd.notna(row.get('Laki_Laki')) else None
            perempuan = row.get('Perempuan') if pd.notna(row.get('Perempuan')) else None
            total = row.get('Total') if pd.notna(row.get('Total')) else None

            if not year:
                continue

            data_to_serialize = {
                'year': year,
                'laki_laki': laki_laki,
                'perempuan': perempuan,
                'total': total
            }

            try:
                instance = KetenagakerjaanTPT.objects.get(year=year)
            except KetenagakerjaanTPT.DoesNotExist:
                instance = None

            serializer = KetenagakerjaanTPTSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan Ketenagakerjaan TPT untuk tahun {year}: {serializer.errors}")

        print(f"[INFO] Total Ketenagakerjaan TPT records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_ketenagakerjaan_tpt(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_ketenagakerjaan_tpt_data()
        created_count, updated_count = cls.save_ketenagakerjaan_tpt_to_db(df)
        return created_count, updated_count

class KetenagakerjaanTPAKService:
    @staticmethod
    def fetch_ketenagakerjaan_tpak_data():
        """Fetches and processes Ketenagakerjaan TPAK data from Google Sheets."""
        print("[INFO] Fetching Ketenagakerjaan TPAK data from Google Sheets...")
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)

            SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
            sheet = client.open_by_key(SHEET_ID).worksheet("Ketenagakerjaan_TPAK")

            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print("[WARNING] No data found in sheet")
                return pd.DataFrame()
            
            headers = data[0]
            data_rows = data[1:]
            df = pd.DataFrame(data_rows, columns=headers)
            print(f"[OK] Raw data fetched with shape: {df.shape}")

            # Find columns: Tahun, Laki-Laki, Perempuan, Total
            year_col = None
            laki_laki_col = None
            perempuan_col = None
            total_col = None
            
            for col in df.columns:
                col_upper = str(col).upper().strip()
                if 'TAHUN' in col_upper or col_upper == 'YEAR' or col_upper == 'TAHUN':
                    year_col = col
                elif 'LAKI' in col_upper or 'LAKI-LAKI' in col_upper or 'MALE' in col_upper:
                    laki_laki_col = col
                elif 'PEREMPUAN' in col_upper or 'FEMALE' in col_upper:
                    perempuan_col = col
                elif 'TOTAL' in col_upper:
                    total_col = col
            
            # If columns not found by name, use position (A=Tahun, B=Laki-Laki, C=Perempuan, D=Total)
            if not year_col and len(df.columns) >= 1:
                year_col = df.columns[0]
            if not laki_laki_col and len(df.columns) >= 2:
                laki_laki_col = df.columns[1]
            if not perempuan_col and len(df.columns) >= 3:
                perempuan_col = df.columns[2]
            if not total_col and len(df.columns) >= 4:
                total_col = df.columns[3]
            
            if not year_col:
                print(f"[ERROR] Cannot identify year column. Found: {df.columns.tolist()}")
                return pd.DataFrame()
            
            # Create standardized dataframe
            df_clean = pd.DataFrame()
            df_clean['Tahun'] = df[year_col]
            
            if laki_laki_col:
                df_clean['Laki_Laki'] = df[laki_laki_col]
            else:
                df_clean['Laki_Laki'] = None
                
            if perempuan_col:
                df_clean['Perempuan'] = df[perempuan_col]
            else:
                df_clean['Perempuan'] = None
                
            if total_col:
                df_clean['Total'] = df[total_col]
            else:
                df_clean['Total'] = None
            
            # Convert year to numeric
            df_clean['Tahun'] = pd.to_numeric(df_clean['Tahun'], errors='coerce')
            
            # Convert values - handle "-" and both decimal formats
            for col in ['Laki_Laki', 'Perempuan', 'Total']:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].astype(str)
                    df_clean[col] = df_clean[col].apply(convert_value_to_numeric)
            
            # Remove rows with invalid year
            df_clean = df_clean.dropna(subset=['Tahun'])
            
            print(f"[OK] Data processed. Total valid records: {len(df_clean)}")
            return df_clean

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID '{SHEET_ID}' not found.")
            print(f"[ERROR] Spreadsheet with ID '{SHEET_ID}' not found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet 'Ketenagakerjaan_TPAK' not found.")
            print(f"[ERROR] Worksheet 'Ketenagakerjaan_TPAK' not found.")
        except Exception as e:
            logger.error(f"An error occurred while fetching/processing Ketenagakerjaan TPAK data: {e}")
            print(f"[ERROR] {e}")
        return pd.DataFrame()

    @staticmethod
    def save_ketenagakerjaan_tpak_to_db(df):
        """Saves the processed Ketenagakerjaan TPAK DataFrame to the database."""
        if df.empty:
            print("[WARNING] No Ketenagakerjaan TPAK data to save.")
            return 0, 0

        created_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            year = int(row['Tahun']) if pd.notna(row['Tahun']) else None
            laki_laki = row.get('Laki_Laki') if pd.notna(row.get('Laki_Laki')) else None
            perempuan = row.get('Perempuan') if pd.notna(row.get('Perempuan')) else None
            total = row.get('Total') if pd.notna(row.get('Total')) else None

            if not year:
                continue

            data_to_serialize = {
                'year': year,
                'laki_laki': laki_laki,
                'perempuan': perempuan,
                'total': total
            }

            try:
                instance = KetenagakerjaanTPAK.objects.get(year=year)
            except KetenagakerjaanTPAK.DoesNotExist:
                instance = None

            serializer = KetenagakerjaanTPAKSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saat menyimpan Ketenagakerjaan TPAK untuk tahun {year}: {serializer.errors}")

        print(f"[INFO] Total Ketenagakerjaan TPAK records created: {created_count}, updated: {updated_count}")
        return created_count, updated_count

    @classmethod
    def sync_ketenagakerjaan_tpak(cls):
        """Fungsi utama untuk sinkronisasi data Google Sheets -> database."""
        df = cls.fetch_ketenagakerjaan_tpak_data()
        created_count, updated_count = cls.save_ketenagakerjaan_tpak_to_db(df)
        return created_count, updated_count


# Helper function untuk parse tahun dengan asterisk
def parse_year_with_flag(year_str):
    """
    Parse tahun dari string seperti "2024**" menjadi (2024, "**")
    Returns: (year: int, flag: str)
    """
    if not year_str or pd.isna(year_str):
        return None, ''
    
    year_str = str(year_str).strip()
    
    # Extract asterisks
    flag = ''
    if year_str.endswith('***'):
        flag = '***'
        year_str = year_str[:-3]
    elif year_str.endswith('**'):
        flag = '**'
        year_str = year_str[:-2]
    elif year_str.endswith('*'):
        flag = '*'
        year_str = year_str[:-1]
    
    # Extract year number
    try:
        year = int(year_str)
        return year, flag
    except (ValueError, TypeError):
        return None, ''


# Helper function untuk convert value dengan handling "-" dan "error"
def convert_value(value):
    """
    Convert value dari spreadsheet, handle "-" dan "error" menjadi None
    Returns: float atau None (None akan di-handle sebagai null di database)
    """
    if pd.isna(value) or value is None:
        return None
    
    value_str = str(value).strip()
    
    # Handle empty string, "-", "error" (case insensitive)
    if value_str.lower() in ['-', 'error', '', 'nan', 'none', 'null']:
        return None
    
    try:
        # Handle numeric strings with commas/dots
        # Preserve negative sign
        is_negative = value_str.startswith('-')
        value_str = value_str.replace(',', '.')
        
        # Remove thousand separators (dots)
        if '.' in value_str:
            parts = value_str.split('.')
            if len(parts) > 2:  # Multiple dots = thousand separators
                value_str = ''.join(parts[:-1]) + '.' + parts[-1]
        
        result = float(value_str)
        return result
    except (ValueError, TypeError) as e:
        # Debug: print problematic values
        print(f"[DEBUG] Could not convert value '{value}' to float: {e}")
        return None


# PDRB Pengeluaran Service
class PDRBPengeluaranService:
    SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
    
    @staticmethod
    def fetch_pdrb_pengeluaran_data(sheet_name, is_quarterly=False):
        """
        Fetch data dari sheet PDRB Pengeluaran.
        
        Args:
            sheet_name: Nama sheet di Google Spreadsheet
            is_quarterly: True jika data triwulanan (memiliki kolom I, II, III, IV)
        
        Returns:
            DataFrame dengan kolom: expenditure_category, year, quarter (jika quarterly), preliminary_flag, value
        """
        print(f"[INFO] Fetching PDRB Pengeluaran data from sheet: {sheet_name}...")
        try:
            # Autentikasi
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)
            
            # Buka sheet
            sheet = client.open_by_key(PDRBPengeluaranService.SHEET_ID).worksheet(sheet_name)
            
            # Ambil semua data
            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print(f"[WARNING] No data found in sheet {sheet_name}")
                return pd.DataFrame()
            
            # Cari kolom untuk kategori pengeluaran
            # Untuk data tahunan: biasanya kolom A (index 0)
            # Untuk data triwulanan: biasanya kolom B (index 1)
            category_col_idx = 0
            
            if is_quarterly:
                # Untuk triwulanan, cek row 2 (index 1) atau row 3 (index 2) untuk kategori
                if len(data) > 2:
                    # Cek row 3 (index 2) - biasanya di sini ada kategori
                    row2 = data[2]
                    for idx in [1, 0]:  # Coba kolom B dulu, lalu A
                        if idx < len(row2):
                            val = str(row2[idx]).strip()
                            if val and val and not val.isdigit() and len(val) > 3:
                                category_col_idx = idx
                                break
            else:
                # Untuk tahunan, cek row 1 atau row 2 untuk kategori
                if len(data) > 1:
                    # Cek row 1 (index 0) dulu
                    first_row = data[0]
                    for idx in [0, 1]:
                        if idx < len(first_row):
                            val_str = str(first_row[idx]).strip().upper()
                            if val_str and ('PENGELUARAN' in val_str or 'COMPONENT' in val_str or 'JENIS' in val_str):
                                category_col_idx = idx
                                break
                    
                    # Jika tidak ditemukan di row 1, cek row 2
                    if category_col_idx == 0 and len(data) > 1:
                        second_row = data[1]
                        for idx in [0, 1]:
                            if idx < len(second_row):
                                val_str = str(second_row[idx]).strip().upper()
                                if val_str and ('PENGELUARAN' in val_str or 'COMPONENT' in val_str or 'JENIS' in val_str or 'TYPE' in val_str):
                                    category_col_idx = idx
                                    break
            
            records = []
            
            if is_quarterly:
                # Handle multi-row headers untuk data triwulanan
                # Row 1: Years (2022, 2023*, 2024**, etc.) - bisa span multiple columns
                # Row 2: Quarters (I, II, III, IV, Jumlah) under each year
                # Row 3+: Data rows
                
                if len(data) < 3:
                    print(f"[WARNING] Not enough rows for quarterly data in sheet {sheet_name}")
                    return pd.DataFrame()
                
                year_row = data[0]  # Row dengan tahun
                quarter_row = data[1]  # Row dengan triwulan
                data_rows = data[2:]  # Data rows mulai dari row 3
                
                # Build mapping: col_idx -> (year, flag, quarter)
                col_mapping = {}
                current_year = None
                current_flag = ''
                
                for col_idx in range(len(year_row)):
                    if col_idx < category_col_idx:
                        continue
                    
                    year_val = str(year_row[col_idx]).strip() if col_idx < len(year_row) else ''
                    quarter_val = str(quarter_row[col_idx]).strip() if col_idx < len(quarter_row) else ''
                    
                    # Jika ada nilai tahun di kolom ini, update current_year
                    if year_val:
                        year, flag = parse_year_with_flag(year_val)
                        if year is not None:
                            current_year = year
                            current_flag = flag
                    
                    # Jika ada nilai triwulan (termasuk "Jumlah")
                    if quarter_val:
                        quarter_upper = quarter_val.upper()
                        quarter = None
                        
                        if quarter_upper == 'I' or ' I ' in quarter_upper or quarter_upper.endswith(' I'):
                            quarter = 'I'
                        elif quarter_upper == 'II' or ' II ' in quarter_upper or quarter_upper.endswith(' II'):
                            quarter = 'II'
                        elif quarter_upper == 'III' or ' III ' in quarter_upper or quarter_upper.endswith(' III'):
                            quarter = 'III'
                        elif quarter_upper == 'IV' or ' IV ' in quarter_upper or quarter_upper.endswith(' IV'):
                            quarter = 'IV'
                        elif 'JUMLAH' in quarter_upper or quarter_upper == 'TOTAL' or quarter_upper == 'TOT':
                            quarter = 'TOTAL'
                        
                        if quarter and current_year is not None:
                            col_mapping[col_idx] = (current_year, current_flag, quarter)
                
                # Process data rows
                for row in data_rows:
                    if not row or len(row) <= category_col_idx:
                        continue
                    
                    category = str(row[category_col_idx]).strip() if category_col_idx < len(row) else None
                    if not category or category == '':
                        continue
                    
                    # Skip jika kategori adalah header atau terlalu pendek
                    category_upper = category.upper()
                    skip_words = ['PENGELUARAN/COMPONENT', 'COMPONENT', 'PENGELUARAN', 'EXPENDITURE']
                    if any(skip_word in category_upper for skip_word in skip_words) and len(category) < 30:
                        continue
                    
                    if len(category) < 3:
                        continue
                    
                    for col_idx, (year, flag, quarter) in col_mapping.items():
                        if col_idx >= len(row):
                            continue
                        
                        value = row[col_idx] if col_idx < len(row) else None
                        value = convert_value(value)
                        
                        records.append({
                            'expenditure_category': category,
                            'year': year,
                            'quarter': quarter,
                            'preliminary_flag': flag,
                            'value': value
                        })
            else:
                # Handle single-row headers untuk data tahunan
                # Header tahun bisa di row 1 atau row 2 (index 0 atau 1)
                # Coba cari di row 1 dulu, kalau tidak ada coba row 2
                headers = None
                header_row_idx = None
                data_start_row = None
                
                # Cek row 1 (index 0) untuk header tahun
                if len(data) > 0:
                    row0 = data[0]
                    year_found = False
                    for val in row0:
                        if val and parse_year_with_flag(str(val).strip())[0] is not None:
                            year_found = True
                            break
                    
                    if year_found:
                        headers = row0
                        header_row_idx = 0
                        data_start_row = 1
                    elif len(data) > 1:
                        # Cek row 2 (index 1) untuk header tahun
                        row1 = data[1]
                        year_found = False
                        for val in row1:
                            if val and parse_year_with_flag(str(val).strip())[0] is not None:
                                year_found = True
                                break
                        
                        if year_found:
                            headers = row1
                            header_row_idx = 1
                            data_start_row = 2
                
                if headers is None:
                    print(f"[WARNING] No year columns found in sheet {sheet_name}. Tried rows 1 and 2.")
                    print(f"[DEBUG] First row sample: {data[0][:5] if len(data) > 0 else 'N/A'}")
                    print(f"[DEBUG] Second row sample: {data[1][:5] if len(data) > 1 else 'N/A'}")
                    return pd.DataFrame()
                
                # Identifikasi kolom tahun
                year_cols = []
                for idx, header in enumerate(headers):
                    if idx == category_col_idx:
                        continue
                    
                    header_str = str(header).strip()
                    year, flag = parse_year_with_flag(header_str)
                    if year is not None:
                        year_cols.append((idx, year, flag))
                
                if not year_cols:
                    print(f"[WARNING] No valid year columns found in sheet {sheet_name} at row {header_row_idx + 1}")
                    return pd.DataFrame()
                
                print(f"[DEBUG] Found {len(year_cols)} year columns in row {header_row_idx + 1}")
                
                # Process data rows (mulai dari data_start_row)
                data_rows = data[data_start_row:] if data_start_row else []
                
                for row in data_rows:
                    if not row or len(row) <= category_col_idx:
                        continue
                    
                    category = str(row[category_col_idx]).strip() if category_col_idx < len(row) else None
                    if not category or category == '':
                        continue
                    
                    # Skip jika kategori adalah header (misalnya "JENIS PENGELUARAN", "TYPE OF EXPENDITURE")
                    category_upper = category.upper()
                    skip_words = ['JENIS', 'TYPE', 'PENGELUARAN', 'EXPENDITURE', 'COMPONENT', 'OF EXPENDITURE']
                    if any(skip_word in category_upper for skip_word in skip_words) and len(category) < 20:
                        continue
                    
                    # Skip jika hanya angka atau kosong
                    if category.isdigit() or len(category) < 3:
                        continue
                    
                    for col_idx, year, flag in year_cols:
                        if col_idx >= len(row):
                            continue
                        
                        value = row[col_idx] if col_idx < len(row) else None
                        value = convert_value(value)
                        
                        records.append({
                            'expenditure_category': category,
                            'year': year,
                            'preliminary_flag': flag,
                            'value': value
                        })
            
            df = pd.DataFrame(records)
            print(f"[OK] Data fetched and processed. Total records: {len(df)}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching PDRB Pengeluaran data from {sheet_name}: {e}")
            print(f"[ERROR] {e}")
            return pd.DataFrame()
    
    @staticmethod
    def save_pdrb_adhb_to_db(df):
        """Save PDRB Pengeluaran ADHB data to database."""
        if df.empty:
            print("[WARNING] No PDRB Pengeluaran ADHB data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            # Handle None/NaN values properly
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'expenditure_category': str(row['expenditure_category']).strip(),
                'year': int(row['year']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBPengeluaranADHB.objects.get(
                    expenditure_category=data_to_serialize['expenditure_category'],
                    year=data_to_serialize['year']
                )
            except PDRBPengeluaranADHB.DoesNotExist:
                instance = None
            
            serializer = PDRBPengeluaranADHBSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB ADHB: {serializer.errors}")
        
        print(f"[INFO] PDRB ADHB: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_adhk_to_db(df):
        """Save PDRB Pengeluaran ADHK data to database."""
        if df.empty:
            print("[WARNING] No PDRB Pengeluaran ADHK data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            # Handle None/NaN values properly
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'expenditure_category': str(row['expenditure_category']).strip(),
                'year': int(row['year']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBPengeluaranADHK.objects.get(
                    expenditure_category=data_to_serialize['expenditure_category'],
                    year=data_to_serialize['year']
                )
            except PDRBPengeluaranADHK.DoesNotExist:
                instance = None
            
            serializer = PDRBPengeluaranADHKSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB ADHK: {serializer.errors}")
        
        print(f"[INFO] PDRB ADHK: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_distribusi_to_db(df):
        """Save PDRB Pengeluaran Distribusi data to database."""
        if df.empty:
            print("[WARNING] No PDRB Pengeluaran Distribusi data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            # Handle None/NaN values properly
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'expenditure_category': str(row['expenditure_category']).strip(),
                'year': int(row['year']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBPengeluaranDistribusi.objects.get(
                    expenditure_category=data_to_serialize['expenditure_category'],
                    year=data_to_serialize['year']
                )
            except PDRBPengeluaranDistribusi.DoesNotExist:
                instance = None
            
            serializer = PDRBPengeluaranDistribusiSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Distribusi: {serializer.errors}")
        
        print(f"[INFO] PDRB Distribusi: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_laju_pdrb_to_db(df):
        """Save PDRB Pengeluaran Laju PDRB data to database."""
        if df.empty:
            print("[WARNING] No PDRB Pengeluaran Laju PDRB data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            # Handle None/NaN values properly
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'expenditure_category': str(row['expenditure_category']).strip(),
                'year': int(row['year']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBPengeluaranLajuPDRB.objects.get(
                    expenditure_category=data_to_serialize['expenditure_category'],
                    year=data_to_serialize['year']
                )
            except PDRBPengeluaranLajuPDRB.DoesNotExist:
                instance = None
            
            serializer = PDRBPengeluaranLajuPDRBSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Laju PDRB: {serializer.errors}")
        
        print(f"[INFO] PDRB Laju PDRB: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_adhb_triwulanan_to_db(df):
        """Save PDRB Pengeluaran ADHB Triwulanan data to database."""
        if df.empty:
            print("[WARNING] No PDRB Pengeluaran ADHB Triwulanan data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            # Handle None/NaN values properly
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'expenditure_category': str(row['expenditure_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBPengeluaranADHBTriwulanan.objects.get(
                    expenditure_category=data_to_serialize['expenditure_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBPengeluaranADHBTriwulanan.DoesNotExist:
                instance = None
            
            serializer = PDRBPengeluaranADHBTriwulananSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB ADHB Triwulanan: {serializer.errors}")
        
        print(f"[INFO] PDRB ADHB Triwulanan: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_adhk_triwulanan_to_db(df):
        """Save PDRB Pengeluaran ADHK Triwulanan data to database."""
        if df.empty:
            print("[WARNING] No PDRB Pengeluaran ADHK Triwulanan data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            # Handle None/NaN values properly
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'expenditure_category': str(row['expenditure_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBPengeluaranADHKTriwulanan.objects.get(
                    expenditure_category=data_to_serialize['expenditure_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBPengeluaranADHKTriwulanan.DoesNotExist:
                instance = None
            
            serializer = PDRBPengeluaranADHKTriwulananSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB ADHK Triwulanan: {serializer.errors}")
        
        print(f"[INFO] PDRB ADHK Triwulanan: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_distribusi_triwulanan_to_db(df):
        """Save PDRB Pengeluaran Distribusi Triwulanan data to database."""
        if df.empty:
            print("[WARNING] No PDRB Pengeluaran Distribusi Triwulanan data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            # Handle None/NaN values properly
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'expenditure_category': str(row['expenditure_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBPengeluaranDistribusiTriwulanan.objects.get(
                    expenditure_category=data_to_serialize['expenditure_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBPengeluaranDistribusiTriwulanan.DoesNotExist:
                instance = None
            
            serializer = PDRBPengeluaranDistribusiTriwulananSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Distribusi Triwulanan: {serializer.errors}")
        
        print(f"[INFO] PDRB Distribusi Triwulanan: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_laju_qtoq_to_db(df):
        """Save PDRB Pengeluaran Laju Q-to-Q data to database."""
        if df.empty:
            print("[WARNING] No PDRB Pengeluaran Laju Q-to-Q data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            # Handle None/NaN values properly
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'expenditure_category': str(row['expenditure_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBPengeluaranLajuQtoQ.objects.get(
                    expenditure_category=data_to_serialize['expenditure_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBPengeluaranLajuQtoQ.DoesNotExist:
                instance = None
            
            serializer = PDRBPengeluaranLajuQtoQSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Laju Q-to-Q: {serializer.errors}")
        
        print(f"[INFO] PDRB Laju Q-to-Q: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_laju_ytoy_to_db(df):
        """Save PDRB Pengeluaran Laju Y-to-Y data to database."""
        if df.empty:
            print("[WARNING] No PDRB Pengeluaran Laju Y-to-Y data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            # Handle None/NaN values properly
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'expenditure_category': str(row['expenditure_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBPengeluaranLajuYtoY.objects.get(
                    expenditure_category=data_to_serialize['expenditure_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBPengeluaranLajuYtoY.DoesNotExist:
                instance = None
            
            serializer = PDRBPengeluaranLajuYtoYSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Laju Y-to-Y: {serializer.errors}")
        
        print(f"[INFO] PDRB Laju Y-to-Y: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_laju_ctoc_to_db(df):
        """Save PDRB Pengeluaran Laju C-to-C data to database."""
        if df.empty:
            print("[WARNING] No PDRB Pengeluaran Laju C-to-C data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            # Handle None/NaN values properly
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'expenditure_category': str(row['expenditure_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBPengeluaranLajuCtoC.objects.get(
                    expenditure_category=data_to_serialize['expenditure_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBPengeluaranLajuCtoC.DoesNotExist:
                instance = None
            
            serializer = PDRBPengeluaranLajuCtoCSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Laju C-to-C: {serializer.errors}")
        
        print(f"[INFO] PDRB Laju C-to-C: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @classmethod
    def sync_all_pdrb_pengeluaran(cls):
        """Sync semua data PDRB Pengeluaran dari semua sheet."""
        print("\n" + "="*60)
        print("SYNCING PDRB PENGELUARAN DATA")
        print("="*60 + "\n")
        
        results = {}
        
        # Annual sheets
        sheets_annual = [
            ("PDRB Pengeluaran_ADHB", cls.save_pdrb_adhb_to_db, False),
            ("PDRB Pengeluaran_ADHK", cls.save_pdrb_adhk_to_db, False),
            ("PDRB Pengeluaran_Distribusi", cls.save_pdrb_distribusi_to_db, False),
            ("PDRB Pengeluaran_Laju PDRB", cls.save_pdrb_laju_pdrb_to_db, False),
        ]
        
        # Quarterly sheets
        sheets_quarterly = [
            ("PDRB Pengeluaran_ADHB_Triwulanan", cls.save_pdrb_adhb_triwulanan_to_db, True),
            ("PDRB Pengeluaran_ADHK_Triwulanan", cls.save_pdrb_adhk_triwulanan_to_db, True),
            ("PDRB Pengeluaran_Distribusi_Triwulanan", cls.save_pdrb_distribusi_triwulanan_to_db, True),
            ("Laju Pertumbuhan_q-to-q_PDRB Pengeluaran_ Triwulan", cls.save_pdrb_laju_qtoq_to_db, True),
            ("Laju Pertumbuhan_y-to-y_PDRB Pengeluaran_ Triwulan", cls.save_pdrb_laju_ytoy_to_db, True),
            ("Laju Pertumbuhan_c-to-c_PDRB Pengeluaran_ Triwulan", cls.save_pdrb_laju_ctoc_to_db, True),
        ]
        
        all_sheets = sheets_annual + sheets_quarterly
        
        for sheet_name, save_func, is_quarterly in all_sheets:
            print(f"\n[PROCESSING] {sheet_name}...")
            df = cls.fetch_pdrb_pengeluaran_data(sheet_name, is_quarterly=is_quarterly)
            if not df.empty:
                created, updated = save_func(df)
                results[sheet_name] = {'created': created, 'updated': updated}
            else:
                print(f"[WARNING] No data found for {sheet_name}")
                results[sheet_name] = {'created': 0, 'updated': 0}
        
        print("\n" + "="*60)
        print("SYNC COMPLETE - SUMMARY")
        print("="*60)
        for sheet_name, counts in results.items():
            print(f"{sheet_name}: {counts['created']} created, {counts['updated']} updated")
        print("="*60 + "\n")
        
        return results


# PDRB Lapangan Usaha Service
class PDRBLapanganUsahaService:
    SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
    
    @staticmethod
    def fetch_pdrb_lapangan_usaha_data(sheet_name, is_quarterly=False):
        """
        Fetch data dari sheet PDRB Lapangan Usaha.
        
        Args:
            sheet_name: Nama sheet di Google Spreadsheet
            is_quarterly: True jika data triwulanan (memiliki kolom I, II, III, IV)
        
        Returns:
            DataFrame dengan kolom: industry_category, year, quarter (jika quarterly), preliminary_flag, value
        """
        print(f"[INFO] Fetching PDRB Lapangan Usaha data from sheet: {sheet_name}...")
        try:
            # Autentikasi
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
            else:
                print("[INFO] Using local credentials.json")
                
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    "credentials.json", scope
                )
            client = gspread.authorize(credentials)
            
            # Buka sheet
            sheet = client.open_by_key(PDRBLapanganUsahaService.SHEET_ID).worksheet(sheet_name)
            
            # Ambil semua data
            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print(f"[WARNING] No data found in sheet {sheet_name}")
                return pd.DataFrame()
            
            # Cari kolom untuk kategori lapangan usaha
            category_col_idx = 0
            
            if is_quarterly:
                # Untuk triwulanan, cek row 2 (index 1) atau row 3 (index 2) untuk kategori
                if len(data) > 2:
                    row2 = data[2]
                    for idx in [1, 0]:
                        if idx < len(row2):
                            val = str(row2[idx]).strip()
                            if val and not val.isdigit() and len(val) > 3:
                                category_col_idx = idx
                                break
            else:
                # Untuk tahunan, cek row 1 atau row 2 untuk kategori
                # Biasanya kolom B (index 1) berisi kategori
                if len(data) > 0:
                    # Cek row 1 (index 0) dulu
                    first_row = data[0]
                    for idx in [1, 0]:  # Prioritas kolom B (index 1) dulu
                        if idx < len(first_row):
                            val_str = str(first_row[idx]).strip().upper()
                            if val_str and ('LAPANGAN USAHA' in val_str or 'LAPUS' in val_str or 'JENIS' in val_str or 'INDUSTRI' in val_str or 'TYPE' in val_str):
                                category_col_idx = idx
                                print(f"[DEBUG] Found category column at index {idx} in row 1: {val_str[:50]}")
                                break
                    
                    # Jika tidak ditemukan di row 1, cek row 2
                    if category_col_idx == 0 and len(data) > 1:
                        second_row = data[1]
                        for idx in [1, 0]:  # Prioritas kolom B (index 1) dulu
                            if idx < len(second_row):
                                val_str = str(second_row[idx]).strip().upper()
                                if val_str and ('LAPANGAN USAHA' in val_str or 'LAPUS' in val_str or 'JENIS' in val_str or 'INDUSTRI' in val_str or 'TYPE' in val_str):
                                    category_col_idx = idx
                                    print(f"[DEBUG] Found category column at index {idx} in row 2: {val_str[:50]}")
                                    break
                
                # Default ke kolom B (index 1) jika tidak ditemukan
                if category_col_idx == 0 and len(data) > 2:
                    # Cek apakah kolom B berisi data yang valid (bukan tahun)
                    test_row = data[2] if len(data) > 2 else []
                    if len(test_row) > 1:
                        test_val = str(test_row[1]).strip()
                        # Jika bukan angka tahun, kemungkinan besar ini kolom kategori
                        year, _ = parse_year_with_flag(test_val)
                        if year is None and len(test_val) > 3:
                            category_col_idx = 1
                            print(f"[DEBUG] Using default category column B (index 1)")
            
            records = []
            
            if is_quarterly:
                # Handle multi-row headers untuk data triwulanan
                if len(data) < 3:
                    print(f"[WARNING] Not enough rows for quarterly data in sheet {sheet_name}")
                    return pd.DataFrame()
                
                year_row = data[0]
                quarter_row = data[1]
                data_rows = data[2:]
                
                # Build mapping: col_idx -> (year, flag, quarter)
                col_mapping = {}
                current_year = None
                current_flag = ''
                
                for col_idx in range(len(year_row)):
                    if col_idx < category_col_idx:
                        continue
                    
                    year_val = str(year_row[col_idx]).strip() if col_idx < len(year_row) else ''
                    quarter_val = str(quarter_row[col_idx]).strip() if col_idx < len(quarter_row) else ''
                    
                    if year_val:
                        year, flag = parse_year_with_flag(year_val)
                        if year is not None:
                            current_year = year
                            current_flag = flag
                    
                    if quarter_val:
                        quarter_upper = quarter_val.upper()
                        quarter = None
                        
                        if quarter_upper == 'I' or ' I ' in quarter_upper or quarter_upper.endswith(' I'):
                            quarter = 'I'
                        elif quarter_upper == 'II' or ' II ' in quarter_upper or quarter_upper.endswith(' II'):
                            quarter = 'II'
                        elif quarter_upper == 'III' or ' III ' in quarter_upper or quarter_upper.endswith(' III'):
                            quarter = 'III'
                        elif quarter_upper == 'IV' or ' IV ' in quarter_upper or quarter_upper.endswith(' IV'):
                            quarter = 'IV'
                        elif 'JUMLAH' in quarter_upper or quarter_upper == 'TOTAL' or quarter_upper == 'TOT':
                            quarter = 'TOTAL'
                        
                        if quarter and current_year is not None:
                            col_mapping[col_idx] = (current_year, current_flag, quarter)
                
                # Process data rows
                for row in data_rows:
                    if not row or len(row) <= category_col_idx:
                        continue
                    
                    category = str(row[category_col_idx]).strip() if category_col_idx < len(row) else None
                    if not category or category == '':
                        continue
                    
                    category_upper = category.upper()
                    skip_words = ['LAPANGAN USAHA', 'LAPUS', 'INDUSTRI', 'INDUSTRY']
                    if any(skip_word in category_upper for skip_word in skip_words) and len(category) < 30:
                        continue
                    
                    if len(category) < 3:
                        continue
                    
                    for col_idx, (year, flag, quarter) in col_mapping.items():
                        if col_idx >= len(row):
                            continue
                        
                        value_str = str(row[col_idx]).strip()
                        value = convert_value(value_str)
                        
                        records.append({
                            'industry_category': category,
                            'year': year,
                            'quarter': quarter,
                            'preliminary_flag': flag,
                            'value': value
                        })
            else:
                # Handle annual data
                # Row 1: Header dengan judul (bisa merge)
                # Row 2: Tahun (2010, 2011, 2012, etc.)
                # Row 3+: Data rows
                if len(data) < 3:
                    print(f"[WARNING] Not enough rows for annual data in sheet {sheet_name}")
                    return pd.DataFrame()
                
                # Cari row yang berisi tahun (bisa row 1 atau row 2)
                headers = None
                header_row_idx = None
                data_start_row = None
                
                # Cek row 1 (index 0) untuk header tahun
                if len(data) > 0:
                    row0 = data[0]
                    year_found = False
                    for val in row0:
                        if val and parse_year_with_flag(str(val).strip())[0] is not None:
                            year_found = True
                            break
                    
                    if year_found:
                        headers = row0
                        header_row_idx = 0
                        data_start_row = 1
                    elif len(data) > 1:
                        # Cek row 2 (index 1) untuk header tahun
                        row1 = data[1]
                        year_found = False
                        for val in row1:
                            if val and parse_year_with_flag(str(val).strip())[0] is not None:
                                year_found = True
                                break
                        
                        if year_found:
                            headers = row1
                            header_row_idx = 1
                            data_start_row = 2
                
                if headers is None:
                    print(f"[WARNING] No year columns found in sheet {sheet_name}. Tried rows 1 and 2.")
                    print(f"[DEBUG] First row sample: {data[0][:5] if len(data) > 0 else 'N/A'}")
                    print(f"[DEBUG] Second row sample: {data[1][:5] if len(data) > 1 else 'N/A'}")
                    return pd.DataFrame()
                
                # Build year mapping from header
                year_mapping = {}
                for col_idx in range(len(headers)):
                    if col_idx < category_col_idx:
                        continue
                    
                    header_val = str(headers[col_idx]).strip()
                    if header_val:
                        year, flag = parse_year_with_flag(header_val)
                        if year is not None:
                            year_mapping[col_idx] = (year, flag)
                
                if not year_mapping:
                    print(f"[WARNING] No valid year columns found in sheet {sheet_name} at row {header_row_idx + 1}")
                    print(f"[DEBUG] Header row sample: {headers[:10]}")
                    return pd.DataFrame()
                
                print(f"[DEBUG] Found {len(year_mapping)} year columns in row {header_row_idx + 1}")
                
                # Process data rows (mulai dari data_start_row)
                data_rows = data[data_start_row:] if data_start_row else []
                
                for row in data_rows:
                    if not row or len(row) <= category_col_idx:
                        continue
                    
                    category = str(row[category_col_idx]).strip() if category_col_idx < len(row) else None
                    if not category or category == '':
                        continue
                    
                    category_upper = category.upper()
                    skip_words = ['LAPANGAN USAHA', 'LAPUS', 'INDUSTRI', 'INDUSTRY', 'JENIS', 'TYPE']
                    if any(skip_word in category_upper for skip_word in skip_words) and len(category) < 30:
                        continue
                    
                    if len(category) < 3:
                        continue
                    
                    for col_idx, (year, flag) in year_mapping.items():
                        if col_idx >= len(row):
                            continue
                        
                        value_str = str(row[col_idx]).strip()
                        value = convert_value(value_str)
                        
                        records.append({
                            'industry_category': category,
                            'year': year,
                            'preliminary_flag': flag,
                            'value': value
                        })
            
            df = pd.DataFrame(records)
            
            if df.empty:
                print(f"[WARNING] No valid records found after processing sheet {sheet_name}")
                return pd.DataFrame()
            
            print(f"[OK] Data processed. Total valid records: {len(df)}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching PDRB Lapangan Usaha data from {sheet_name}: {e}")
            print(f"[ERROR] {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    @staticmethod
    def save_pdrb_lapus_adhb_to_db(df):
        """Save PDRB Lapangan Usaha ADHB data to database."""
        if df.empty:
            print("[WARNING] No PDRB Lapangan Usaha ADHB data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'industry_category': str(row['industry_category']).strip(),
                'year': int(row['year']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBLapanganUsahaADHB.objects.get(
                    industry_category=data_to_serialize['industry_category'],
                    year=data_to_serialize['year']
                )
            except PDRBLapanganUsahaADHB.DoesNotExist:
                instance = None
            
            serializer = PDRBLapanganUsahaADHBSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Lapus ADHB: {serializer.errors}")
        
        print(f"[INFO] PDRB Lapus ADHB: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_lapus_adhk_to_db(df):
        """Save PDRB Lapangan Usaha ADHK data to database."""
        if df.empty:
            print("[WARNING] No PDRB Lapangan Usaha ADHK data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'industry_category': str(row['industry_category']).strip(),
                'year': int(row['year']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBLapanganUsahaADHK.objects.get(
                    industry_category=data_to_serialize['industry_category'],
                    year=data_to_serialize['year']
                )
            except PDRBLapanganUsahaADHK.DoesNotExist:
                instance = None
            
            serializer = PDRBLapanganUsahaADHKSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Lapus ADHK: {serializer.errors}")
        
        print(f"[INFO] PDRB Lapus ADHK: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_lapus_distribusi_to_db(df):
        """Save PDRB Lapangan Usaha Distribusi data to database."""
        if df.empty:
            print("[WARNING] No PDRB Lapangan Usaha Distribusi data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'industry_category': str(row['industry_category']).strip(),
                'year': int(row['year']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBLapanganUsahaDistribusi.objects.get(
                    industry_category=data_to_serialize['industry_category'],
                    year=data_to_serialize['year']
                )
            except PDRBLapanganUsahaDistribusi.DoesNotExist:
                instance = None
            
            serializer = PDRBLapanganUsahaDistribusiSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Lapus Distribusi: {serializer.errors}")
        
        print(f"[INFO] PDRB Lapus Distribusi: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_lapus_laju_pdrb_to_db(df):
        """Save PDRB Lapangan Usaha Laju PDRB data to database."""
        if df.empty:
            print("[WARNING] No PDRB Lapangan Usaha Laju PDRB data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'industry_category': str(row['industry_category']).strip(),
                'year': int(row['year']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBLapanganUsahaLajuPDRB.objects.get(
                    industry_category=data_to_serialize['industry_category'],
                    year=data_to_serialize['year']
                )
            except PDRBLapanganUsahaLajuPDRB.DoesNotExist:
                instance = None
            
            serializer = PDRBLapanganUsahaLajuPDRBSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Lapus Laju PDRB: {serializer.errors}")
        
        print(f"[INFO] PDRB Lapus Laju PDRB: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_lapus_laju_implisit_to_db(df):
        """Save PDRB Lapangan Usaha Laju Implisit data to database."""
        if df.empty:
            print("[WARNING] No PDRB Lapangan Usaha Laju Implisit data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'industry_category': str(row['industry_category']).strip(),
                'year': int(row['year']),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBLapanganUsahaLajuImplisit.objects.get(
                    industry_category=data_to_serialize['industry_category'],
                    year=data_to_serialize['year']
                )
            except PDRBLapanganUsahaLajuImplisit.DoesNotExist:
                instance = None
            
            serializer = PDRBLapanganUsahaLajuImplisitSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Lapus Laju Implisit: {serializer.errors}")
        
        print(f"[INFO] PDRB Lapus Laju Implisit: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_lapus_adhb_triwulanan_to_db(df):
        """Save PDRB Lapangan Usaha ADHB Triwulanan data to database."""
        if df.empty:
            print("[WARNING] No PDRB Lapangan Usaha ADHB Triwulanan data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'industry_category': str(row['industry_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']).strip(),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBLapanganUsahaADHBTriwulanan.objects.get(
                    industry_category=data_to_serialize['industry_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBLapanganUsahaADHBTriwulanan.DoesNotExist:
                instance = None
            
            serializer = PDRBLapanganUsahaADHBTriwulananSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Lapus ADHB Triwulanan: {serializer.errors}")
        
        print(f"[INFO] PDRB Lapus ADHB Triwulanan: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_lapus_adhk_triwulanan_to_db(df):
        """Save PDRB Lapangan Usaha ADHK Triwulanan data to database."""
        if df.empty:
            print("[WARNING] No PDRB Lapangan Usaha ADHK Triwulanan data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'industry_category': str(row['industry_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']).strip(),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBLapanganUsahaADHKTriwulanan.objects.get(
                    industry_category=data_to_serialize['industry_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBLapanganUsahaADHKTriwulanan.DoesNotExist:
                instance = None
            
            serializer = PDRBLapanganUsahaADHKTriwulananSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Lapus ADHK Triwulanan: {serializer.errors}")
        
        print(f"[INFO] PDRB Lapus ADHK Triwulanan: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_lapus_distribusi_triwulanan_to_db(df):
        """Save PDRB Lapangan Usaha Distribusi Triwulanan data to database."""
        if df.empty:
            print("[WARNING] No PDRB Lapangan Usaha Distribusi Triwulanan data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'industry_category': str(row['industry_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']).strip(),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBLapanganUsahaDistribusiTriwulanan.objects.get(
                    industry_category=data_to_serialize['industry_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBLapanganUsahaDistribusiTriwulanan.DoesNotExist:
                instance = None
            
            serializer = PDRBLapanganUsahaDistribusiTriwulananSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Lapus Distribusi Triwulanan: {serializer.errors}")
        
        print(f"[INFO] PDRB Lapus Distribusi Triwulanan: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_lapus_laju_qtoq_to_db(df):
        """Save PDRB Lapangan Usaha Laju Q-to-Q data to database."""
        if df.empty:
            print("[WARNING] No PDRB Lapangan Usaha Laju Q-to-Q data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'industry_category': str(row['industry_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']).strip(),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBLapanganUsahaLajuQtoQ.objects.get(
                    industry_category=data_to_serialize['industry_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBLapanganUsahaLajuQtoQ.DoesNotExist:
                instance = None
            
            serializer = PDRBLapanganUsahaLajuQtoQSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Lapus Laju Q-to-Q: {serializer.errors}")
        
        print(f"[INFO] PDRB Lapus Laju Q-to-Q: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_lapus_laju_ytoy_to_db(df):
        """Save PDRB Lapangan Usaha Laju Y-to-Y data to database."""
        if df.empty:
            print("[WARNING] No PDRB Lapangan Usaha Laju Y-to-Y data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'industry_category': str(row['industry_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']).strip(),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBLapanganUsahaLajuYtoY.objects.get(
                    industry_category=data_to_serialize['industry_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBLapanganUsahaLajuYtoY.DoesNotExist:
                instance = None
            
            serializer = PDRBLapanganUsahaLajuYtoYSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Lapus Laju Y-to-Y: {serializer.errors}")
        
        print(f"[INFO] PDRB Lapus Laju Y-to-Y: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_pdrb_lapus_laju_ctoc_to_db(df):
        """Save PDRB Lapangan Usaha Laju C-to-C data to database."""
        if df.empty:
            print("[WARNING] No PDRB Lapangan Usaha Laju C-to-C data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            value = row['value']
            if pd.isna(value) or value is None:
                value = None
            else:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None
            
            data_to_serialize = {
                'industry_category': str(row['industry_category']).strip(),
                'year': int(row['year']),
                'quarter': str(row['quarter']).strip(),
                'preliminary_flag': row.get('preliminary_flag', ''),
                'value': value
            }
            
            try:
                instance = PDRBLapanganUsahaLajuCtoC.objects.get(
                    industry_category=data_to_serialize['industry_category'],
                    year=data_to_serialize['year'],
                    quarter=data_to_serialize['quarter']
                )
            except PDRBLapanganUsahaLajuCtoC.DoesNotExist:
                instance = None
            
            serializer = PDRBLapanganUsahaLajuCtoCSerializer(instance=instance, data=data_to_serialize)
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving PDRB Lapus Laju C-to-C: {serializer.errors}")
        
        print(f"[INFO] PDRB Lapus Laju C-to-C: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @classmethod
    def sync_all_pdrb_lapangan_usaha(cls):
        """Sync semua data PDRB Lapangan Usaha dari semua sheet."""
        print("\n" + "="*60)
        print("SYNCING PDRB LAPANGAN USAHA DATA")
        print("="*60 + "\n")
        
        results = {}
        
        # Annual sheets
        sheets_annual = [
            ("PDRB Lapus_ADHB", cls.save_pdrb_lapus_adhb_to_db, False),
            ("PDRB Lapus_ADHK", cls.save_pdrb_lapus_adhk_to_db, False),
            ("PDRB Lapus_Distribusi", cls.save_pdrb_lapus_distribusi_to_db, False),
            ("PDRB Lapus_Laju PDRB", cls.save_pdrb_lapus_laju_pdrb_to_db, False),
            ("PDRB Lapus_Laju Implisit", cls.save_pdrb_lapus_laju_implisit_to_db, False),
        ]
        
        # Quarterly sheets
        sheets_quarterly = [
            ("PDRB Lapus_ADHB_Triwulanan", cls.save_pdrb_lapus_adhb_triwulanan_to_db, True),
            ("PDRB Lapus_ADHK_Triwulanan", cls.save_pdrb_lapus_adhk_triwulanan_to_db, True),
            ("PDRB Lapus_Distribusi_Triwulanan", cls.save_pdrb_lapus_distribusi_triwulanan_to_db, True),
            ("Laju Pertumbuhan_q-to-q_PDRB Lapus_ Triwulan", cls.save_pdrb_lapus_laju_qtoq_to_db, True),
            ("Laju Pertumbuhan_y-to-y_PDRB Lapus_ Triwulan", cls.save_pdrb_lapus_laju_ytoy_to_db, True),
            ("Laju Pertumbuhan_c-to-c_PDRB Lapus_ Triwulan", cls.save_pdrb_lapus_laju_ctoc_to_db, True),
        ]
        
        all_sheets = sheets_annual + sheets_quarterly
        
        for sheet_name, save_func, is_quarterly in all_sheets:
            print(f"\n[PROCESSING] {sheet_name}...")
            df = cls.fetch_pdrb_lapangan_usaha_data(sheet_name, is_quarterly=is_quarterly)
            if not df.empty:
                created, updated = save_func(df)
                results[sheet_name] = {'created': created, 'updated': updated}
            else:
                print(f"[WARNING] No data found for {sheet_name}")
                results[sheet_name] = {'created': 0, 'updated': 0}
        
        print("\n" + "="*60)
        print("SYNC COMPLETE - SUMMARY")
        print("="*60)
        for sheet_name, counts in results.items():
            print(f"{sheet_name}: {counts['created']} created, {counts['updated']} updated")
        print("="*60 + "\n")
        
        return results

# ========== Inflasi Services ==========

class InflasiService:
    """
    Service untuk mengambil dan menyimpan data inflasi dari Google Sheets.
    Menangani:
    1. Sheet "Inflasi" - data inflasi umum per bulan dan tahun
    2. Sheet "Inflasi_perkom_YYYY" - data inflasi per komoditas per tahun
    """
    SHEET_ID = "1keS9YFYO1qzAawWgLh2U2pY6xX5ppKUnhbdHQYfU5HM"
    
    # Mapping bulan dari nama Indonesia ke format model
    # Note: Di model, November menggunakan value 'NOPEMBER' bukan 'NOVEMBER'
    MONTH_MAPPING = {
        'JANUARI': 'JANUARI',
        'FEBRUARI': 'FEBRUARI',
        'MARET': 'MARET',
        'APRIL': 'APRIL',
        'MEI': 'MEI',
        'JUNI': 'JUNI',
        'JULI': 'JULI',
        'AGUSTUS': 'AGUSTUS',
        'SEPTEMBER': 'SEPTEMBER',
        'OKTOBER': 'OKTOBER',
        'NOPEMBER': 'NOPEMBER',  # Value di model adalah 'NOPEMBER'
        'NOVEMBER': 'NOPEMBER',   # Konversi "NOVEMBER" dari spreadsheet ke 'NOPEMBER'
        'DESEMBER': 'DESEMBER'
    }
    
    @staticmethod
    def get_client():
        """Mendapatkan client Google Sheets yang sudah di-authenticate."""
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        if "GOOGLE_CREDENTIALS_JSON" in os.environ:
                print("[INFO] Using Google credentials from ENV (Railway)")
                
                creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                    creds_dict, scope
                )
        else:
            print("[INFO] Using local credentials.json")
            
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                "credentials.json", scope
            )
        return gspread.authorize(credentials)
    
    @staticmethod
    def fetch_inflasi_data():
        """
        Mengambil data dari sheet "Inflasi".
        Format: Row 1 = Tahun headers (2008, 2009, etc.), Row 2 = Sub-headers (Bulanan, Kumulatif, YoY)
        Row 3+ = Data bulan (Januari, Februari, etc.)
        """
        print("[INFO] Fetching Inflasi data from Google Sheets...")
        try:
            client = InflasiService.get_client()
            spreadsheet = client.open_by_key(InflasiService.SHEET_ID)
            
            try:
                sheet = spreadsheet.worksheet("Inflasi")
            except gspread.exceptions.WorksheetNotFound:
                print("[WARNING] Sheet 'Inflasi' not found")
                return pd.DataFrame()
            
            data = sheet.get_all_values()
            if not data or len(data) < 3:
                print("[WARNING] No data found in Inflasi sheet")
                return pd.DataFrame()
            
            # Row 0 = Tahun headers (2008, 2009, etc.)
            # Row 1 = Sub-headers (Bulanan, Kumulatif, YoY)
            # Row 2+ = Data bulan
            year_row = data[0]
            subheader_row = data[1]
            data_rows = data[2:]
            
            # Parse struktur kolom: tahun dan sub-header
            records = []
            current_year = None
            current_type = None
            
            for col_idx in range(len(year_row)):
                year_val = year_row[col_idx].strip()
                subheader_val = subheader_row[col_idx].strip().upper() if col_idx < len(subheader_row) else ""
                
                # Deteksi tahun
                if year_val and year_val.isdigit():
                    current_year = int(year_val)
                
                # Deteksi tipe (Bulanan, Kumulatif, YoY)
                if subheader_val:
                    if 'BULANAN' in subheader_val or 'MONTHLY' in subheader_val:
                        current_type = 'bulanan'
                    elif 'KUMULATIF' in subheader_val or 'CUMULATIVE' in subheader_val:
                        current_type = 'kumulatif'
                    elif 'YOY' in subheader_val or 'YEAR' in subheader_val:
                        current_type = 'yoy'
                
                # Jika ada tahun dan tipe yang valid, ambil data
                if current_year and current_type and col_idx < len(data_rows[0]) if data_rows else False:
                    for row_idx, row in enumerate(data_rows):
                        if col_idx < len(row):
                            month_name = row[0].strip().upper() if row else ""
                            value_str = row[col_idx].strip() if col_idx < len(row) else ""
                            
                            # Skip jika bukan bulan yang valid
                            if month_name not in InflasiService.MONTH_MAPPING:
                                continue
                            
                            # Convert value
                            try:
                                # Replace comma with dot for decimal
                                value_str = value_str.replace(',', '.')
                                value = float(value_str) if value_str else None
                            except (ValueError, AttributeError):
                                value = None
                            
                            if value is not None:
                                records.append({
                                    'year': current_year,
                                    'month': InflasiService.MONTH_MAPPING[month_name],
                                    current_type: value
                                })
            
            # Convert to DataFrame dan group by year-month untuk menggabungkan Bulanan, Kumulatif, YoY
            if records:
                df = pd.DataFrame(records)
                # Group by year and month, aggregate values
                df = df.groupby(['year', 'month']).agg({
                    'bulanan': 'first',
                    'kumulatif': 'first',
                    'yoy': 'first'
                }).reset_index()
                
                print(f"[OK] Inflasi data processed. Total records: {len(df)}")
                return df
            else:
                print("[WARNING] No valid records found in Inflasi sheet")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching Inflasi data: {e}")
            print(f"[ERROR] {e}")
            return pd.DataFrame()
    
    @staticmethod
    def fetch_inflasi_perkom_data(sheet_name):
        """
        Mengambil data dari sheet "Inflasi_perkom_YYYY".
        Format: 
        - Column A = Kode Komoditas
        - Column B = Nama Komoditas
        - Column C = Flag
        - Column D+ = Bulan (JANUARI, FEBRUARI, etc.)
        """
        print(f"[INFO] Fetching data from sheet '{sheet_name}'...")
        try:
            client = InflasiService.get_client()
            spreadsheet = client.open_by_key(InflasiService.SHEET_ID)
            
            try:
                sheet = spreadsheet.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                print(f"[WARNING] Sheet '{sheet_name}' not found")
                return pd.DataFrame()
            
            data = sheet.get_all_values()
            if not data or len(data) < 2:
                print(f"[WARNING] No data found in sheet '{sheet_name}'")
                return pd.DataFrame()
            
            # Extract year from sheet name (e.g., "Inflasi_perkom_2025" -> 2025)
            year_match = re.search(r'(\d{4})', sheet_name)
            if not year_match:
                print(f"[WARNING] Could not extract year from sheet name '{sheet_name}'")
                return pd.DataFrame()
            
            year = int(year_match.group(1))
            
            # Row 0 = Headers (Kode Komoditas, Nama Komoditas, Flag, JANUARI, FEBRUARI, etc.)
            headers = [h.strip().upper() for h in data[0]]
            data_rows = data[1:]
            
            # Find column indices
            kode_idx = None
            nama_idx = None
            flag_idx = None
            month_cols = {}  # month_name -> column_index
            
            for idx, header in enumerate(headers):
                if 'KODE' in header or 'CODE' in header:
                    kode_idx = idx
                elif 'NAMA' in header or 'NAME' in header or 'KOMODITAS' in header:
                    nama_idx = idx
                elif 'FLAG' in header:
                    flag_idx = idx
                elif header in InflasiService.MONTH_MAPPING:
                    month_cols[InflasiService.MONTH_MAPPING[header]] = idx
            
            if kode_idx is None or nama_idx is None:
                print(f"[WARNING] Could not find required columns in sheet '{sheet_name}'")
                return pd.DataFrame()
            
            # Process data rows
            records = []
            for row in data_rows:
                if len(row) <= max(kode_idx, nama_idx):
                    continue
                
                kode = str(row[kode_idx]).strip() if kode_idx < len(row) else ""
                nama = str(row[nama_idx]).strip() if nama_idx < len(row) else ""
                flag = str(row[flag_idx]).strip() if flag_idx and flag_idx < len(row) else ""
                
                # Skip jika kode atau nama kosong
                if not kode or not nama:
                    continue
                
                # Process monthly values
                for month, col_idx in month_cols.items():
                    if col_idx < len(row):
                        value_str = str(row[col_idx]).strip()
                        try:
                            # Replace comma with dot for decimal
                            value_str = value_str.replace(',', '.')
                            value = float(value_str) if value_str and value_str != '' else None
                        except (ValueError, AttributeError):
                            value = None
                        
                        if value is not None:
                            records.append({
                                'commodity_code': kode,
                                'commodity_name': nama,
                                'flag': flag if flag else None,
                                'year': year,
                                'month': month,
                                'value': value
                            })
            
            if records:
                df = pd.DataFrame(records)
                print(f"[OK] Sheet '{sheet_name}' processed. Total records: {len(df)}")
                return df
            else:
                print(f"[WARNING] No valid records found in sheet '{sheet_name}'")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching data from sheet '{sheet_name}': {e}")
            print(f"[ERROR] {e}")
            return pd.DataFrame()
    
    @staticmethod
    def find_perkom_sheets():
        """Mencari semua sheet dengan pattern 'Inflasi_perkom_YYYY'."""
        try:
            client = InflasiService.get_client()
            spreadsheet = client.open_by_key(InflasiService.SHEET_ID)
            all_sheets = spreadsheet.worksheets()
            
            perkom_sheets = []
            pattern = re.compile(r'Inflasi_perkom_(\d{4})', re.IGNORECASE)
            
            for sheet in all_sheets:
                if pattern.match(sheet.title):
                    perkom_sheets.append(sheet.title)
            
            print(f"[INFO] Found {len(perkom_sheets)} perkom sheets: {perkom_sheets}")
            return sorted(perkom_sheets)
            
        except Exception as e:
            logger.error(f"Error finding perkom sheets: {e}")
            print(f"[ERROR] {e}")
            return []
    
    @staticmethod
    def save_inflasi_to_db(df):
        """Menyimpan data inflasi umum ke database."""
        if df.empty:
            print("[WARNING] No Inflasi data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            year = int(row['year']) if pd.notna(row['year']) else None
            month = row['month'] if pd.notna(row['month']) else None
            
            if not year or not month:
                continue
            
            data_to_serialize = {
                'year': year,
                'month': month,
                'bulanan': float(row['bulanan']) if pd.notna(row.get('bulanan')) else None,
                'kumulatif': float(row['kumulatif']) if pd.notna(row.get('kumulatif')) else None,
                'yoy': float(row['yoy']) if pd.notna(row.get('yoy')) else None,
            }
            
            try:
                instance = Inflasi.objects.get(year=year, month=month)
            except Inflasi.DoesNotExist:
                instance = None
            
            serializer = InflasiSerializer(instance=instance, data=data_to_serialize)
            
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving Inflasi for {year}-{month}: {serializer.errors}")
        
        print(f"[INFO] Inflasi records: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @staticmethod
    def save_inflasi_perkom_to_db(df):
        """Menyimpan data inflasi per komoditas ke database."""
        if df.empty:
            print("[WARNING] No InflasiPerKomoditas data to save.")
            return 0, 0
        
        created_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            commodity_code = str(row['commodity_code']).strip()
            year = int(row['year']) if pd.notna(row['year']) else None
            month = row['month'] if pd.notna(row['month']) else None
            
            if not commodity_code or not year or not month:
                continue
            
            data_to_serialize = {
                'commodity_code': commodity_code,
                'commodity_name': str(row['commodity_name']).strip(),
                'flag': str(row['flag']).strip() if pd.notna(row.get('flag')) else None,
                'year': year,
                'month': month,
                'value': float(row['value']) if pd.notna(row.get('value')) else None,
            }
            
            # IMPORTANT: Include 'flag' in the query because same commodity_code 
            # can exist with different flags (e.g., code "11" can be Flag 1 or Flag 2)
            flag_value = data_to_serialize.get('flag')
            try:
                instance = InflasiPerKomoditas.objects.get(
                    commodity_code=commodity_code,
                    flag=flag_value,
                    year=year,
                    month=month
                )
            except InflasiPerKomoditas.DoesNotExist:
                instance = None
            
            serializer = InflasiPerKomoditasSerializer(instance=instance, data=data_to_serialize)
            
            if serializer.is_valid():
                serializer.save()
                if instance is None:
                    created_count += 1
                else:
                    updated_count += 1
            else:
                print(f"[ERROR] Error saving InflasiPerKomoditas for {commodity_code}-{year}-{month}: {serializer.errors}")
        
        print(f"[INFO] InflasiPerKomoditas records: {created_count} created, {updated_count} updated")
        return created_count, updated_count
    
    @classmethod
    def sync_all_inflasi(cls):
        """
        Fungsi utama untuk sinkronisasi semua data inflasi.
        - Mengambil data dari sheet "Inflasi"
        - Mengambil data dari semua sheet "Inflasi_perkom_YYYY" yang ditemukan
        - Menyimpan semua data ke database
        """
        print("\n" + "="*60)
        print("SYNCING INFLASI DATA")
        print("="*60 + "\n")
        
        results = {}
        
        # 1. Sync data Inflasi umum
        print("[PROCESSING] Sheet 'Inflasi'...")
        df_inflasi = cls.fetch_inflasi_data()
        if not df_inflasi.empty:
            created, updated = cls.save_inflasi_to_db(df_inflasi)
            results['Inflasi'] = {'created': created, 'updated': updated}
        else:
            print("[WARNING] No data found for 'Inflasi' sheet")
            results['Inflasi'] = {'created': 0, 'updated': 0}
        
        # 2. Sync data Inflasi per komoditas
        perkom_sheets = cls.find_perkom_sheets()
        for sheet_name in perkom_sheets:
            print(f"\n[PROCESSING] Sheet '{sheet_name}'...")
            df_perkom = cls.fetch_inflasi_perkom_data(sheet_name)
            if not df_perkom.empty:
                created, updated = cls.save_inflasi_perkom_to_db(df_perkom)
                results[sheet_name] = {'created': created, 'updated': updated}
            else:
                print(f"[WARNING] No data found for '{sheet_name}'")
                results[sheet_name] = {'created': 0, 'updated': 0}
        
        print("\n" + "="*60)
        print("INFLASI SYNC COMPLETE - SUMMARY")
        print("="*60)
        for sheet_name, counts in results.items():
            print(f"{sheet_name}: {counts['created']} created, {counts['updated']} updated")
        print("="*60 + "\n")
        
        return results
