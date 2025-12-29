# import gspread
# from oauth2client.service_account import ServiceAccountCredentials

# # Define the scope
# scope = [
#     'https://www.googleapis.com/auth/spreadsheets',
#     'https://www.googleapis.com/auth/drive'
# ]

# # Authenticate with credentials
# credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
# client = gspread.authorize(credentials)

# # Open the Google Sheet
# sheet = client.open('Indikator Makro Kota Surabaya').sheet('Indeks Pembangunan Manusia Menurut Kabupaten/Kota di Provinsi Jawa Timur, 2017-2024')

# # Fetch the first row of data
# first_row = sheet.row_values(1)
# print(f"First row of data: {first_row}")