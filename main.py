from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
from datetime import datetime
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import pytz
import re

# Google APIへのアクセスにはOAuth 2.0という認証プロトコルが使用されており、scope呼ばれる権限の範囲を使ってアクセスを制御
scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# 環境変数からサービスアカウントキーのJSONコンテンツを取得
credentials_json_str = os.environ['CREDENTIALS_JSON']
credentials_info = json.loads(credentials_json_str)

# 認証情報を生成
credentials = Credentials.from_service_account_info(credentials_info, scopes=scope)

#認証情報を取得
gc = gspread.authorize(credentials)

# スプレッドシートのIDを環境変数から取得
SPREADSHEET_KEY = os.getenv('SPREADSHEET_KEY')

# 指定されたスプレッドシートとシート名からDataFrameを作成する関数
def get_dataframe_from_sheet(spreadsheet, sheet_name):
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_values()
    return pd.DataFrame(data[1:], columns=data[0])

# スプレッドシートのIDを指定して開く
spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

# DataFrameに変換
df_url = get_dataframe_from_sheet(spreadsheet, 'suumo_url')

# bukken_url 列のみを取り出してリストに変換
Bukken_URL = df_url['Bukken_URL'].tolist()

# 結果を格納するリスト
results = []

for url in Bukken_URL:
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    # bc_codeの取得
    bc_code_match = re.search(r'bc=(\d+)', url)
    bc_code = bc_code_match.group(1) if bc_code_match else None

    # 不動産会社名の取得
    name_tag = soup.find('span', class_='itemcassette-header-ttl')
    real_estate_company_name = name_tag.text.strip() if name_tag else None

    # 免許番号の取得
    license_tag = soup.find('span', class_='itemcassette-header-sub')
    license_number = None
    if license_tag:
        license_search = re.search(r'国土交通大臣（\d+）第(\d+)号', license_tag.text)
        license_number = license_search.group() if license_search else None

    # URLの取得と変換
    url_div = soup.find('div', class_='itemcassette_img-desc')
    if url_div:
        url_tag = url_div.find('a', href=True)
        full_url = f"https://suumo.jp{url_tag['href']}" if url_tag else None
    else:
        full_url = None

    # 不動産コードの取得
    property_code = None
    if full_url:
        code_search = re.search(r'kc_(\d+)', full_url)
        property_code = code_search.group(1) if code_search else None

    # 辞書に格納
    property_dict = {'Bc_code': bc_code, 'Real_estate_company_name': real_estate_company_name, 'URL': full_url, 'License_number': license_number, 'Property_code': property_code}

    # 各テーブルのデータを辞書に追加 (既存のコードをそのまま使用)
    # ...

    # 結果リストに追加
    results.append(property_dict)

    # 次のページのリクエスト前に3秒待機
    time.sleep(3)

# 結果をデータフレームに変換
df = pd.DataFrame(results)

# スプレッドシートのaddress用のsheetを指定して開く
worksheet = spreadsheet.worksheet('suumo_bukkenn')

# ワークシートの内容をクリア
worksheet.clear()

set_with_dataframe(worksheet, df)

###### EOF
