import pandas as pd
import numpy as np
import json
import requests
import pymysql
from sqlalchemy import create_engine
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

auth_url = "https://www.strava.com/oauth/token"
activites_url = "https://www.strava.com/api/v3/athlete/activities"

payload = {
    'client_id': "112934",
    'client_secret': 'fc7f48cb74285045d609d3b6a972aff4315f7beb',
    'refresh_token': '900a6b0e72e017cc321db252daaf329a150f70af',
    'grant_type': "refresh_token",
    'f': 'json'
}

print("Requesting Token...\n")
res = requests.post(auth_url, data=payload, verify=False)
access_token = res.json()['access_token']

print("Access Token = {}\n".format(access_token))
header = {'Authorization': 'Bearer ' + access_token}

# The first loop, request_page_number will be set to one, so it requests the first page. Increment this number after
# each request, so the next time we request the second page, then third, and so on...
request_page_num = 1
all_activities = []

while True:
    param = {'per_page': 200, 'page': request_page_num}
    my_dataset = requests.get(activites_url, headers=header, params=param).json()


    if len(my_dataset) == 0:
        print("breaking out of while loop because the response is zero, which means there must be no more activities")
        break

    if all_activities:
        print("all_activities is populated")
        all_activities.extend(my_dataset)

    else:
        print("all_activities is NOT populated")
        all_activities = my_dataset

    request_page_num += 1



# 데이터를 JSON 파일로 저장할 경로와 파일 이름 설정
json_file_path = "activities.json"

# all_activities 리스트를 JSON 형식으로 변환하여 파일에 저장
with open(json_file_path, 'w') as json_file:
    json.dump(all_activities, json_file)



# 평탄화 함수 - dict type mysql 넣기위해서
def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


data_flattened = [flatten(item) for item in all_activities]
df = pd.DataFrame(data_flattened)

# NaN 처리
df = df.fillna(0)

# 고도경도 list 분리 함수
def split_latlng(row):
    if row['start_latlng']:
        row['start_latitude'], row['start_longitude'] = row['start_latlng']
    else:
        row['start_latitude'], row['start_longitude'] = [None, None]
    
    if row['end_latlng']:
        row['end_latitude'], row['end_longitude'] =  row['end_latlng']
    else:
        row['end_latitude'],  row['end_longitude'] = [None, None]
    
    return row

df = df.apply(split_latlng, axis=1)
df.drop(['start_latlng', 'end_latlng'], axis=1, inplace=True)

#----------mysql 적재-----------#

host="localhost"
user="testuser"
password="1234"
database="TESTDB"

mysql_conn_str = "mysql+pymysql://testuser:1234@localhost/testdb"

engine = create_engine(mysql_conn_str)
df.to_sql('t5_table', con=engine, if_exists='replace', index=False) # if_exists 확인

db = pymysql.connect(host=host, user=user, password=password, database=database)
cursor = db.cursor()

cursor.execute("SELECT VERSION()")
data = cursor.fetchone()

