import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd

# Naver Finance 에서 재무제표 Crawling
url_tmpl = 'https://finance.naver.com/item/main.nhn?code=%s'
url = url_tmpl % '005930'

item_info = requests.get(url, verify=False).text
soup = BeautifulSoup(item_info, 'html.parser')
finance_info = soup.select('div.section.cop_analysis div.sub_section')[0]

th_data = [item.get_text().strip() for item in finance_info.select('thead th')]
annual_date = th_data[3:6]
quarter_date = th_data[7:12]

finance_index = [item.get_text().strip() for item in finance_info.select('th.h_th2')][3:]

finance_data = [item.get_text().strip() for item in finance_info.select('td')]
finance_data = np.array(finance_data)
finance_data.resize(len(finance_index), 10)

annual_finance = pd.DataFrame(data=finance_data[0:, 0:3], index=finance_index, columns=annual_date)
quarter_finance = pd.DataFrame(data=finance_data[0:, 4:9], index=finance_index, columns=quarter_date)
total_finance = pd.concat([annual_finance, quarter_finance], axis=1)


# Naver Finance 에서 종목별 일자 데이터 Crawling
def get_url(corp_name, corp_list):
    code = corp_list.query("name=='{}'".format(corp_name))['code'].to_string(index=False)
    target_url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)
    print("요청 URL = {}".format(target_url))
    return target_url


# 한국거래소에서 종목코드 가져오기
corp_list = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0]
corp_list = corp_list.rename(columns={'회사명': 'name', '종목코드': 'code'})
corp_list.code = corp_list.code.map('{:06d}'.format)
corp_list = corp_list[['name', 'code']]

corp_name = '삼성생명'
url = get_url(corp_name, corp_list)

# 일자 데이터를 담을 df라는 DataFrame 정의
df = pd.DataFrame()

# 1페이지에서 20페이지의 데이터만 가져오기
# TODO: 최근 데이터만 가져오는 구조로 변경
for page in range(1, 21):
    pg_url = '{url}&page={page}'.format(url=url, page=page)
    df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

# df.dropna()를 이용해 결측값 있는 행 제거
df = df.dropna()
