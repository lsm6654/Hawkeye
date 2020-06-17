import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import pymysql as mysql
import re
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# 한국거래소에서 종목코드 가져오기
def get_krx_corp_list():
    corp_list = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0]
    corp_list = corp_list.rename(columns={'업종': 'sector',
                                          '회사명': 'name',
                                          '종목코드': 'code',
                                          '주요제품': 'product',
                                          '결산월': 'settle'})
    corp_list.code = corp_list.code.map('{:06d}'.format)
    corp_list = corp_list[['name', 'code', 'sector', 'product', 'settle']]
    corp_list = corp_list.sort_values(by=['sector', 'name'], axis=0)

    # 데이터 Insert
    # engine = create_engine("mysql+pymysql://logan:"+"logan"+"@localhost:3306/finance?charset=utf8", encoding='utf-8')
    # conn = engine.connect()
    # corp_list.to_sql(name='corporate', con=engine, if_exists='append', index=False)
    # conn.close()
    return corp_list


# DB 에서 종목코드 가져오기
def get_db_corp_list():
    conn = mysql.connect(host='localhost', user='logan', password='logan', db='finance', charset='utf8')
    sql = "select name, code from corporate"
    results_df = pd.read_sql(sql, conn)
    conn.close()
    return results_df


# DB 에서 업종 가져오기
def get_db_sector_list():
    conn = mysql.connect(host='localhost', user='logan', password='logan', db='finance', charset='utf8')
    sql = "select sector from corporate group by sector"
    results_df = pd.read_sql(sql, conn)
    conn.close()
    return results_df


# Naver Finance Crawling
def get_html_body(corp_list, corp_name):
    code = corp_list.query("name=='{}'".format(corp_name))['code'].to_string(index=False)
    target_url = 'https://finance.naver.com/item/main.nhn?code={code}'.format(code=code)
    print("요청 URL = {}".format(target_url))

    item_info = requests.get(target_url, verify=False).text
    html_body = BeautifulSoup(item_info, 'html.parser')

    return html_body


# Naver Finance 에서 재무제표 추출
def get_naver_financial_stat(html_body):
    # 재무제표
    finance_body = html_body.select('div.section.cop_analysis div.sub_section')[0]
    th_data = [item.get_text().strip() for item in finance_body.select('thead th')]
    annual_date = th_data[3:6]
    quarter_date = th_data[7:12]
    finance_index = [item.get_text().strip() for item in finance_body.select('th.h_th2')][3:]
    finance_data = [item.get_text().strip() for item in finance_body.select('td')]
    finance_data = np.array(finance_data)
    finance_data.resize(len(finance_index), 10)

    annual_finance = pd.DataFrame(data=finance_data[0:, 0:3], index=finance_index, columns=annual_date)
    quarter_finance = pd.DataFrame(data=finance_data[0:, 4:9], index=finance_index, columns=quarter_date)
    total_finance = pd.concat([annual_finance, quarter_finance], axis=1)

    return annual_finance, quarter_finance, total_finance


# Naver Finance 에서 동종업종 리스트 추출
def get_naver_sector_corp_list(html_body):
    # 동종업종 리스트
    sector_body = html_body.find('table', {"class": "tb_type1 tb_num"})
    sector_list = [item.get_text().strip() for item in sector_body.select('thead th')][1:]
    sector_temp_df = pd.DataFrame(sector_list, columns=['code'])
    sector_df = pd.DataFrame(columns=['code', 'name'])
    errs = []
    for col in sector_temp_df:
        try:
            sector_df[col] = [item[-6:] for item in sector_temp_df[col]]
            sector_df['name'] = [item[:-6] for item in sector_temp_df[col]]
        except TypeError:
            errs.extend([item for item in sector_df[col]])
    sector_df['name'].replace('\*', '', regex=True, inplace=True)

    return sector_df


# Naver Finance 에서 종목별 일자 데이터 Crawling
def get_naver_finance_daily_quoutes(corp_list):
    corp_name = '삼성생명'
    code = corp_list.query("name=='{}'".format(corp_name))['code'].to_string(index=False)
    target_url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)
    print("요청 URL = {}".format(target_url))

    daily_quotes = pd.DataFrame()

    # 1페이지에서 20페이지의 데이터만 가져오기
    # TODO: 데이터 가져오는 구조 변경
    for page in range(1, 21):
        pg_url = '{url}&page={page}'.format(url=target_url, page=page)
        daily_quotes = daily_quotes.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

    # df.dropna()를 이용해 결측값 있는 행 제거
    daily_quotes = daily_quotes.dropna()
    return daily_quotes


# def main():
# 전체 상장법인 목록 가져오기
corp_list_df = get_db_corp_list()

# 업종 목록 가져오기
# sector_list_df = get_db_sector_list()

# 특정기업 재무제표 가져오기
corp_name = '삼성생명'
html = get_html_body(corp_list_df, corp_name)

financial_stat = get_naver_financial_stat(html)
annual_finance_df = financial_stat[0]
quarter_finance_df = financial_stat[1]
total_finance_df = financial_stat[2]

sector_corp_df = get_naver_sector_corp_list(html)

# if __name__ == "__main__":
#     main()
