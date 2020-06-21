import os
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import pymysql as mysql
import time
from sqlalchemy import create_engine
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from random import randint

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# 한국거래소에서 전체 상장법인목록 가져오기
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

    return corp_list


# 한국거래소에서 코스닥 상장법인목록 가져오기
def get_krx_kosdaq_corp_list():
    corp_list = \
    pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt',
                 header=0)[0]
    corp_list = corp_list.rename(columns={'업종': 'sector',
                                          '회사명': 'name',
                                          '종목코드': 'code',
                                          '주요제품': 'product',
                                          '결산월': 'settle'})
    corp_list.code = corp_list.code.map('{:06d}'.format)
    corp_list = corp_list[['name', 'code', 'sector', 'product', 'settle']]
    corp_list = corp_list.sort_values(by=['sector', 'name'], axis=0)

    return corp_list


# 코스닥 상장법인 목록 가져온 후 DB 에 저장
def insert_krx_kosdaq_corp_list():
    kosdaq_corp_list_df = get_krx_kosdaq_corp_list()
    engine = create_engine("mysql+pymysql://logan:logan@localhost:3306/finance?charset=utf8", echo=True)
    conn = engine.connect()
    kosdaq_corp_list_df.to_sql(name='kosdaq_corp', con=engine, if_exists='append', index=False)
    conn.close()


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
def get_html_body(name, code):
    target_url = 'https://finance.naver.com/item/main.nhn?code={code}'.format(code=code)
    print("     {0} = {1}".format(name, target_url))

    item_info = requests.get(target_url, verify=False).text
    html_body = BeautifulSoup(item_info, 'html.parser')

    return html_body


# Naver Finance 에서 재무제표 추출
def get_naver_financial_stat(html_body):
    try:
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
        # total_finance = pd.concat([annual_finance, quarter_finance], axis=1)

        return annual_finance, quarter_finance
    except Exception:
        print("::::: 재무제표 정보 추출 실패 :::::")


# Naver Finance 에서 동종업종 기업리스트 추출
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
def get_naver_finance_daily_quoutes(corp_list, corp_name):
    code = corp_list.query("name=='{}'".format(corp_name))['code'].to_string(index=False)
    target_url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)
    print("     {0} = {1}".format(corp_name, target_url))

    daily_quotes = pd.DataFrame()

    # 1페이지에서 20페이지의 데이터만 가져오기
    # TODO: 데이터 가져오는 구조 변경
    for page in range(1, 21):
        pg_url = '{url}&page={page}'.format(url=target_url, page=page)
        daily_quotes = daily_quotes.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

    # df.dropna()를 이용해 결측값 있는 행 제거
    daily_quotes = daily_quotes.dropna()

    return daily_quotes


# Pivoting DataFrame
def pivoting_df(df):
    # temp_df = pd.DataFrame(total_finance_df.loc[:, '2017.12'])
    df['index1'] = df.index
    temp_df = df.T
    temp_df = temp_df.drop('index1')
    return temp_df


def prepare_insert(df, corp_name):
    code = df.query("name=='{}'".format(corp_name))['code'].to_string(index=False)
    df['corp_code'] = code
    df['corp_name'] = corp_name
    df['fiscal_period'] = df.index.str.replace(r'\D+', '')
    df.replace(',', '', regex=True, inplace=True)
    df.replace('', '-', regex=True, inplace=True)
    df.rename(columns={'매출액': 'revenue',
                       '영업이익': 'net_profit',
                       '당기순이익': 'net_income',
                       '영업이익률': 'net_profit_margin',
                       '순이익률': 'roa',
                       'ROE(지배주주)': 'roe',
                       '부채비율': 'debt_ratio',
                       '당좌비율': 'quick_ratio',
                       '유보율': 'reserve_ratio',
                       'EPS(원)': 'eps',
                       'PER(배)': 'per',
                       'BPS(원)': 'bps',
                       'PBR(배)': 'pbr',
                       '주당배당금(원)': 'dps',
                       '시가배당률(%)': 'dividend_ratio',
                       '배당성향(%)': 'dividend_payout_ratio'}, inplace=True)
    # df['revenue'] = df['revenue'].astype('int')
    # df['net_profit'] = df['net_profit'].astype('int')
    # df['net_income'] = df['net_income'].astype('int')
    # df['net_profit_margin'] = df['net_profit_margin'].astype('float')
    # df['roa'] = df['roa'].astype('float')
    # df['roe'] = df['roe'].astype('float')
    # df['debt_ratio'] = df['debt_ratio'].astype('float')
    # df['quick_ratio'] = df['quick_ratio'].astype('float')
    # df['reserve_ratio'] = df['reserve_ratio'].astype('float')
    # df['eps'] = df['eps'].astype('int')
    # df['per'] = df['per'].astype('float')
    # df['bps'] = df['bps'].astype('int')
    # df['pbr'] = df['pbr'].astype('float')
    # df['dps'] = df['dps'].astype('int')
    # df['dividend_ratio'] = df['dividend_ratio'].astype('float')
    # df['dividend_payout_ratio'] = df['dividend_payout_ratio'].astype('float')

    return df


# 재무제표 DB 에 저장
def insert_financial_stat(df):
    engine = create_engine("mysql+pymysql://logan:logan@localhost:3306/finance?charset=utf8", echo=True)
    conn = engine.connect()
    df.to_sql(name='financial_statement', con=engine, if_exists='append', index=False)
    conn.close()


# 전체 상장법인 재무제표 crawling 후 DB 에 저장
def insert_all_corp_financial_stat(df_list):
    for i, row in df_list.iterrows():
        corp_name = row['name']
        corp_code = row['code']
        print("Getting [{0}]'s Financial Statement...".format(corp_name))
        html = get_html_body(corp_name, corp_code)
        financial_stat = get_naver_financial_stat(html)

        if financial_stat is not None:
            annual_finance_df = financial_stat[0]
            quarter_finance_df = financial_stat[1]

            p_annual_df = pivoting_df(annual_finance_df)
            p_quarter_df = pivoting_df(quarter_finance_df)
            p_annual_df['fiscal_division_code'] = 'A'
            p_quarter_df['fiscal_division_code'] = 'Q'

            # DB 에 Insert 준비 작업
            p_annual_df = prepare_insert(p_annual_df, corp_name)
            p_quarter_df = prepare_insert(p_quarter_df, corp_name)

            # DB 에 Insert
            insert_financial_stat(p_annual_df)
            insert_financial_stat(p_quarter_df)

            print("{1} out of {0} left.".format(len(df_list), len(df_list) - i - 1))
            print("Waiting for getting the next corporate Financial Statement...")

        time.sleep(5)


def insert_one_corp_financial_stat(corp_info):
    corp_name = corp_info['name']
    corp_code = corp_info['code']
    print("[{0}] Getting [{1}]'s Financial Statement...".format(os.getpid(), corp_name))
    html = get_html_body(corp_name, corp_code)
    financial_stat = get_naver_financial_stat(html)

    if financial_stat is not None:
        annual_finance_df = financial_stat[0]
        quarter_finance_df = financial_stat[1]

        p_annual_df = pivoting_df(annual_finance_df)
        p_quarter_df = pivoting_df(quarter_finance_df)
        p_annual_df['fiscal_division_code'] = 'A'
        p_quarter_df['fiscal_division_code'] = 'Q'

        # DB 에 Insert 준비 작업
        p_annual_df = prepare_insert(p_annual_df, corp_name)
        p_quarter_df = prepare_insert(p_quarter_df, corp_name)

        # DB 에 Insert
        insert_financial_stat(p_annual_df)
        insert_financial_stat(p_quarter_df)

        print("[{0}] Completed inserting [{1}]'s corporate Financial Statement...".format(os.getpid(), corp_name))
        time.sleep(randint(10, 15))


# 코스닥 상장법인 정보 삭제
def delete_kosdaq_from_financial_stat():
    conn = mysql.connect(host='localhost', user='logan', password='logan', db='finance', charset='utf8')
    sql = "select code from kosdaq_corp"
    results_df = pd.read_sql(sql, conn)

    try:
        mycur = conn.cursor()

        for i, row in results_df.iterrows():
            code = row['code']
            sql = 'delete from financial_statement_kospi where corp_code = "{}"'.format(code)
            mycur.execute(sql)
            time.sleep(1)
    except Exception:
        print("ERROR")
    finally:
        conn.commit()
        conn.close()


# def main():
# 전체 상장법인 목록 가져오기
# corp_list_df = get_db_corp_list()

# (Single processing)전체 상장법인 재무제표 crawling 후 DB 에 저장
# insert_all_corp_financial_stat(corp_list_df)

# 특정기업 재무제표 가져오기
# corp_name = '삼성생명'
# corp_code = corp_list_df.query("name=='{}'".format(corp_name))['code'].to_string(index=False)
# html = get_html_body(corp_name, corp_code)
# financial_stat = get_naver_financial_stat(html)
# annual_finance_df = financial_stat[0]
# quarter_finance_df = financial_stat[1]
# p_annual_df = pivoting_df(annual_finance_df)
# p_quarter_df = pivoting_df(quarter_finance_df)
# p_annual_df['fiscal_division_code'] = 'A'
# p_quarter_df['fiscal_division_code'] = 'Q'
# p_annual_df = prepare_insert(p_annual_df, corp_name)
# p_quarter_df = prepare_insert(p_quarter_df, corp_name)
# insert_financial_stat(p_annual_df)
# insert_financial_stat(p_quarter_df)

# 업종 목록 가져오기
# sector_list_df = get_db_sector_list()

# 동종업종 기업리스트 가져오기
# sector_corp_df = get_naver_sector_corp_list(html)


# (Multi processing) 전체 상장법인 재무제표 crawling 후 DB 에 저장
"""
Using ProcessPoolExecutor
"The ProcessPoolExecutor class is an Executor subclass that uses a pool of processes to execute calls asynchronously.
ProcessPoolExecutor uses the multiprocessing module, which allows it to side-step the Global Interpreter Lock 
but also means that only picklable objects can be executed and returned."
"""
# async def main(loop):
#     print('entering main')
#     executor = ProcessPoolExecutor(max_workers=3)
#     data = await asyncio.gather(*(loop.run_in_executor(executor, insert_one_corp_financial_stat, row)
#                                   for i, row in corp_list_df.iterrows()))
#     print('got result', data)
#     print('leaving main')
#
#
# loop = asyncio.get_event_loop()
# loop.run_until_complete(main(loop))

# if __name__ == "__main__":
#     main()