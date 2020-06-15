import pandas as pd

url_tmpl = 'https://finance.naver.com/item/main.nhn?code=%s'
url = url_tmpl % ('005930')
tables = pd.read_html(url, encoding='euc-kr')
df = tables[3]