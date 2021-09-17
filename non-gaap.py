from numpy import mat
import pandas as pd
import warnings
import requests
import math
from bs4 import BeautifulSoup
import tabula
from urllib.parse import urljoin
from selenium import webdriver
import sqlalchemy
warnings.filterwarnings('ignore')

# Establish connection to SQL DB
engine = sqlalchemy.create_engine('mysql+mysqldb://{user}:{pw}@los-test-db.mysql.database.azure.com/{db}'
                                  .format(user='csladmin@los-test-db',
                                          pw='csLabs$2019',
                                          db='csl_ds_quandl'))
connection = engine.connect()
# # functions
# def make_url(base_url, comp):
#     url = base_url
#     # add each component to the base url
#     for r in comp:
#         url = '{}/{}'.format(url, r)
#     return url


# # reading in cik numbers
# ciks = pd.read_csv(r'D:/cslabs/CIK_NEW.csv')


headers = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582"
}

tickers = ['PRU', 'ADBE', 'AAPL']
flag = 0
# setting parameters and finding the relevant cik number
# tickers = ['A', 'AAL', 'AAP', 'AAPL', 'ABBV', 'ABC', 'ABMD', 'ABT', 'ACN', 'ADBE', 'ADI', 'ADM', 'ADP', 'ADSK', 'AEE', 'AEP', 'AES', 'AFL',
#            'AIG', 'AIZ', 'AJG', 'AKAM', 'ALB', 'ALGN', 'ALK', 'ALL', 'ALLE', 'AMAT', 'AMCR', 'AMD', 'AME', 'AMGN', 'AMP', 'AMT', 'AMZN',
#            'ANET', 'ANSS', 'ANTM', 'AON', 'AOS', 'APA', 'APD', 'APH', 'APTV', 'ARE', 'ATO', 'ATVI', 'AVB', 'AVGO', 'AVY', 'AWK', 'AXP', 'AZO',
#            'BA', 'BAC', 'BAX', 'BBWI', 'BBY', 'BDX', 'BEN', 'BF.B', 'BIIB', 'BIO', 'BK', 'BKNG', 'BKR', 'BLK', 'BLL', 'BMY', 'BR', 'BRK.B', 'BSX',
#            'BWA', 'BXP', 'C', 'CAG', 'CAH', 'CARR', 'CAT', 'CB', 'CBOE', 'CBRE', 'CCI', 'CCL', 'CDNS', 'CDW', 'CE', 'CERN', 'CF', 'CFG', 'CHD',
#            'CHRW', 'CHTR', 'CI', 'CINF', 'CL', 'CLX', 'CMA', 'CMCSA', 'CME', 'CMG', 'CMI', 'CMS', 'CNC', 'CNP', 'COF', 'COG', 'COO', 'COP',
#            'COST', 'CPB', 'CPRT', 'CRL', 'CRM', 'CSCO', 'CSX', 'CTAS', 'CTLT', 'CTSH', 'CTVA', 'CTXS', 'CVS', 'CVX', 'CZR', 'D', 'DAL', 'DD',
#            'DE', 'DFS', 'DG', 'DGX', 'DHI', 'DHR', 'DIS', 'DISCA', 'DISCK', 'DISH', 'DLR', 'DLTR', 'DOV', 'DOW', 'DPZ', 'DRE', 'DRI', 'DTE', 'DUK',
#            'DVA', 'DVN', 'DXC', 'DXCM', 'EA', 'EBAY', 'ECL', 'ED', 'EFX', 'EIX', 'EL', 'EMN', 'EMR', 'ENPH', 'EOG', 'EQIX', 'EQR', 'ES', 'ESS',
#            'ETN', 'ETR', 'ETSY', 'EVRG', 'EW', 'EXC', 'EXPD', 'EXPE', 'EXR', 'F', 'FANG', 'FAST', 'FB', 'FBHS', 'FCX', 'FDX', 'FE', 'FFIV',
#            'FIS', 'FISV', 'FITB', 'FLT', 'FMC', 'FOX', 'FOXA', 'FRC', 'FRT', 'FTNT', 'FTV', 'GD', 'GE', 'GILD', 'GIS', 'GL', 'GLW', 'GM',
#            'GNRC', 'GOOG', 'GOOGL', 'GPC', 'GPN', 'GPS', 'GRMN', 'GS', 'GWW', 'HAL', 'HAS', 'HBAN', 'HBI', 'HCA', 'HD', 'HES', 'HIG', 'HII',
#            'HLT', 'HOLX', 'HON', 'HPE', 'HPQ', 'HRL', 'HSIC', 'HST', 'HSY', 'HUM', 'HWM', 'IBM', 'ICE', 'IDXX', 'IEX', 'IFF', 'ILMN', 'INCY',
#            'INFO', 'INTC', 'INTU', 'IP', 'IPG', 'IPGP', 'IQV', 'IR', 'IRM', 'ISRG', 'IT', 'ITW', 'IVZ', 'J', 'JBHT', 'JCI', 'JKHY', 'JNJ',
#            'JNPR', 'JPM', 'K', 'KEY', 'KEYS', 'KHC', 'KIM', 'KLAC', 'KMB', 'KMI', 'KMX', 'KO', 'KR', 'KSU', 'L', 'LDOS', 'LEG', 'LEN',
#            'LH', 'LHX', 'LIN', 'LKQ', 'LLY', 'LMT', 'LNC', 'LNT', 'LOW', 'LRCX', 'LUMN', 'LUV', 'LVS', 'LW', 'LYB', 'LYV', 'MA', 'MAA',
#            'MAR', 'MAS', 'MCD', 'MCHP', 'MCK', 'MCO', 'MDLZ', 'MDT', 'MET', 'MGM', 'MHK', 'MKC', 'MKTX', 'MLM', 'MMC', 'MMM', 'MNST', 'MO',
#            'MOS', 'MPC', 'MPWR', 'MRK', 'MRNA', 'MRO', 'MS', 'MSCI', 'MSFT', 'MSI', 'MTB', 'MTD', 'MU', 'NCLH', 'NDAQ', 'NEE', 'NEM', 'NFLX',
#            'NI', 'NKE', 'NLOK', 'NLSN', 'NOC', 'NOV', 'NOW', 'NRG', 'NSC', 'NTAP', 'NTRS', 'NUE', 'NVDA', 'NVR', 'NWL', 'NWS', 'NWSA', 'NXPI',
#            'O', 'ODFL', 'OGN', 'OKE', 'OMC', 'ORCL', 'ORLY', 'OTIS', 'OXY', 'PAYC', 'PAYX', 'PBCT', 'PCAR', 'PEAK', 'PEG', 'PENN', 'PEP',
#            'PFE', 'PFG', 'PG', 'PGR', 'PH', 'PHM', 'PKG', 'PKI', 'PLD', 'PM', 'PNC', 'PNR', 'PNW', 'POOL', 'PPG', 'PPL', 'PRGO', 'PRU', 'PSA',
#            'PSX', 'PTC', 'PVH', 'PWR', 'PXD', 'PYPL', 'QCOM', 'QRVO', 'RCL', 'RE', 'REG', 'REGN', 'RF', 'RHI', 'RJF', 'RL', 'RMD', 'ROK', 'ROL',
#            'ROP', 'ROST', 'RSG', 'RTX', 'SBAC', 'SBUX', 'SCHW', 'SEE', 'SHW', 'SIVB', 'SJM', 'SLB', 'SNA', 'SNPS', 'SO', 'SPG', 'SPGI', 'SRE',
#            'STE', 'STT', 'STX', 'STZ', 'SWK', 'SWKS', 'SYF', 'SYK', 'SYY', 'T', 'TAP', 'TDG', 'TDY', 'TECH', 'TEL', 'TER', 'TFC', 'TFX', 'TGT',
#            'TJX', 'TMO', 'TMUS', 'TPR', 'TRMB', 'TROW', 'TRV', 'TSCO', 'TSLA', 'TSN', 'TT', 'TTWO', 'TWTR', 'TXN', 'TXT', 'TYL', 'UA', 'UAA',
#            'UAL', 'UDR', 'UHS', 'ULTA', 'UNH', 'UNM', 'UNP', 'UPS', 'URI', 'USB', 'V', 'VFC', 'VIAC', 'VLO', 'VMC', 'VNO', 'VRSK', 'VRSN',
#            'VRTX', 'VTR', 'VTRS', 'VZ', 'WAB', 'WAT', 'WBA', 'WDC', 'WEC', 'WELL', 'WFC', 'WHR', 'WLTW', 'WM', 'WMB', 'WMT', 'WRB', 'WRK',
#            'WST', 'WU', 'WY', 'WYNN', 'XEL', 'XLNX', 'XOM', 'XRAY', 'XYL', 'YUM', 'ZBH', 'ZBRA', 'ZION', 'ZTS']


revenue = "randomnumber"
opinc = "randomnumber"
netinc = "randomnumber"
gp = "randomnumber"
link_list = []

flag = 0
iframe = 0


def google(ticker, flag):
    url = str(f'https://www.google.com/search?q={ticker} investor relations')
    response = requests.get(url, headers=headers).text
    soup = BeautifulSoup(response, 'lxml')
    investor_relations = soup.find_all('div', class_='yuRUbf')[0].a['href']
    ir_response = requests.get(investor_relations, headers=headers).text
    for link in BeautifulSoup(ir_response, 'html.parser').find_all('a', href=True):
        open_links(urljoin(investor_relations, link['href']),
                   depth=0, ticker=ticker, flag=flag)
    # ir_soup = BeautifulSoup(ir_request, 'html.parser')


def read_pdf(filepath, ticker):
    print(filepath)
    try:
        dfs = tabula.read_pdf(filepath, pages='all',
                              silent=True, lattice=True)
        for df in dfs:
            m = df.to_string().lower().replace(",", "")
            if "non-gaap" in m and revenue in m or "non-gaap" in m and netinc in m or "non-gaap" in m and opinc in m or "non-gaap" in m and gp in m:
                print(df)
                iframe = iframe+1
                df.to_csv(r'D:/cslabs/non_gaap/'+ticker+str(iframe)+'.csv')
        return 0
    except Exception as e:
        print(e)
        return 0


def read_htmls(filepath, ticker):
    print(filepath)
    try:
        content = requests.get(filepath, headers=headers).content
        tables = BeautifulSoup(content, 'html.parser').find_all('table')
        for table in tables:
            rows = BeautifulSoup(str(table), 'html.parser').find_all('tr')
            for row in rows:
                text = BeautifulSoup(str(row), 'html.parser').find_all('td')
                print(text.lower())
                if "non-gaap" in text.lower():
                    is_non_gaap = True
        dfs = pd.read_html(filepath['href'])
    except Exception as e:
        print(e)
        print("couldn't find non-gaap keyword")
        is_non_gaap = False
    try:
        for df in dfs:
            m = df.to_string().lower().replace(",", "")
            if "non-gaap" in m and revenue in m or "non-gaap" in m and opinc in m or is_non_gaap and revenue in m or is_non_gaap in m and opinc in m:
                print(df)
                iframe = iframe+1
                df.to_csv(r'D:/cslabs/non_gaap/'+ticker+str(iframe)+'.csv')
        return 0
    except:
        return 0


url_string = ['sec', 'annual', 'prox', 'filling', 'financ', 'q4', 'quarter',
              'press release', '2021', '2020', '2019', '2018', '2017', '2016', '2015', '2014']


def open_links(link, depth, ticker, flag):
    # print(link)
    link_list.append(link)
    if any(ext in link for ext in url_string):
        # if 'sec' in link or 'annual' in link or 'prox' in link or 'filling' in link or '2021' in link or '2020' in link or '2019' in link or '2018' in link or '2017' in link or '2016' in link or '2015' in link or '2014' in link or 'financ' in link or 'press release' in link or 'q4' in link or 'quarter' in link:
        print(link)
        browser = webdriver.PhantomJS(
            r"F:\downloads\phantomjs-2.1.1-windows\bin\phantomjs.exe")
        browser.get(link)
        response = browser.page_source
        # try:
        #     response = requests.get(link, headers=headers)
        # except:
        #     return
        soup = BeautifulSoup(response, 'html.parser')
        # print(soup.select("a[href$='.pdf']"))
        for filepath in soup.find_all('a', href=True):
            if '.pdf' in filepath['href']:
                if "http" not in filepath:
                    flag = read_pdf(urljoin(link, filepath['href']), ticker)
                    if flag == 1:
                        break
        for sublink in soup.find_all('a', href=True):
            flag = read_htmls(urljoin(link, sublink['href']), ticker)
            if flag == 1:
                break
            if depth < 3 and sublink not in link_list and flag == 0:
                try:
                    link_list.append(sublink)
                    open_links(urljoin(link, sublink['href']), depth=depth +
                               1, ticker=ticker, flag=flag)
                except:
                    continue
            if flag == 1:
                break


for ticker in tickers:
    flag = 0
    iframe = 0
    link_list = []
    df = pd.read_sql(
        f'select * from mry_income_statement where ticker = "{ticker}" and calendardate = "2020-12-31"', con=engine)
    print(df.values)
    revenue = str(int(df['revenue'][0])).strip("0")
    revenue = revenue.replace(" ", "")
    opinc = str(int(df['opinc'][0])).strip("0")
    opinc = opinc.replace(" ", "")
    netinc = str(int(df['netinc'][0])).strip("0")
    netinc = netinc.replace(" ", "")
    gp = str(int(math.abs(df['gp'][0]))).strip("0")
    gp = gp.replace(" ", "")
    print(revenue)
    print(opinc)
    print(netinc)
    print(gp)
    date = '2021-03-31'
    google(ticker, flag)
