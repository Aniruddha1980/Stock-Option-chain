# Importing libs
import logging
import requests
import pandas as pd
from requests.exceptions import HTTPError
import xlwings as xw

logging.basicConfig(level=logging.DEBUG)
# setting up web scrapping url
baseUrl = 'https://www.nseindia.com'
indexUrl = f'{baseUrl}/api/option-chain-indices'
equityUrl = f'{baseUrl}/api/option-chain-equities'
# setting up headers
urlHeaders = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 OPR/68.0.3618.206", Cookie="cookiedata',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en'
}

def calculate(op:float,nseDf:pd.DataFrame) -> pd.DataFrame:
    sprice = op * 0.05
    lprice = op - sprice
    uprice = op + sprice
    df1 = nseDf.loc[(nseDf['strikePrice_PE']>lprice) & (nseDf['strikePrice_PE']<uprice) & (nseDf['strikePrice_PE']%100==0)]
    sumColNames = ['openInterest','changeinOpenInterest','totalTradedVolume']
    for colName in sumColNames:
        df1.at['Total',colName+'_CE'] = df1[colName+'_CE'].sum()
        df1.at['Total',colName+'_PE'] = df1[colName+'_PE'].sum()
    return df1

def createDataFrame(nseData:dict) -> pd.DataFrame:
    df = pd.DataFrame.from_dict(nseData).transpose()
    dropColNames=['strikePrice','expiryDate','identifier','underlying','totalBuyQuantity','totalSellQuantity','bidQty','bidprice','askQty','askPrice']
    for colName in dropColNames:
        df.drop(colName, axis=1, inplace=True)
    return df

def processData(nseData:dict) -> pd.DataFrame:
    logging.info(f'starting to process NSE Datain processData')
    expiryDate = nseData['records']['expiryDates'][0]
    ce = pe = {}
    i = 0
    try:
        for data in nseData['records']['data']:
            if data['expiryDate'] == expiryDate:
                try:
                    ce[i] = data['CE']
                except KeyError:
                    ce[i] = {}
                try:
                    pe[i] = data['PE']
                except KeyError:
                    pe[i] = {}
                i+=1
        ceDf = createDataFrame(ce)
        ceDf.columns += '_CE'
        peDf = createDataFrame(pe)
        peDf.columns += '_PE'
        df = pd.concat([ceDf, peDf], axis = 1)
    except Exception as e:
        logging.debug(f'Exception in processData: {e}')
        logging.debug(f'CE Dataframe: {ceDf}')
        logging.debug(f'PE Dataframe: {peDf}')
    return df

def getNseData(sym:str, url:str, header:dict, retry:int = 0) -> dict:
    logging.debug(f'establishing session with {baseUrl}')
    session = requests.Session()
    resp = session.get(baseUrl, headers=header, timeout=10)
    cookies = dict(resp.cookies)
    logging.debug(f'cookies={cookies}')
    resp.close()
    logging.debug(f'')
    resp = requests.get(url, params={'symbol':sym.upper()}, headers=header, cookies=cookies, timeout=10)
    data = None
    try:
        if resp.status_code != 200:
            resp.raise_for_status()
        else:
            data=resp.json()
            return data
    except HTTPError as e:
        logging.warn(f'non-OK status received for URL: {e.request.url} Status code: {str(e.response.status_code)}, skipping update for symbol: {sym}')
        if retry <= 3:
            logging.debug(f'retrying to get data from NSE, retry count: {retry}')
            getNseData(sym, url, header, retry+1)
        return data
    except Exception as e:
        logging.warn(f'Exception occurred in request NSE api: {e.with_traceback}')
        if retry <= 3:
            logging.debug(f'retrying to get data from NSE, retry count: {retry}')
            getNseData(sym, url, header, retry+1)
        return data
    finally:
        logging.debug(f'Closing session for {sym}')
        session.close()

##Input File Name
inpFile='Input.xlsx'
inpSheet='Input'
wb = xw.Book(inpFile)
inSheet=wb.sheets(inpSheet)
inpDF = pd.read_excel(inpFile, sheet_name=inpSheet)
stockDf = inpDF[['Stock','Opening Price']].rename(columns={'Opening Price': 'OP', 'Stock': 'SYM'}).dropna(axis=0)
indexDf = inpDF[['Index', 'Opening Price.1']].rename(columns={'Opening Price.1': 'OP', 'Stock': 'SYM'}).dropna(axis=0)
runIntrvl = inpDF['Interval'][0]
for index, row in stockDf.iterrows():
    logging.debug(f'Getting data for {row["SYM"]}')
    data = getNseData(str(row['SYM']),equityUrl,urlHeaders)
    if data != None:
        df = processData(data)
        sh = None
        try:
            sh = wb.sheets.add(row['SYM'])
        except ValueError as V:
            sh=wb.sheets(row['SYM'])
            print('WARN:',V)
        logging.info(f'updating sheet for {row["SYM"]}')
        sh.range("A1").value = df
        logging.info(f'saving workbook: {wb.name}')
        wb.save()
    else:
        logging.debug(f'unable to get NSE data for {row["SYM"]}')