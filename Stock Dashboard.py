# Importing libs
import requests
import pandas as pd
from requests.exceptions import HTTPError
import xlwings as xw

# setting up web scrapping url
indexUrl = 'https://www.nseindia.com/api/option-chain-indices'
equityUrl = 'https://www.nseindia.com/api/option-chain-equities'
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
    expiryDate = nseData['records']['expiryDates'][0]
    ce = pe = {}
    i = 0
    for data in nseData['records']['data']:
        if data['expiryDate'] == expiryDate:
            ce[i] = data['CE']
            pe[i] = data['PE']
            i+=1
    ceDf = createDataFrame(ce)
    ceDf.columns += '_CE'
    peDf = createDataFrame(pe)
    peDf.columns += '_PE'
    df = pd.concat([ceDf, peDf], axis = 1)
    return df

def getNseData(sym:str, url:str, header:dict) -> dict:
    resp=requests.get(url, params={'symbol':sym.upper()}, headers=header)
    try:
        if resp.status_code != 200:
            resp.raise_for_status()
        else:
            data=resp.json()
            return data
    except HTTPError as e:
        print('non-OK status received for URL:'+ e.request.url+' Status code:'+str(e.response.status_code))
        return None
    except Exception as e:
        print('Exception occurred: '+e)
        return None

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
    data = getNseData(str(row['SYM']),equityUrl,urlHeaders)
    if data != None:
        df=processData(data)
        print(df)
        try:
            sh = wb.sheets.add(row['SYM'])
        except ValueError as V:
            sh=wb.sheets(row['SYM'])
            print('WARN:',V)
            sh.clear()
            sh.range("A1").value = df