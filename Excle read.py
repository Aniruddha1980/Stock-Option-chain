# Importing libs
import requests
import json
import pandas as pd
import xlwings as xw
import math

def read_input_file():
    ## Excle read function.
    inpDF = pd.read_excel('Input.xlsx', sheet_name='Input')
    stockDf = inpDF[['Stock','Opening Price']].rename(columns={'Opening Price': 'OP', 'Stock': 'SYM'}).dropna(axis=0)
    indexDf = inpDF[['Index', 'Opening Price.1']].rename(columns={'Opening Price.1': 'OP', 'Stock': 'SYM'}).dropna(axis=0)
    runIntrvl = inpDF['Interval'][0]
    print(stockDf)
    print(indexDf)
    print(runIntrvl)
    return

def web_scrapping():
    # setting up web scrapping header, url and getting response back function.
    #url = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'
    url = 'https://www.nseindia.com/api/option-chain-equities?symbol=HDFC'

    # setting up headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 OPR/68.0.3618.206", Cookie="cookiedata',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en'
    }
    response = requests.get(url, headers = headers).text
    # Getting data into Json format.
    data = json.loads(response)
    process_json(data)
    return data


def process_json(data):
    
    ## -- Need to copy the code below to create dataframe of response and write the data into excle sheet.

    ## Json read function.
    # getting Expiry list
    exp_list = data['records']['expiryDates']
    # Selecting latest expiry only
    exp_date = exp_list[0]
    # veriable setup
    ce = {}
    pe = {}
    n = 0
    m = 0

    # looking to get PE and CE data into veriables 
    try:
        for i in data['records']['data']:
            if i['expiryDate'] == exp_date:
                try:         
                    ce[n] = i['CE']
                    n = n+1
                except:
                    pass
                try:
                    pe[m] = i['PE']
                    m = m+1
                except:
                    pass
    except:
        pass

    # creating data frame for CE and PE with concatination of _PE/_CE in the column
    ce_df = pd.DataFrame.from_dict(ce).transpose()
    ce_df.columns += "_CE"

    try:
        ce_df.drop('strikePrice_CE', axis=1, inplace=True)
        ce_df.drop('expiryDate_CE', axis=1, inplace=True)
        ce_df.drop('identifier_CE', axis=1, inplace=True)
        ce_df.drop('underlying_CE', axis=1, inplace=True)
        ce_df.drop('totalBuyQuantity_CE', axis=1, inplace=True)
        ce_df.drop('totalSellQuantity_CE', axis=1, inplace=True)
        ce_df.drop('bidQty_CE', axis=1, inplace=True)
        ce_df.drop('bidprice_CE', axis=1, inplace=True)
        ce_df.drop('askQty_CE', axis=1, inplace=True)
        ce_df.drop('askPrice_CE', axis=1, inplace=True)
        
    except:
        pass

    pe_df = pd.DataFrame.from_dict(pe).transpose()
    pe_df.columns += "_PE"

    try:
        pe_df.drop('expiryDate_PE', axis=1, inplace=True)
        pe_df.drop('identifier_PE', axis=1, inplace=True)
        pe_df.drop('underlying_PE', axis=1, inplace=True)
        pe_df.drop('totalBuyQuantity_PE', axis=1, inplace=True)
        pe_df.drop('totalSellQuantity_PE', axis=1, inplace=True)
        pe_df.drop('bidQty_PE', axis=1, inplace=True)
        pe_df.drop('bidprice_PE', axis=1, inplace=True)
        pe_df.drop('askQty_PE', axis=1, inplace=True)
        pe_df.drop('askPrice_PE', axis=1, inplace=True)
        pe_df.drop('underlyingValue_PE', axis=1, inplace=True)
        
    except:
        pass

    # Creating combined data frame of PE and CE
    df = pd.concat([ce_df, pe_df], axis = 1)

    ## Calculation Function
    # input the price and based on that input data the upper and lower strick price
    sprice=2450*0.05
    lprice=2450 - sprice
    uprice=2450 + sprice
    
    #df1=df.loc[(df["strikePrice_PE"]>lprice) & (df["strikePrice_PE"]<uprice) & (df["strikePrice_PE"]%100==0)]
    df1=df.loc[(df["strikePrice_PE"]>lprice) & (df["strikePrice_PE"]<uprice)]

    # calculate the total of OI, COI and Volume
   
    pw = df1["lastPrice_PE"] - df1["change_PE"] 
    pwp = df1["change_PE"]*100/pw
    print(pw)
    print(pwp)


    #try:
    #    df1.at['Total', 'openInterest_CE'] = df1['openInterest_CE'].sum()
    #    df1.at['Total', 'changeinOpenInterest_CE'] = df1['changeinOpenInterest_CE'].sum()
    #    df1.at['Total', 'totalTradedVolume_CE'] = df1['totalTradedVolume_CE'].sum()

    #    df1.at['Total', 'openInterest_PE'] = df1['openInterest_PE'].sum()
    #    df1.at['Total', 'changeinOpenInterest_PE'] = df1['changeinOpenInterest_PE'].sum()
    #    df1.at['Total', 'totalTradedVolume_PE'] = df1['totalTradedVolume_PE'].sum()
    #except:
    #    pass
    write_file(df1)
    return df1
    
def write_file(df1):
    ## Excle write Function
    # If file exist then it will open else it will create and then write data into it thru data frame.
    wb = xw.Book("Input.xlsx")

    #Add sheet should be dynamic to add so that we can fully automate the code.
    try:
        sh1=wb.sheets.add('HDFC')
    except ValueError as V:
        sh1=wb.sheets('HDFC')
        print("Error:", V)
    sh1.clear()
    sh1.range("A1").value = df1


    return

read_input_file()
web_scrapping()