import asyncio
import requests
from pix_apidata import *
import time as tt
import requests
import pandas as pd
from datetime import date ,timedelta
from datetime import date,datetime,timedelta
import math
from urllib.request import urlopen
from time import time, sleep
from talib.abstract import *
import json
import numpy as np
import datetime
from talipp.ohlcv import OHLCVFactory
from talipp.indicators import Indicator,VWAP
import threading
import warnings
import json
pd.options.mode.chained_assignment = None
api = apidata_lib.ApiData()
event_loop = asyncio.get_event_loop()
import telegram

Posted_2hr_BO=[]
Posted_TCUP2HR=[]
Posted_TCUP1HR=[] 
Posted_DIV=[]   

def calculate_indicator(res_json):
    
    df = res_json    
    df['RSI_5'] = RSI(df.cp, timeperiod=5) 
    df['RSI_BO_F']=0
   
    df = df.round(decimals=2)   
       
    for i in range(5,len(df)):  
          
        if  df['RSI_5'][i-1] <= 70.00 and df['RSI_5'][i] >= 70.00:
            df['RSI_BO_F'][i]=1
    return df

def calculate_indicator_withvol(res_json):
    
    df = res_json    
    df['RSI_5'] = RSI(df.cp, timeperiod=5) 
    df['RSI_BO_F']=0
    df['SMA'] = SMA(df.vol,20)
    df = df.round(decimals=2)   
       
    for i in range(5,len(df)):  
          
        if  df['RSI_5'][i-1] <= 70.00 and df['RSI_5'][i] >= 70.00 and df['vol'][i] >= (df['SMA'][i]*2.0).astype(int):
            df['RSI_BO_F'][i]=1
    return df


    

    

def calculate_indicator_Engulf(res_json):
    
    
    try:
        df = res_json   
        df['Engulf']=0
       
        if ((df['cp'].iloc[-2] < df['op'].iloc[-2]) & (df['op'].iloc[-1] <= df['hp'].iloc[-2]) & (df['cp'].iloc[-1] >= df['hp'].iloc[-2]) & (df['op'].iloc[-1] <= df['cp'].iloc[-1])):
            df['Engulf'].iloc[-1]=1
            #print(df.tail(5))           
        
        if ((df['cp'].iloc[-3] < df['op'].iloc[-3]) & (df['lp'].iloc[-2] >= df['lp'].iloc[-3]) & (df['lp'].iloc[-2] <= df['hp'].iloc[-3]) & (df['hp'].iloc[-2] <= df['hp'].iloc[-3]) & (df['hp'].iloc[-2] >= df['lp'].iloc[-3]) & (df['op'].iloc[-1] <= df['hp'].iloc[-3]) & (df['cp'].iloc[-1] >= df['hp'].iloc[-3])):
            df['Engulf'].iloc[-1]=1   
            #print(df.tail(5)) 
            
        return df

    except Exception as e:
            print("Error in calculate_indicator_Engulf" , e )   

def telegram_bot_2HR_BO(symbol,cp):
       
       try:
            t_string = datetime.datetime.strftime(datetime.datetime.now(),"%H:%M")
            TextMsg=''
            spl_word='22APR'
            utkr = symbol.split(spl_word)[0] 
            sttt=symbol.split(spl_word)[1] 
            if symbol.endswith('PE') and symbol not in Posted_2hr_BO:
                Posted_2hr_BO.append(symbol)
                TextMsg="120 MIN Breakout--{0} \n Time_Alert--{1} \n Strike--{2} \n CMP--{3}".format(utkr,t_string,sttt,cp)

            if symbol.endswith('CE') and symbol not in Posted_2hr_BO:
                Posted_2hr_BO.append(symbol)            
                TextMsg="120 MIN Breakout---{0} \n Time_Alert--{1} \n Strike--{2} \n CMP--{3}".format(utkr,t_string,sttt,cp)    
           
            SUD_URL_DEV ='https://api.telegram.org/bot5202677849:AAFCy6pRCuEjCJPlbKA8jTyjVk8wfpLU-Ws/sendMessage?chat_id=-1001799843710&text=' + TextMsg
            
            requests.post(SUD_URL_DEV)
            
       except Exception as e:
            print("Error in Sending to Function" , e )        
     
def telegram_bot_2HR_Engulf(symbol,cp):
       
       try:
            t_string = datetime.datetime.strftime(datetime.datetime.now(),"%H:%M")
            TextMsg=''
            spl_word='22APR'
            utkr = symbol.split(spl_word)[0] 
            sttt=symbol.split(spl_word)[1]
           
            if symbol.endswith('PE') and symbol not in Posted_TCUP2HR:
             
                Posted_TCUP2HR.append(symbol)
                TextMsg="2 HR ENGULF \n Trend Change DOWN--{0} \n Time_Alert--{1} \n Strike--{2} \n CMP--{3}".format(utkr,t_string,sttt,cp)

            if symbol.endswith('CE') and symbol not in Posted_TCUP2HR:
              
                Posted_TCUP2HR.append(symbol)            
                TextMsg="2 HR ENGULF \n Trend Change UP--{0} \n Time_Alert--{1} \n Strike--{2} \n CMP--{3}".format(utkr,t_string,sttt,cp)    
           
           
            SUD_URL_DEV ='https://api.telegram.org/bot5260765805:AAEItsxA727DS4Jhxzk5ADk2m8Q6FApWCoE/sendMessage?chat_id=-1001776893569&text=' + TextMsg
           
            requests.post(SUD_URL_DEV)
            
       except Exception as e:
            print("Error in Sending to Function" , e )        

def telegram_bot_DIV(symbol,cp):
       
       try:
            t_string = datetime.datetime.strftime(datetime.datetime.now(),"%H:%M")
            TextMsg=''
            spl_word='22APR'
            utkr = symbol.split(spl_word)[0] 
            sttt=symbol.split(spl_word)[1]
           
            if symbol.endswith('PE') and symbol not in Posted_DIV:
             
                Posted_DIV.append(symbol)
                TextMsg="15 MIN DIV \n Trend Change DOWN --{0} \n Time_Alert--{1} \n Strike--{2} \n CMP--{3}".format(utkr,t_string,sttt,cp)

            if symbol.endswith('CE') and symbol not in Posted_DIV:
              
                Posted_DIV.append(symbol)            
                TextMsg="15 MIN DIV \n Trend Change UP--{0} \n Time_Alert--{1} \n Strike--{2} \n CMP--{3}".format(utkr,t_string,sttt,cp)    
           
           
            SUD_URL_DEV ='https://api.telegram.org/bot5221707144:AAHSu59g1M1_4j3m-Ubux5_l7fUBknN1UK0/sendMessage?chat_id=-1001615862559&text=' + TextMsg
           
            requests.post(SUD_URL_DEV)
            
       except Exception as e:
            print("Error in Sending to Function" , e )       


   
async def main():

    api.on_connection_started(connection_started)    
    key = "A+94FmyC5FsUXlRVtVaj2tpTE7o="
    host = "apidata.accelpix.in"
    scheme = "http"
    s = await api.initialize(key, host,scheme)
    print(s)
    Expiry=datetime.date(2022,4,28)
    Expiry_index=datetime.date(2022,3,3)
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    Day_start=datetime.datetime.strptime("09:10:00", "%H:%M:%S")
    Day_start_time=Day_start.strftime("%H:%M")
    Day_end = datetime.datetime.strptime("15:31:00", "%H:%M:%S")
    Day_end_time = Day_end.strftime("%H:%M")
    
    ##TIME SETUP
    tt_15_today='20220401'
    ltp_date="20220330"
    tt_15_from="20220320"
   
    
    url = 'https://apidata.accelpix.in/api/hsd/Masters/2?fmt=json'
    
    d = requests.get(url).json()
   
    token_df = pd.DataFrame.from_dict(d)    
    token_df['exp'] = pd.to_datetime(token_df['exp']).apply(lambda x: x.date())
    token_df = token_df.astype({'sp': float}) 
    
    list2=['ADANIENT-1','ADANIPORTS-1','AMBUJACEM-1','APOLLOHOSP-1','APOLLOTYRE-1','ASHOKLEY-1','ASIANPAINT-1','AUROPHARMA-1','AXISBANK-1','BAJFINANCE-1','BALRAMCHIN-1','BANKBARODA-1','BATAINDIA-1','BEL-1','BERGEPAINT-1','BHARATFORG-1','BHARTIARTL-1','BHEL-1','BIOCON-1','BPCL-1','BRITANNIA-1','BSOFT-1','CANBK-1','CHAMBLFERT-1','CHOLAFIN-1','CIPLA-1','COALINDIA-1','COFORGE-1','COLPAL-1','CONCOR-1','CUMMINSIND-1','DABUR-1','DEEPAKNTR-1','DELTACORP-1','DLF-1','DRREDDY-1','EICHERMOT-1','FEDERALBNK-1','FSL-1','GAIL-1','GNFC-1','GODREJCP-1','GODREJPROP-1','GRASIM-1','HAL-1','HAVELLS-1','HCLTECH-1','HDFC-1','HDFCBANK-1','HEROMOTOCO-1','HINDALCO-1','HINDPETRO-1','HINDUNILVR-1','ICICIBANK-1','IEX-1','IGL-1','INDHOTEL-1','INDIACEM-1','INDIGO-1','INDUSINDBK-1','INDUSTOWER-1','INFY-1','INTELLECT-1','IOC-1','IRCTC-1','ITC-1','JINDALSTEL-1','JSWSTEEL-1','JUBLFOOD-1','KOTAKBANK-1','LAURUSLABS-1','LICHSGFIN-1','LT-1','LTI-1','LTTS-1','LUPIN-1','MARICO-1','MARUTI-1','MCDOWELL-N-1','MCX-1','MINDTREE-1','MOTHERSUMI-1','MUTHOOTFIN-1','NAM-INDIA-1','NATIONALUM-1','NAUKRI-1','NMDC-1','OBEROIRLTY-1','ONGC-1','PEL-1','PERSISTENT-1','PFC-1','POLYCAB-1','POWERGRID-1','PVR-1','RECLTD-1','RELIANCE-1','SAIL-1','SBICARD-1','SBILIFE-1','SBIN-1','SRF-1','SRTRANSFIN-1','STAR-1','SUNPHARMA-1','TATACHEM-1','TATACOMM-1','TATACONSUM-1','TATAMOTORS-1','TATAPOWER-1','TATASTEEL-1','TCS-1','TECHM-1','TITAN-1','TORNTPHARM-1','TORNTPOWER-1','TVSMOTOR-1','UBL-1','ULTRACEMCO-1','UPL-1','VEDL-1','VOLTAS-1','WIPRO-1','ZEEL-1']
    
    list1=['ADANIENT','ADANIPORTS','AMBUJACEM','APOLLOHOSP','APOLLOTYRE','ASHOKLEY','ASIANPAINT','AUROPHARMA','AXISBANK','BAJFINANCE','BALRAMCHIN','BANKBARODA','BATAINDIA','BEL','BERGEPAINT','BHARATFORG','BHARTIARTL','BHEL','BIOCON','BPCL','BRITANNIA','BSOFT','CANBK','CHAMBLFERT','CHOLAFIN','CIPLA','COALINDIA','COFORGE','COLPAL','CONCOR','CUMMINSIND','DABUR','DEEPAKNTR','DELTACORP','DLF','DRREDDY','EICHERMOT','FEDERALBNK','FSL','GAIL','GNFC','GODREJCP','GODREJPROP','GRASIM','HAL','HAVELLS','HCLTECH','HDFC','HDFCBANK','HEROMOTOCO','HINDALCO','HINDPETRO','HINDUNILVR','ICICIBANK','IEX','IGL','INDHOTEL','INDIACEM','INDIGO','INDUSINDBK','INDUSTOWER','INFY','INTELLECT','IOC','IRCTC','ITC','JINDALSTEL','JSWSTEEL','JUBLFOOD','KOTAKBANK','LAURUSLABS','LICHSGFIN','LT','LTI','LTTS','LUPIN','MARICO','MARUTI','MCDOWELL-N','MCX','MINDTREE','MOTHERSUMI','MUTHOOTFIN','NAM-INDIA','NATIONALUM','NAUKRI','NMDC','OBEROIRLTY','ONGC','PEL','PERSISTENT','PFC','POLYCAB','POWERGRID','PVR','RECLTD','RELIANCE','SAIL','SBICARD','SBILIFE','SBIN','SRF','SRTRANSFIN','STAR','SUNPHARMA','TATACHEM','TATACOMM','TATACONSUM','TATAMOTORS','TATAPOWER','TATASTEEL','TCS','TECHM','TITAN','TORNTPHARM','TORNTPOWER','TVSMOTOR','UBL','ULTRACEMCO','UPL','VEDL','VOLTAS','WIPRO','ZEEL']


    
    
    
    
    symbol_token = token_df[token_df['inst'].str.contains('OPTSTK') & token_df.utkr.isin(list1)]
    symbol_token = symbol_token[symbol_token['exp']==Expiry]
    print(symbol_token)
    
    fd = pd.DataFrame()
       
    for i in list2:
        Temp_df_CE=pd.DataFrame()
        Temp_df_PE=pd.DataFrame()         
        ltp_val = await api.get_intra_eod(i,ltp_date,tt_15_today,"60")
        symbol_price = pd.DataFrame.from_dict(ltp_val)            
        ltp=symbol_price['cp'].iloc[-1].astype(float)
        
       
        j=list2.index(i)
                   
        Temp_df_CE=symbol_token[(symbol_token['utkr']==list1[j]) & (symbol_token['tkr'].str.endswith('CE')) & (symbol_token['sp'].astype(float) <=ltp*1.05) & (symbol_token['sp'].astype(float)  >= ltp*1.0)]           
        Temp_df_PE=symbol_token[(symbol_token['utkr']==list1[j]) & (symbol_token['tkr'].str.endswith('PE')) & (symbol_token['sp'].astype(float) < ltp*1.0) & (symbol_token['sp'].astype(float)  >= ltp*0.95)]
        fd=fd.append(Temp_df_CE, ignore_index=True)                      
        fd=fd.append(Temp_df_PE, ignore_index=True)       
        
    
    
    while (datetime.datetime.strftime(datetime.datetime.now(), "%H:%M") >= Day_start_time and datetime.datetime.strftime(datetime.datetime.now(),"%H:%M") < Day_end_time):
       
        for i in fd.index:
            try:
                symbol = fd.loc[i]['tkr']
              
                qhis = await api.get_intra_eod(symbol,tt_15_from,tt_15_today,"120")
                symbol_data1 = pd.DataFrame.from_dict(qhis)
                symbol_data1['tkr']=symbol               
                
               
                if len(symbol_data1.index)>14:
                    Technical_data1=calculate_indicator(symbol_data1[['tkr','td','op','hp','lp','cp','vol','oi']]) 
                    if len(Technical_data1.index)>14:                      
                        Technical_data1.reset_index(inplace =True)
                        print(symbol+"--120 MIN BO STARTED")
                        if  Technical_data1['RSI_BO_F'].iloc[-1] == 1:  
                            telegram_bot_2HR_BO(Technical_data1['tkr'].iloc[-1],Technical_data1['cp'].iloc[-1])
                        print(symbol+"--120 MIN BO SCANNING COMPLETED")  
               
               
                Engulf2 = await api.get_intra_eod(symbol,tt_15_from,tt_15_today,"120")
                symbol_data2 = pd.DataFrame.from_dict(Engulf2)
                symbol_data2['tkr']=symbol
               
                if len(symbol_data2.index)>14:
                   
                    Technical_data2=calculate_indicator_Engulf(symbol_data2[['tkr','td','op','hp','lp','cp','vol','oi']])
                   
                    print(symbol+"--2 HR ENGULF STARTED")
                    if  Technical_data2['Engulf'].iloc[-1] == 1:
                        telegram_bot_2HR_Engulf(Technical_data2['tkr'].iloc[-1],Technical_data2['cp'].iloc[-1])                    
                           
                    print(symbol+"--2 HR ENGULF SCANNING COMPLETED")             
                
                
                div = await api.get_intra_eod(symbol,tt_15_from,tt_15_today,"15")
                symbol_data3 = pd.DataFrame.from_dict(div)
                symbol_data3['tkr']=symbol
               
                if len(symbol_data3.index)>14:
                   
                    Technical_data3=calculate_indicator_withvol(symbol_data3[['tkr','td','op','hp','lp','cp','vol','oi']])
                   
                    print(symbol+"--15 Min Div Scanning")
                    if  Technical_data3['RSI_BO_F'].iloc[-1] == 1:                        
                        div2hr = await api.get_intra_eod(symbol,tt_15_from,tt_15_today,"120")
                        symbol_data4 = pd.DataFrame.from_dict(div2hr)
                        symbol_data4['tkr']=symbol                   
                        if len(symbol_data4.index)>14:
                            Technical_data4=calculate_indicator_div(symbol_data4[['tkr','td','op','hp','lp','cp','vol','oi']])
                            if  Technical_data4['RSI_BO_F'].iloc[-1] == 1:
                                telegram_bot_DIV(Technical_data4['tkr'].iloc[-1],Technical_data4['cp'].iloc[-1]) 
                    print(symbol+"--15 Min Div Scanning")               
                
               
                              
                        
             
                
                
            except Exception as e:
                print("Error in Sending to Telegram 123" , e)
        
        if datetime.datetime.strftime(datetime.datetime.now(),"%H:%M") > Day_end_time:
            break

def connection_started():
    print("Connection started callback")

def connection_stopped():
    print("Connection stopped callback")

event_loop.create_task(main())
try:
   event_loop.run_forever()
finally:
   event_loop.close()