import os
import pandas as pd
import json
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

#DB connection
mydb = mysql.connector.connect(
  host="localhost",
  user="dinesh",
  password="dinesh@1234"
)
mycursor = mydb.cursor()

#Processing the agg trans data and loading to database
aggr_trans_path="C:\Dinesh\Python\Git/pulse/data/aggregated/transaction/country/india/state/"
Agg_trans_state_list=os.listdir(aggr_trans_path)
#Agg_trans_state_list

Agg_trans_clm={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}

for ati in Agg_trans_state_list:
    p_ati=aggr_trans_path+ati+"/"
    Agg_trans_yr=os.listdir(p_ati)
    for atj in Agg_trans_yr:
        p_atj=p_ati+atj+"/"
        Agg_trans_yr_list=os.listdir(p_atj)
        for atk in Agg_trans_yr_list:
            p_atk=p_atj+atk
            Data=open(p_atk,'r')
            D=json.load(Data)
            for atz in D['data']['transactionData']:
              Name=atz['name']
              count=atz['paymentInstruments'][0]['count']
              amount=atz['paymentInstruments'][0]['amount']
              Agg_trans_clm['Transacion_type'].append(Name)
              Agg_trans_clm['Transacion_count'].append(count)
              Agg_trans_clm['Transacion_amount'].append(amount)
              Agg_trans_clm['State'].append(ati)
              Agg_trans_clm['Year'].append(atj)
              Agg_trans_clm['Quater'].append(int(atk.strip('.json')))
#Succesfully created a dataframe
Agg_Trans=pd.DataFrame(Agg_trans_clm)

#Delete before inserting
aggr_trans_del="DELETE FROM DINESH.ARR_TRANS_DATA" 
aggr_user_del="DELETE FROM DINESH.ARR_USER_DATA"
map_trans_del="DELETE FROM DINESH.MAP_TRANS_DATA" 
map_user_del="DELETE FROM DINESH.MAP_USER_DATA"
top_trans_del="DELETE FROM DINESH.TOP_TRANS_DATA" 
top_user_del="DELETE FROM DINESH.TOP_USER_DATA"
try:
    mycursor.execute(aggr_trans_del)
    mycursor.execute(aggr_user_del)
    mycursor.execute(map_trans_del)
    mycursor.execute(map_user_del)
    mycursor.execute(top_trans_del)
    mycursor.execute(top_user_del)
    mydb.commit()
except  :
   #st.write('test2')
    mydb.rollback()
#Insert agg trans data into Mysql DB
for index, row in Agg_Trans.iterrows():
  agg_trans_sql = "INSERT INTO dinesh.ARR_TRANS_DATA(TRANS_STATE,TRANS_YEAR,TRANS_QRT,TRANS_TYPE,TRANS_AMT,TRANS_CNT) VALUES (%s,%s,%s,%s,%s,%s)"
  agg_trans_valu= row.State,row.Year,row.Quater,row.Transacion_type,row.Transacion_amount,row.Transacion_count
  try:
    mycursor.execute(agg_trans_sql, agg_trans_valu)
    mydb.commit()
  except  :
   #st.write('test2')
    mydb.rollback()  

#print(Agg_Trans)

#Processing the agg user data and loading to database
aggr_user_path="C:\Dinesh\Python\Git/pulse/data/aggregated/user/country/india/state/"
Agg_user_state_list=os.listdir(aggr_user_path)
#Agg_user_state_list

Agg_user_clm={'user_State':[], 'user_Year':[],'user_Quater':[],'user_device':[], 'user_cnt':[], 'user_perc':[]}

for aui in Agg_user_state_list:
    p_aui=aggr_user_path+aui+"/"
    Agg_user_yr=os.listdir(p_aui)
    for auj in Agg_user_yr:
        p_auj=p_aui+auj+"/"
        Agg_user_yr_list=os.listdir(p_auj)
        for auk in Agg_user_yr_list:
            p_auk=p_auj+auk
            user_Data=open(p_auk,'r')
            u_D=json.load(user_Data)
            if u_D['data']['usersByDevice'] is not None :
                for auz in u_D['data']['usersByDevice']:
                    brand=auz['brand']
                    brand=auz['brand']
                    ucount=auz['count']
                    uperc=auz['percentage']
                    Agg_user_clm['user_device'].append(brand)
                    Agg_user_clm['user_cnt'].append(ucount)
                    Agg_user_clm['user_perc'].append(uperc)
                    Agg_user_clm['user_State'].append(aui)
                    Agg_user_clm['user_Year'].append(auj)
                    Agg_user_clm['user_Quater'].append(int(auk.strip('.json')))
#Succesfully created a dataframe                    
Agg_user=pd.DataFrame(Agg_user_clm) 
#print(Agg_user)   
#Insert agg user data into Mysql DB
for index, row in Agg_user.iterrows():
  agg_user_sql = "INSERT INTO dinesh.ARR_USER_DATA(USER_STATE,USER_YEAR,USER_QRT,USER_DEV,USER_CNT,USER_PER) VALUES (%s,%s,%s,%s,%s,%s)"
  agg_user_valu= row.user_State,row.user_Year,row.user_Quater,row.user_device,row.user_cnt,row.user_perc
  try:
    mycursor.execute(agg_user_sql, agg_user_valu)
    mydb.commit()
  except  :
   #st.write('test2')
    mydb.rollback()                  
#Processing the map trans data and loading to database                                
map_trans_path="C:\Dinesh\Python\Git/pulse/data/map/transaction/hover/country/india/state/"
Map_trans_state_list=os.listdir(map_trans_path)
#Map_trans_state_list

Map_trans_clm={'map_State':[], 'map_Year':[],'map_Quater':[],'map_Transacion_dist':[], 'map_Transacion_count':[], 'map_Transacion_amount':[]}

for mti in Map_trans_state_list:
    p_mti=map_trans_path+mti+"/"
    Map_trans_yr=os.listdir(p_mti)
    for mtj in Map_trans_yr:
        p_mtj=p_mti+mtj+"/"
        Map_trans_yr_list=os.listdir(p_mtj)
        for mtk in Map_trans_yr_list:
            p_mtk=p_mtj+mtk
            mt_Data=open(p_mtk,'r')
            mt_D=json.load(mt_Data)
            if mt_D['data']['hoverDataList'] is not None :
             for mtz in mt_D['data']['hoverDataList']:
              dist_Name=mtz['name']
              mt_count=mtz['metric'][0]['count']
              mt_amount=mtz['metric'][0]['amount']
              Map_trans_clm['map_Transacion_dist'].append(dist_Name)
              Map_trans_clm['map_Transacion_count'].append(mt_count)
              Map_trans_clm['map_Transacion_amount'].append(mt_amount)
              Map_trans_clm['map_State'].append(mti)
              Map_trans_clm['map_Year'].append(mtj)
              Map_trans_clm['map_Quater'].append(int(mtk.strip('.json')))
#Succesfully created a dataframe
Map_trans=pd.DataFrame(Map_trans_clm)

#Insert map trans data into Mysql DB
for index, row in Map_trans.iterrows():
  Map_trans_sql = "INSERT INTO dinesh.MAP_TRANS_DATA(TRANS_STATE,TRANS_YEAR,TRANS_QRT,TRANS_DIST,TRANS_AMT,TRANS_CNT) VALUES (%s,%s,%s,%s,%s,%s)"
  Map_trans_valu= row.map_State,row.map_Year,row.map_Quater,row.map_Transacion_dist,row.map_Transacion_amount,row.map_Transacion_count
  try:
    mycursor.execute(Map_trans_sql, Map_trans_valu)
    mydb.commit()
  except  :
   #st.write('test2')
    mydb.rollback()  

#print(Map_trans)

#Processing the map user data and loading to database
map_user_path="C:\Dinesh\Python\Git/pulse/data/map/user/hover/country/india/state/"
map_user_state_list=os.listdir(map_user_path)
#map_user_state_list
map_user_clm={'map_user_State':[], 'map_user_Year':[],'map_user_Quater':[],'map_user_dist':[], 'map_user_cnt':[]}

for mui in map_user_state_list:
    p_mui=map_user_path+mui+"/"
    map_user_yr=os.listdir(p_mui)
    for muj in map_user_yr:
        p_muj=p_mui+muj+"/"
        map_user_yr_list=os.listdir(p_muj)
        for muk in map_user_yr_list:
            p_muk=p_muj+muk
            muser_Data=open(p_muk,'r')
            mu_D=json.load(muser_Data)
            if mu_D['data']['hoverData'] is not None :
                for muz in mu_D['data']['hoverData']:
                    mu_dist=muz
                    mucount=mu_D['data']['hoverData'][muz]['registeredUsers']
                    map_user_clm['map_user_dist'].append(mu_dist)
                    map_user_clm['map_user_cnt'].append(mucount)
                    map_user_clm['map_user_State'].append(mui)
                    map_user_clm['map_user_Year'].append(muj)
                    map_user_clm['map_user_Quater'].append(int(muk.strip('.json')))
#Succesfully created a dataframe                    
map_user=pd.DataFrame(map_user_clm) 
#print(Agg_user)   
#Insert agg user data into Mysql DB
for index, row in map_user.iterrows():
  map_user_sql = "INSERT INTO DINESH.MAP_USER_DATA(USER_STATE,USER_YEAR,USER_QRT,USER_DIST,USER_CNT) VALUES (%s,%s,%s,%s,%s)"
  map_user_valu= row.map_user_State,row.map_user_Year,row.map_user_Quater,row.map_user_dist,row.map_user_cnt
  try:
    mycursor.execute(map_user_sql, map_user_valu)
    mydb.commit()
  except  :
   #st.write('test2')
    mydb.rollback()  	

#Processing the top trans data and loading to database
top_trans_path="C:\Dinesh\Python\Git/pulse/data/top/transaction/country/india/state/"
top_trans_state_list=os.listdir(top_trans_path)
#top_trans_state_list

top_trans_clm={'top_State':[], 'top_Year':[],'top_Quater':[],'top_Transacion_dist':[], 'top_Transacion_count':[], 'top_Transacion_amount':[]}

for tti in top_trans_state_list:
    p_tti=top_trans_path+tti+"/"
    top_trans_yr=os.listdir(p_tti)
    for ttj in top_trans_yr:
        p_ttj=p_tti+ttj+"/"
        top_trans_yr_list=os.listdir(p_ttj)
        for ttk in top_trans_yr_list:
            p_ttk=p_ttj+ttk
            tt_Data=open(p_ttk,'r')
            tt_D=json.load(tt_Data)
            if tt_D['data']['pincodes'] is not None :
             for ttz in tt_D['data']['pincodes']:
              dist_pin=ttz['entityName']
              mt_count=ttz['metric']['count']
              mt_amount=ttz['metric']['amount']
              top_trans_clm['top_Transacion_dist'].append(dist_pin)
              top_trans_clm['top_Transacion_count'].append(mt_count)
              top_trans_clm['top_Transacion_amount'].append(mt_amount)
              top_trans_clm['top_State'].append(tti)
              top_trans_clm['top_Year'].append(ttj)
              top_trans_clm['top_Quater'].append(int(ttk.strip('.json')))
#Succesfully created a dataframe
top_trans=pd.DataFrame(top_trans_clm)

#Insert top trans data into Mysql DB
for index, row in top_trans.iterrows():
  top_trans_sql = "INSERT INTO dinesh.TOP_TRANS_DATA(TRANS_STATE,TRANS_YEAR,TRANS_QRT,TRANS_PIN,TRANS_AMT,TRANS_CNT) VALUES (%s,%s,%s,%s,%s,%s)"
  top_trans_valu= row.top_State,row.top_Year,row.top_Quater,row.top_Transacion_dist,row.top_Transacion_amount,row.top_Transacion_count
  try:
    mycursor.execute(top_trans_sql, top_trans_valu)
    mydb.commit()
  except  :
   #st.write('test2')
    mydb.rollback()  



#Processing the top user data and loading to database
top_user_path="C:\Dinesh\Python\Git/pulse/data/top/user/country/india/state/"
top_user_state_list=os.listdir(top_user_path)
#top_user_state_list
top_user_clm={'top_user_State':[], 'top_user_Year':[],'top_user_Quater':[],'top_user_pin':[], 'top_user_cnt':[]}

for tui in top_user_state_list:
    p_tui=top_user_path+tui+"/"
    top_user_yr=os.listdir(p_tui)
    for tuj in top_user_yr:
        p_tuj=p_tui+tuj+"/"
        top_user_yr_list=os.listdir(p_tuj)
        for tuk in top_user_yr_list:
            p_tuk=p_tuj+tuk
            tuser_Data=open(p_tuk,'r')
            tu_D=json.load(tuser_Data)
            if tu_D['data']['pincodes'] is not None :
                for tuz in tu_D['data']['pincodes']:
                    tu_pin=tuz['name']
                    tucount=tuz['registeredUsers']
                    top_user_clm['top_user_pin'].append(tu_pin)
                    top_user_clm['top_user_cnt'].append(tucount)
                    top_user_clm['top_user_State'].append(tui)
                    top_user_clm['top_user_Year'].append(tuj)
                    top_user_clm['top_user_Quater'].append(int(tuk.strip('.json')))
#Succesfully created a dataframe                    
top_user=pd.DataFrame(top_user_clm) 
#print(Agg_user)   
#Insert top user data into Mysql DB
for index, row in top_user.iterrows():
  top_user_sql = "INSERT INTO DINESH.TOP_USER_DATA(USER_STATE,USER_YEAR,USER_QRT,USER_PIN,USER_CNT) VALUES (%s,%s,%s,%s,%s)"
  top_user_valu= row.top_user_State,row.top_user_Year,row.top_user_Quater,row.top_user_pin,row.top_user_cnt
  try:
    mycursor.execute(top_user_sql, top_user_valu)
    mydb.commit()
  except  :
   #st.write('test2')
    mydb.rollback()        