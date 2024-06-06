import streamlit as st
import pandas as pd 
import plotly.express as px
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
import humanize

#connecting to DB
mydb = mysql.connector.connect(
  host="localhost",
  user="dinesh",
  password="dinesh@1234"
)
mycursor = mydb.cursor()
#Query to get the state and it's transaction count
map_sql='select  MAP_STATE,SUM(TRANS_CNT) AS TRANS_CNT  from DINESH.MAP_TRANS_DATA mt  join DINESH.MAP_COORDINATES mu on mt.TRANS_STATE = mu.DATA_STATE   WHERE TRANS_YEAR = %s AND TRANS_QRT = %s GROUP BY MAP_STATE ORDER BY 2 DESC'

#st.write(df)
#df['states'] = df[0]
#df['transation']=df[1]
#Query to get the state and it's user count
map_user_sql='select  MAP_STATE,SUM(USER_CNT) AS USER_CNT  from DINESH.MAP_USER_DATA mt  join DINESH.MAP_COORDINATES mu on mt.USER_STATE = mu.DATA_STATE   WHERE USER_YEAR = %s AND USER_QRT = %s GROUP BY MAP_STATE ORDER BY 2 DESC'
dropdown2_sql='SELECT DISTINCT TRANS_YEAR,TRANS_QRT FROM DINESH.MAP_TRANS_DATA'

mycursor.execute(dropdown2_sql)
dd2 = pd.DataFrame(mycursor.fetchall())
dd2_list=dd2.values.tolist()
st.set_page_config(layout="wide")
st.title("PhonePe Data Exploring")
col1, col2= st.columns(2)
#side_1, side_2 = st.beta_columns(2)
#button_ED=st.button("Explore Data")
#sepperating the page into 2 columns
with col1:
  #based on the dropdown selected the map will be colored
  dropdown = st.selectbox("Select an option", ('Transactions','Users'))
  dropdown2 = st.selectbox("Select an Year & Quater", dd2_list)  
  if dropdown== "Transactions":
    map_sql_val=(dropdown2[0],dropdown2[1])
    mycursor.execute(map_sql,map_sql_val)  
    df = pd.DataFrame(mycursor.fetchall())
    
  if dropdown== "Users":  
    map_user_sql_val=(dropdown2[0],dropdown2[1])
    mycursor.execute(map_user_sql,map_user_sql_val)
    df = pd.DataFrame(mycursor.fetchall())

  fig = px.choropleth(
          df,
          geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
          featureidkey='properties.ST_NM',
          locations=0,
          color=1,
          color_continuous_scale='Reds',
          labels={'1':'Counts',
                  '0':'State'}
          )
  fig.update_geos(fitbounds='locations', visible=False)
  fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
  st.plotly_chart(fig, use_container_width=True)
  st.write('State Level User Data')
  map_user_stsql = 'SELECT USER_STATE,SUM(USER_CNT) FROM DINESH.MAP_USER_DATA GROUP BY USER_STATE ORDER BY 2 DESC'
  mycursor.execute(map_user_stsql)
  df_map_user_st = pd.DataFrame(mycursor.fetchall())
  #st.write(df_map_user_st)
  fig_st = px.histogram(df_map_user_st,  x=0,y=1)
  st.plotly_chart(fig_st, use_container_width=True)

with col2:
  drp2_sql_val=(dropdown2[0],dropdown2[1])
  #clicking on explore button following details will be shown
  if st.button("Explore Data"):
    total_tran='SELECT CASE         WHEN TRANS_AMT >= 10000000 THEN CONCAT(ROUND(TRANS_AMT / 10000000, 2), " Crores")         WHEN TRANS_AMT >= 100000 THEN CONCAT(ROUND(TRANS_AMT / 100000, 2), " Lakhs")         WHEN TRANS_AMT >= 1000 THEN CONCAT(ROUND(TRANS_AMT / 1000, 2), " Thousands")         ELSE CONCAT(TRANS_AMT, " Units")     END AS formatted_amount, CASE         WHEN ATRANS_AMT >= 10000000 THEN CONCAT(ROUND(ATRANS_AMT / 10000000, 2), " Crores")         WHEN ATRANS_AMT >= 100000 THEN CONCAT(ROUND(ATRANS_AMT / 100000, 2), " Lakhs")         WHEN ATRANS_AMT >= 1000 THEN CONCAT(ROUND(ATRANS_AMT / 1000, 2), " Thousands")         ELSE CONCAT(ATRANS_AMT, " Units")     END AS Aformatted_amount FROM(SELECT SUM(TRANS_AMT) AS TRANS_AMT,AVG(TRANS_AMT) AS ATRANS_AMT FROM DINESH.ARR_TRANS_DATA WHERE TRANS_YEAR = %s AND TRANS_QRT = %s) A'
    mycursor.execute(total_tran,drp2_sql_val)
    total_ret= mycursor.fetchall()  
    cat_tran='SELECT TRANS_TYPE,CASE         WHEN TRANS_AMT >= 10000000 THEN CONCAT(ROUND(TRANS_AMT / 10000000, 2), " Crores")         WHEN TRANS_AMT >= 100000 THEN CONCAT(ROUND(TRANS_AMT / 100000, 2), " Lakhs")         WHEN TRANS_AMT >= 1000 THEN CONCAT(ROUND(TRANS_AMT / 1000, 2), " Thousands")         ELSE CONCAT(TRANS_AMT, " Units")     END AS formatted_amount FROM (SELECT TRANS_TYPE,SUM(TRANS_AMT) AS TRANS_AMT FROM DINESH.ARR_TRANS_DATA  WHERE TRANS_YEAR = %s AND TRANS_QRT = %s GROUP BY TRANS_TYPE ORDER BY 2 DESC) A'
    mycursor.execute(cat_tran,drp2_sql_val)
    total_cat_ret= pd.DataFrame(mycursor.fetchall())
    st.write('Total Transactions for Selected Year & Quater:', humanize.intword(total_ret[0][0]),'and Average Transaction :',humanize.intword(total_ret[0][1]))
    total_cat_ret['Transaction Type'] = total_cat_ret[0]
    total_cat_ret['Transaction Amount'] = total_cat_ret[1]
    #total_cat_ret=
    st.write('Categories:',total_cat_ret[['Transaction Type','Transaction Amount']] )
    top_10_state='SELECT TRANS_STATE,CASE         WHEN TRANS_AMT >= 10000000 THEN CONCAT(ROUND(TRANS_AMT / 10000000, 2), " Crores")         WHEN TRANS_AMT >= 100000 THEN CONCAT(ROUND(TRANS_AMT / 100000, 2), " Lakhs")         WHEN TRANS_AMT >= 1000 THEN CONCAT(ROUND(TRANS_AMT / 1000, 2), " Thousands")         ELSE CONCAT(TRANS_AMT, " Units")     END AS formatted_amount FROM (SELECT TRANS_STATE,SUM(TRANS_AMT) AS TRANS_AMT FROM DINESH.TOP_TRANS_DATA  WHERE TRANS_YEAR = %s AND TRANS_QRT = %s GROUP BY TRANS_STATE ORDER BY 2 DESC LIMIT 10) A'
    mycursor.execute(top_10_state,drp2_sql_val)
    total_10st_ret= pd.DataFrame(mycursor.fetchall())
    total_10st_ret['Transaction State'] = total_10st_ret[0]
    total_10st_ret['Transaction Amount'] = total_10st_ret[1]
    top_10_pin='SELECT TRANS_PIN,CASE         WHEN TRANS_AMT >= 10000000 THEN CONCAT(ROUND(TRANS_AMT / 10000000, 2), " Crores")         WHEN TRANS_AMT >= 100000 THEN CONCAT(ROUND(TRANS_AMT / 100000, 2), " Lakhs")         WHEN TRANS_AMT >= 1000 THEN CONCAT(ROUND(TRANS_AMT / 1000, 2), " Thousands")         ELSE CONCAT(TRANS_AMT, " Units")     END AS formatted_amount FROM (SELECT TRANS_PIN,SUM(TRANS_AMT) AS TRANS_AMT FROM DINESH.TOP_TRANS_DATA  WHERE TRANS_YEAR = %s AND TRANS_QRT = %s GROUP BY TRANS_PIN ORDER BY 2 DESC LIMIT 10) A'
    mycursor.execute(top_10_pin,drp2_sql_val)
    total_10pin_ret= pd.DataFrame(mycursor.fetchall())
    total_10pin_ret['Transaction Pincode'] = total_10pin_ret[0]
    total_10pin_ret['Transaction Amount'] = total_10pin_ret[1]
    top_10_dist='SELECT TRANS_DIST,CASE         WHEN TRANS_AMT >= 10000000 THEN CONCAT(ROUND(TRANS_AMT / 10000000, 2), " Crores")         WHEN TRANS_AMT >= 100000 THEN CONCAT(ROUND(TRANS_AMT / 100000, 2), " Lakhs")         WHEN TRANS_AMT >= 1000 THEN CONCAT(ROUND(TRANS_AMT / 1000, 2), " Thousands")         ELSE CONCAT(TRANS_AMT, " Units")     END AS formatted_amount FROM (SELECT TRANS_DIST,SUM(TRANS_AMT) AS TRANS_AMT FROM DINESH.MAP_TRANS_DATA  WHERE TRANS_YEAR = %s AND TRANS_QRT = %s GROUP BY TRANS_DIST ORDER BY 2 DESC LIMIT 10) A'
    mycursor.execute(top_10_dist,drp2_sql_val)
    total_10dist_ret= pd.DataFrame(mycursor.fetchall())
    total_10dist_ret['Transaction District'] = total_10dist_ret[0]
    total_10dist_ret['Transaction Amount'] = total_10dist_ret[1]
    st.write('Top 10 Transactions' )
    st.write('States:',total_10st_ret[['Transaction State','Transaction Amount']] ,'Districts :',total_10dist_ret[['Transaction District','Transaction Amount']],'Postal Codes :',total_10pin_ret[['Transaction Pincode','Transaction Amount']])  





  