# PhonePe_Project2
This the project#2 for Phonepe transactions. 

The Phonepe data is available in Github repository, we need to write a code to clone the git repositary data to local machine,load it to the database and plot it in India map to show the amount of transacion/User on each state.

Description: This project is to clone the data repository from Github to our local machine. Analyze the data clean the data as much as possible and load it to the MYSQL database.After loading it to the database we need to write the appropriate queries to get the data from the database.We need to write a pythone program to load the data from the files which were cloned to local machine to database.Once the data is loaded we need write a streamlit code to show India map and the values of the states will be colored accordingly.

Database: We need to create three table for the above requirements, 
ARR_TRANS_DATA - Will store the quarterly arrgrigated transaction data for yearly for all states
ARR_USER_DATA - Will store the quarterly arrgrigated user data for yearly for all states
MAP_TRANS_DATA  - Will store the quarterly transaction data for yearly for all states at disctict level
MAP_USER_DATA - Will store the quarterly  user data for yearly for all states  at disctict level
TOP_TRANS_DATA  - Will store the quarterly transaction data for yearly for all states at pincode level
TOP_USER_DATA - Will store the quarterly user data for yearly for all states  at pincode level
MAP_COORDINATES - will store the cooridinates of each state

After cleaning up we need to connnect the Database from python and get the data.
After which we need use Python package Streamlit to create fields, button and list. When the user selects the Transaction/Users from the first drop down and year and quarter from the second drop down, the map will be populated with the appropriate data of the year and quarter for that transaction/User. There will be a button "Explore Data" whihc will take us to the real numerical data which shows total & avg transaction,categorical data and top ten transactions for state level,district level and pioncode level data.
