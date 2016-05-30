

# -*- coding: utf-8 -*-
"""
Created on Wed May 11 09:11:30 2016

@author: telmo
"""



#Import libraries
#psycopg2: SQL connector library
#os: methods to check files in a directory
#datetime:date processing
#numpy and panda for dataframes
import psycopg2
import os
from datetime import datetime
import numpy as np 
import pandas as pd 



def which_day(table_identifier, date_indentifier):

	date_snapshot = datetime.strptime(date_indentifier, '%y%m%d')
	a = datetime.strptime(table_identifier[3:9], '%y%m%d')
	delta =  (a - date_snapshot).days


	if delta == 0 or delta == 1 or delta == 2 or delta == 3 or delta == 5 or delta == 7 or delta == 10 or delta == 13 or delta == 17 or delta == 21 or delta == 26 or delta == 32 or delta == 40 or delta == 48 or delta == 59 or delta == 70 or delta == 82 or delta == 97 or delta == 114 or delta == 136 or delta == 173 or delta == 219 or delta == 293:	
		return delta

	return -1



#method to fill the table
def fill_table(table_identifier, bookings_fare_day, delta, df, columns_dataframe):


	bookings_fare_day.append(delta)
	df.loc[delta] = bookings_fare_day


	df.to_csv('C:\\Users\\Telmo\\Desktop\\Python\\Reports SN\\data_flights\\' + table_identifier +".csv", index = False)
	return
	




def which_route(first_city, second_city):
	if first_city == 'GVA' or  second_city == 'GVA':
		return 'gva'
	elif first_city == 'BUD' or  second_city == 'BUD':
		return 'bud'
	elif first_city == 'FCO' or  second_city == 'FCO':
		return 'fco'
	elif first_city == 'IAD' or  second_city == 'IAD':
		return 'iad'
	elif first_city == 'JFK' or  second_city == 'JFK':
		return 'jfk'

	return 'na'






all_fares = "J,C,D,Z,P,I,R,O,Y,B,M,U,H,Q,V,W,S,T,E,L,K,N,G,X,A"
fares = all_fares.split(',')

fares.append("days_to_departure")
accumlated_data = []






#Loops through all the leg files in a directory
for file in os.listdir("C:\\Users\\Telmo\\Desktop\\Python\\Reports SN\\Daily Bookings"):
	if file.endswith(".LEG"):


		#Opens an individual file
		with open('C:\\Users\\Telmo\\Desktop\\Python\\Reports SN\\Daily Bookings\\' + file) as inputfile:
			#Loops through the lines of the file

			for line in inputfile:
				#Breaks the information in a line into a list
				aux = line.strip().split(',')
				
				#if the line starts with 01 it will have the flight information
				if aux[0] == '01':
					

					airport = which_route(aux[3].strip(), aux[4].strip())
					if airport == 'na':
						inputfile.close()
						break
					
					#checks if table exists, if not, create table
					flight_identifier = airport + aux[5] + aux[2]

					delta = which_day(flight_identifier, "150601")

					if delta == -1:
						continue

					if (flight_identifier +".csv") in os.listdir("C:\\Users\\Telmo\\Desktop\\Python\\Reports SN\\data_flights"):
						data_df = pd.read_csv('C:\\Users\\Telmo\\Desktop\\Python\\Reports SN\\data_flights\\' + flight_identifier +".csv")

					else:
						data_df = pd.DataFrame(columns= fares)



				if aux[0] == '03':

					if delta == -1:
							continue

					accumlated_data.append(int(aux[5]))
					#method to fill the table

				if aux[0] == '02' and aux[1] == 'Y':

					if delta == -1:
							continue
					fill_table(flight_identifier , accumlated_data, delta , data_df, fares)
					del accumlated_data[:]



		#Close the file
		inputfile.close()