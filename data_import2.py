
# -*- coding: utf-8 -*-
"""
Created on Wed May 11 09:11:30 2016

@author: telmo
"""
#Import libraries
#psycopg2: SQL connector library
#os: methods to check files in a directory
import psycopg2
import os





#auxiliary list to keep some data temporarly
aux = []
accumlated_data = []



#method to fill the table
def fill_table(table_identifier, class_fare, bookings_fare_day, date_indentifier):

	#Verifies if the flight already exists in the database if so it updates that row with the correct values, if not creates a row for the flight
	cur.execute("select exists(select * FROM " + table_identifier + " WHERE Date_Gathering='" + date_indentifier + "');")
	if not cur.fetchone()[0]:
		cur.execute("INSERT INTO " + table_identifier + " (Date_Gathering, " + all_fares + ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (date_indentifier, bookings_fare_day[0], bookings_fare_day[1], bookings_fare_day[2], bookings_fare_day[3], bookings_fare_day[4], bookings_fare_day[5], bookings_fare_day[6], bookings_fare_day[7], bookings_fare_day[8], bookings_fare_day[9], bookings_fare_day[10], bookings_fare_day[11], bookings_fare_day[12], bookings_fare_day[13], bookings_fare_day[14], bookings_fare_day[15], bookings_fare_day[16], bookings_fare_day[17], bookings_fare_day[18], bookings_fare_day[19], bookings_fare_day[20], bookings_fare_day[21], bookings_fare_day[22], bookings_fare_day[23], bookings_fare_day[24],))
	else: cur.execute("UPDATE " + table_identifier + " SET "+ class_fare + "=" + bookings_fare_day + " WHERE Date_Gathering='" + date_indentifier + "';")



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







     
# Connect to an existing database
conn = psycopg2.connect(database="test5", user="postgres", password="postgres")

# Open a cursor to perform database operations
cur = conn.cursor()

#Variable definition to use in the queries
C_fares = "J integer, C integer, D integer, Z integer, P integer, I integer, R integer, O integer, "
Y_fares1 = "Y integer, B integer, M integer, U integer, H integer, Q integer, V integer, W integer, "
Y_fares2 = "S integer, T integer, E integer, L integer, K integer, N integer, G integer, X integer, A integer);"
all_fares = "J, C, D, Z, P, I, R, O, Y, B, M, U, H, Q, V, W, S, T, E, L, K, N, G, X, A"

#Loops through all the leg files in a directory
for file in os.listdir("C:\\Users\\Telmo\\Desktop\\Python\\Reports SN\\Daily Bookings"):
	if file.endswith(".LEG"):
		print (file)

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
					cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (flight_identifier,))
					
					if not cur.fetchone()[0]:
							
						# Execute a command: this creates a new table
						cur.execute("CREATE TABLE " + flight_identifier + " (id serial PRIMARY KEY, Date_Gathering varchar, " + C_fares + Y_fares1 + Y_fares2 )


				if aux[0] == '03':

					accumlated_data.append(int(aux[5]))
					#method to fill the table

				if aux[0] == '02' and aux[1] == 'Y':
					fill_table(flight_identifier , aux[2].strip(), accumlated_data, "150601" )
					del accumlated_data[:]



		#Close the file
		inputfile.close()
		# Make the changes to the database persistent
		conn.commit()
					


cur.execute("SELECT * FROM GVA15102602715")
print(cur.fetchall())



# Close communication with the database
cur.close()
conn.close()

