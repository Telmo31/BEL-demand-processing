

import pandas as pd
import os 
from datetime import datetime





data_accumulator = []
columns =[]


for i in range (342):
	columns.append('t_' + str(i))

columns.append('flight_number')
columns.append('DoW')
columns.append('Month')
columns.append('Year')



i = 0
a = 0


new_dataframe = pd.DataFrame(columns = columns)
new_dataframe2 = pd.DataFrame(columns = columns)

for file in os.listdir("C:\\Users\\Telmo\\Desktop\\Python\\Reports SN\\data_flights"):
	#Opens an individual file

	data_df = pd.read_csv('C:\\Users\\Telmo\\Desktop\\Python\\Reports SN\\data_flights\\' + file)


	col_list = list(data_df)
	col_list.remove('days_to_departure')

	data_accumulator = data_df[col_list].sum(axis=1).tolist()

	first_point = data_df['days_to_departure'].loc[0]

	last_point = int(data_df['days_to_departure'].tail(1))



	columns_aux = columns[int(first_point):last_point + 1] 

	columns_aux.extend(columns[-4:])

	dep_date = datetime.strptime(file[3:9], '%y%m%d')
	data_accumulator.append(file[9:14])
	data_accumulator.append(dep_date.isoweekday())
	data_accumulator.append(dep_date.month)
	data_accumulator.append(dep_date.year)




	aux_df = pd.DataFrame([data_accumulator], columns = columns_aux)



	new_dataframe2 = pd.concat([new_dataframe2, aux_df], ignore_index = True)


	print(len(new_dataframe2.index))


	b = a
	i += 1

	a = i//1000 
	

	if b != a:
		new_dataframe = new_dataframe2.reindex_axis(new_dataframe.columns, axis=1)
		new_dataframe.to_csv('C:\\Users\\Telmo\\Desktop\\Python\\Reports SN\\data_flights\\teste'+ str(b) + ".csv", index = False)
		new_dataframe2 = pd.DataFrame(columns = columns)






new_dataframe = new_dataframe2.reindex_axis(new_dataframe.columns, axis=1)
new_dataframe.to_csv('C:\\Users\\Telmo\\Desktop\\Python\\Reports SN\\data_flights\\teste'+ str(a) + ".csv", index = False)

	







