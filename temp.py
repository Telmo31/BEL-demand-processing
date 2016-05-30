


import pandas as pd
import matplotlib.pyplot as plt
import csv



b = []
loc = []

new_dataframe = pd.read_csv('C:\\Users\\Telmo\\Desktop\\Python\\Face\\train.csv')

a =new_dataframe.describe()

for place in new_dataframe['place_id']:
	if str(place).endswith("13"):
		if place in b:
			continue
		b.append(place)
		print ("ola")


print(len(b))
myfile = open('C:\\Users\\Telmo\\Desktop\\Python\\Face\\lista.csv', 'w')
wr = csv.writer(myfile)
wr.writerow(b)


#new_dataframe[new_dataframe['place_id'] == b[0]]

# for idx in b:
# 	new_dataframe[new_dataframe['place_id'] == idx]









