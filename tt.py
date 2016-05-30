import csv
import pandas as pd
import matplotlib.pyplot as plt



myfile = open('C:\\Users\\Telmo\\Desktop\\Python\\Face\\lista.csv', 'r')
spamreader = csv.reader(myfile)
for row in spamreader:
	b = row
	break

myfile.close()

x = []
y = []
time = []
ano = []

new_dataframe = pd.read_csv('C:\\Users\\Telmo\\Desktop\\Python\\Face\\train.csv')

for i in range(336):
	ano.append(i)


for i in range(336) :
	df  = new_dataframe[((new_dataframe['time'] // 60) == i)]

	df_new = df[df['place_id'] == int(b[0])]

	time.append(df_new['x'].count())

	print (i)
	# x.append(df_new['x'].mean())
	# y.append(df_new['y'].mean())

	
	

plt.plot(ano, time)
plt.show()


# for place in b:
# 	df  = new_dataframe[new_dataframe['place_id'] == int(place)]
# 	x.append(df['x'].mean())
# 	y.append(df['y'].mean())
# 	time.append
# 	print (place)









# plt.plot(x, y, 'ro')
# plt.axis([0, 10, 0, 10])
# plt.show()