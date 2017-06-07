from kafka import KafkaClient,KafkaProducer
import pandas as pd
import datetime
import numpy as np
import csv
import random
import time
import calendar

x = 95
a = datetime.datetime(2016,1,1,00,00,00)
y = np.arange(1000000)
delta  = np.random.uniform(-0.00001,0.00001, size = (1000000))
choice = ['a','b','c','d']

producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

for i in range (0,len(y)):
	z = 0.0000001 * y[i] +0.00001 + delta[i]
	sign = random.choice(choice)
	if sign =='a':
		x = x+z
	elif sign == 'b':
		x = x + (z/2)
	elif sign == 'c':
		x = x -z
	else:
		x = x - (z/2)
	ticker = "AAPL"
	company_name = "Apple INC"
	industry = "Technology"
	sector = "Computer Manufacturing"
	exchange = "NASDAQ"
	a = a+datetime.timedelta(seconds = 1)
	#epo = calendar.timegm(a.timetuple())
	seq = ticker+","+company_name+","+industry+","+sector+","+exchange+","+str(x)+","+str(a)
	producer.send("test",seq)#,timestamp_ms = epo)
	
	time.sleep(2)
