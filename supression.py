import sys;
import os
import numpy as np
import math
import time
from dateutil import parser
import holidays
import random

import pandas as pd
import pickle

#sensor streams to procress
sensorStreams = ["co2","humidity","Illuminate","temperature","occ_count","vav", "occ_count_cam1_FW","occ_count_cam1_BW","occ_count_cam2_FW","occ_count_cam2_BW"]

#find danish holidays
dk_holidays =  holidays.Denmark()

#datatypes to procress
datasetTypes = ["original", "filleddata"]


#get seed for ordering the dates for the release
seed = random.randint(0,sys.maxsize)

for dataset in datasetTypes:
    for sensorStream in sensorStreams:
        roomNr = 0
        #read all files in the path
        for file in os.listdir("./Preprocessed Data/"+dataset+"/"+sensorStream+"/"):
                    if not file.endswith(".csv"):
                        continue
                    roomNr += 1
                    #read file
                    stream = pd.read_csv(os.path.join("./Preprocessed Data/"+dataset+"/"+sensorStream+"/", file))
                    stream = stream.infer_objects()

                    #Create the temp time att
                    stream['time'] = pd.to_datetime(stream.time)
                    stream['datetime'] = pd.to_datetime(stream.time)

                    #Create month
                    stream['Month'] = stream.time.dt.month
                    stream['Year'] = stream.time.dt.year


                    #create temp Holiday att
                    stream['Holiday'] = [True if x in dk_holidays else False for x in stream.time.dt.date]

                    #create workday att
                    stream['Workday'] = (stream.time.dt.weekday <= 4) & ~stream['Holiday']

                    #create new time and date att
                    stream['date']  = stream.time.dt.date
                    stream['time']  = stream.time.dt.time


                    #change order of days

                    #find all unique dates in the stream, and create a random ordered list for the new order of the data
                    days = stream['date'].unique()
                    days_index = list(range(len(days)))
                    random.seed(seed)
                    random.shuffle(days_index)

                    #create DayId column
                    stream["DayId"] = None
                    i = 0
                    for d in days_index:
                        #set DayId using the order from days_index
                        day_data = stream[stream['date']==days[d]]
                        stream.loc[day_data.index, 'DayId'] = int(i)
                        i += 1

                    #Set pd index
                    stream.index = stream['DayId'].values

                    #Sort the value for the output
                    stream = stream.sort_values(by=["Year",'Month','DayId','time'])

                    #rename columns for output
                    stream.columns = ['Time', "room_"+ str(roomNr), "datetime", 'Month',"Year","Holiday" ,'Workday', 'Date', 'DayId']

                    #columns to go into the release dataset
                    header = ["Year",'Month','Time','DayId', 'Workday', "room_"+ str(roomNr)]

                    #save file
                    stream.to_csv('./Release data/'+dataset+"/"+sensorStream+'_room_'+str(roomNr)+'.csv', sep=',', index=0, columns=header)





