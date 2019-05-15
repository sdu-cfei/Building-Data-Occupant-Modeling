import sys;
import os
import numpy as np
import math
import time
import pandas as pd

sensorStreams = ["co2","humidity","Illuminate","temperature","occ_count","vav", "occ_count_cam1_FW","occ_count_cam1_BW","occ_count_cam2_FW","occ_count_cam2_BW"]

for sensorStream in sensorStreams:
    for file in os.listdir("./Raw data/"+sensorStream+"/"):
        stream = pd.read_csv(os.path.join("./Raw data/"+sensorStream+"/", file))
        stream.columns = ['time', file.replace(".csv", "")]
        stream.time = pd.to_datetime(stream.time)
        stream = stream.set_index("time")
        stream= stream.sort_index()

        stream = stream.resample('1Min').ffill()
        stream = stream.resample('1Min').bfill()

        header = [file.replace(".csv", "")]
        stream.to_csv('./filleddata/'+sensorStream+"/"+file, columns=header)



