import pandas as pd
import glob
import datetime as dt
import math

def split_data():
    # get file names
    csi_path = "new_csi_data/*.csv"
    light_path = "new_light_data/*.csv"

    csi_fnames = glob.glob(csi_path)
    light_fnames = glob.glob(light_path)

    # break into batches of 80 rows
    # assuming no. of files in csi_path == light_path
    for i in range(len(light_fnames)):
        light_df = pd.read_csv(light_fnames[i],header=0)
        csi_df = pd.read_csv(csi_fnames[i],header=0)

        # widen light data
        # get min and max time
        min_time = dt.datetime.strptime(light_df.iloc[0][0],"%Y-%m-%d %H:%M:%S")
        max_time = dt.datetime.strptime(light_df.iloc[-1][0],"%Y-%m-%d %H:%M:%S")

        # removing one second to account for delay
        start_time = min_time - dt.timedelta(seconds=1)

        # make buckets of 100 ms in csv
        time_buckets = []

        while start_time < max_time:

            time_buckets.append(start_time.strftime("%Y-%m-%d %H:%M:%S.%f"))

            # new_df[j]["time"] = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")

            start_time = start_time + dt.timedelta(milliseconds=100)

        # make new dataframe with buckets and empty data
        new_df = pd.DataFrame({
            "time": time_buckets,
            "1": [None] * len(time_buckets),
            "2": [None] * len(time_buckets),
            "3": [None] * len(time_buckets),
            "4": [None] * len(time_buckets),
            "5": [None] * len(time_buckets),
            "6": [None] * len(time_buckets),
            "7": [None] * len(time_buckets),
            "8": [None] * len(time_buckets),
            "9": [None] * len(time_buckets),
        })

       
        # convert from long to wide using df.pivot()
        pivoted = light_df.pivot(index="msg_published_datetime", columns="sensor_id", values="sensor_val")

        # put pivoted data into new dataframe
        for index, row in pivoted.iterrows():
            
            # round time to know the bucket
            rounded_time = dt.datetime.strptime(str(row.name), "%Y-%m-%d %H:%M:%S.%f")

            if (round(rounded_time.microsecond,-5) == 1000000):
                rounded_time = rounded_time.replace(microsecond=0)
                rounded_time = rounded_time + dt.timedelta(seconds=1)
            else:
                rounded_time = rounded_time.replace(microsecond=round(rounded_time.microsecond,-5))

            # get sensor number
            for l in range(len(row.values)):
                if pd.notnull(row.values[l]):
                    sensor_number = l

            new_df.loc[new_df['time'] == rounded_time.strftime("%Y-%m-%d %H:%M:%S.%f"), str(sensor_number)] = row.values[sensor_number]

        #TODO split new_df to groups of 80

        #TODO split csi_df to groups of 80

        #TODO combine the groups of 80 from csi and light

        #TODO save group of 80 as new file 

    
if __name__ == "__main__":
    split_data()







