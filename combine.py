import pandas as pd
import glob
import datetime as dt
import math

def combine_data():
    # get file names
    csi_path = "csi_data/*.csv"
    light_path = "light_data/*.csv"

    csi_fnames = glob.glob(csi_path)
    light_fnames = glob.glob(light_path)

    # convert light data from long to wide
    # for i in range(len(light_fnames)):
    for i in range(1):

        # read csv
        light_df = pd.read_csv(light_fnames[i],header=0)

        print("Reading", light_fnames[i])
        
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
        pivoted = light_df.pivot(index=1, columns=2, values=3)

        print(pivoted)

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


        # add new columns in new_df

        csi_column_names = {}

        for p in range(256):
            csi_column_names.update({f"csi_channel_{p}":[None] * len(new_df)})
        

        csi_empty_df = pd.DataFrame(
            csi_column_names
        )

        new_df = pd.concat([new_df, csi_empty_df])

        # print(new_df)

        # read CSI data file
        print("Reading", csi_fnames[i])

        csi_df = pd.read_csv(csi_fnames[i])

        for index, row in csi_df.iterrows():
            
            # print(f"index: {index}")

            csi_row_dt = dt.datetime.fromtimestamp(row.iloc[1])
            
            # add 5 mins 5 seconds 7 ms to csi data
            csi_row_dt = csi_row_dt + dt.timedelta(minutes=5, seconds= 5 , milliseconds= 7)

            # round time to nearest millisecond
            # csi_row_dt = csi_row_dt.replace(microsecond=(csi_row_dt.microsecond // 100000) * 100000)
            if (round(csi_row_dt.microsecond,-5) == 1000000):
                csi_row_dt = csi_row_dt.replace(microsecond=0)
                csi_row_dt = csi_row_dt + dt.timedelta(seconds=1)
            else:
                csi_row_dt = csi_row_dt.replace(microsecond=round(csi_row_dt.microsecond,-5))
            
            csi_row_dt_string = csi_row_dt.strftime("%Y-%m-%d %H:%M:%S.%f")

            # print(f"{index}")

            for p in range(246):
                new_df.loc[new_df["time"] == csi_row_dt_string, f"csi_channel_{p}"] = row[p+2]
            


        # export to csv

        filename = "combined_data/" + csi_fnames[i][8:-4] + "_combined.csv"

        new_df.to_csv(filename, index=False)

        print(f"Saved to {filename}")
            

        """
        # checking delay
        for k in range(0,5):
            print(new_df.iloc[k,0])
            light_row_dt = dt.datetime.strptime(new_df.iloc[k,0], "%Y-%m-%d %H:%M:%S.%f")
            csi_row_dt = dt.datetime.fromtimestamp(csi_df.iloc[k,1])
            csi_row_dt = csi_row_dt.replace(microsecond=(csi_row_dt.microsecond // 100000) * 100000)
            print(csi_row_dt)
            print( light_row_dt - csi_row_dt)
        """
            


        # export to csv

        # filename = light_fnames[i][11:-4] + "_wide.csv"

        # new_df.to_csv("temp_wide_light_data/"+filename, index=False)
    
if __name__ == "__main__":
    combine_data()







