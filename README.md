# FIT3164 Team MDS5 Human Activitiy Recognition Using WiFi CSI and Visible Light Data

This project contains code that can help to set up and collect Wifi CSI using a Raspberry Pi and to set up and collect light data using ESP32.

"csi-extract" provides the python script to run on a Raspberry Pi to extract CSI data.

"light_collection" provides the C code to flash onto ESP32 to collect light data.

We also provide a python script to combine light and CSI data samples into one sample.

Sample combined dataset that we collected during our project can be found under "dataset/2351010_combined_data"

Our experimental models can be found in "model"