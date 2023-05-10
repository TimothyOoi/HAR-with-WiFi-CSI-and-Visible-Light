import paho.mqtt.client as mqtt
import csv
import time
import glob
import argparse
from datetime import datetime

broker = "test.mosquitto.org"

# Callback function that will be called when a new message is received
def on_message(client, userdata, message):
    global messages
    message_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    published_dt, sensor_id, sensor_val = message.payload.decode().split(',')
    messages.append([message_time, published_dt, sensor_id, sensor_val])
    print(f"Received: {messages[-1]}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-a', 
        '--activity', 
        choices=['empty', 'stand', 'sit', 'walk'],
        type=str,
        help='Subject\'s activity label')
    parser.add_argument(
        '-t', 
        '--time_interval', 
        type=int, 
        default=40,
        help='time interval for activity data collection')
    args = parser.parse_args()

    print("creating new client instance")
    client = mqtt.Client("COLLECT_LIGHT_DATA")
    client.on_message = on_message # specify callback function
    print(f"connecting to {broker}...", end="")
    client.connect(broker, 1883, 60) # Connect to the broker
    print(f"successfully connected to {broker}. Subscribing to topics...")
    client.subscribe("/mds5/light")

    # Start the network loop
    print("starting network loop...")
    client.loop_start()

    # Store all received messages for the next collection_time seconds
    start_time = time.time()
    # get start date and time as a string
    # file_suffix = time.strftime("%Y%m%d_%H%M%S", time.localtime(start_time))
    messages = []
    while (time.time() - start_time) < args.time_interval:
        pass

    # print("stopping network loop...", end='')
    time.sleep(5) # sleep for 5 seconds to make sure all messages are received

    # Stop the network loop
    client.loop_stop()
    print("stopped network loop. ", end="")

    # Write the received messages to a CSV file
    start_dt = datetime.fromtimestamp(start_time)
    activity_idx = len(glob.glob(f"D:/fyp_mds5/tools/{args.activity}*")) + 1
    fname = f"{args.activity}_{activity_idx}.csv"
    print(f"Writing data to file {fname}")
    with open(fname, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['msg_received_datetime', 'msg_published_datetime', 'sensor_id', 'sensor_val'])
        for message in messages:
            if (datetime.strptime(message[1], "%Y-%m-%d %H:%M:%S.%f") - start_dt).total_seconds() < args.time_interval:
                writer.writerow(message)
    print("data written to file. Disconnecting from broker...", end='')

    # Disconnect from the broker
    client.disconnect()
    print("Program complete")
