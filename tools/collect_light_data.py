import paho.mqtt.client as mqtt
import csv
import time
import json
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
        default=30,
        help='time interval for activity data collection')
    args = parser.parse_args()

    print("creating new client instance")
    client = mqtt.Client("COLLECT_LIGHT_DATA")
    client.on_message = on_message # specify callback function
    print(f"connecting to {broker}...", end="")
    client.connect(broker, 1883, 60) # Connect to the broker
    print(f"successfully connected to {broker}. Subscribing to topics...")
    client.subscribe("/topic/mds5_light")

    # Start the network loop
    print("starting network loop...")
    client.loop_start()

    # Store all received messages for the next collection_time seconds
    start_time = time.time()
    # get start date and time as a string
    file_suffix = time.strftime("%Y%m%d_%H%M%S", time.localtime(start_time))
    messages = []
    while (time.time() - start_time) < args.time_interval:
        pass

    # print("stopping network loop...", end='')
    time.sleep(5) # sleep for 5 seconds to make sure all messages are received

    # Stop the network loop
    client.loop_stop()
    print("stopped network loop. Writing data to file")

    # Write the received messages to a CSV file
    start_dt = datetime.fromtimestamp(start_time)
    with open(f'{args.activity}_{file_suffix}.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        for message in messages:
            if (datetime.strptime(message[1], "%Y-%m-%d %H:%M:%S.%f") - start_dt).total_seconds() < args.time_interval:
                writer.writerow(message)
    print("data written to file. Disconnecting from broker...", end='')

    # Disconnect from the broker
    client.disconnect()
    print("Program complete")
