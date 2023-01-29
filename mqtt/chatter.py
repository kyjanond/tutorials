#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2020 Ondrej Kyjanek <ondrej.kyjanek@gmail.com>
#
# Contributors:
#    Ondrej Kyjanek - initial implementation

# This shows a simple example of a chat application with mqtt.

import paho.mqtt.client as mqttc
import logging
from datetime import datetime
import sys

# Setup a basic logging for our program
logging.basicConfig(
    format="%(levelname)s:%(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Set username as the first argument or 'noname' in case an argument is missing
USERNAME = "noname" if len(sys.argv)<2 else sys.argv[1]

# Set your broker host and port
HOST = "broker.hivemq.com"
PORT = 1883

# Create topics
BASE_TOPIC = "oky/tutorials/chatter"
MSG_TOPIC_PUB = "{}/{}/message".format(BASE_TOPIC,USERNAME)
MSG_TOPIC_SUB = "{}/+/message".format(BASE_TOPIC)
STATUS_TOPIC_PUB = "{}/{}/status".format(BASE_TOPIC,USERNAME)
STATUS_TOPIC_SUB = "{}/+/status".format(BASE_TOPIC)

# Format and print message as 'time > user: message'
def print_msg(user:str, msg:str):
    date_time = datetime.now().strftime("%H:%M:%S")
    print("{} > {}: {}".format(
        date_time,
        user,
        msg
    ))

# Parse message and return tuple '(user_name, message)'
def parse_msg(msg:mqttc.MQTTMessage):
    try:
        # Payload is utf-8 encoded string so we need to decode it first
        msg_payload = msg.payload.decode("utf-8")
        # Get username from topic 
        user_name = msg.topic.split('/')[-2]
        return(user_name,msg_payload)
    except Exception as e:
        logger.error(e)

# Callback for the MSG_TOPIC_SUB
def on_message(client:mqttc.Client, userdata, msg:mqttc.MQTTMessage):
    user_message = parse_msg(msg)
    if user_message:
        print_msg(*user_message)

# Callback for the STATUS_TOPIC_SUB
def on_user_status(client:mqttc.Client, userdata, msg:mqttc.MQTTMessage):
    user_message = parse_msg(msg)
    if user_message:
        print_msg(*user_message)

# Callback for when MQTT client successfuly subscribes
def on_subscribe(client:mqttc.Client, userdata, mid:str, granted_qos:int, properties):
    logger.info("Subscribed")

# Callback for when MQTT client successfuly disconnects
def on_disconnect(client:mqttc.Client, userdata, rc:int, properties):
    logger.info("Disconnected {}".format(rc))

# Callback for when MQTT client successfuly connects
def on_connect(client:mqttc.Client, userdata, flags, rc, properties):
    # Publish our CONNECT status
    client.publish(
        STATUS_TOPIC_PUB, 
        payload="connected",
        qos=1,
        retain=True
    )
    # Subscribe to MSG_TOPIC_SUB with QoS 2 and STATUS_TOPIC_SUB with QoS 1
    client.subscribe([
        (MSG_TOPIC_SUB,2),
        (STATUS_TOPIC_SUB,1)
    ])
    logger.info("Connected {}".format(rc))

def main():
    # Create an instance of the MQTT client
    client = mqttc.Client(protocol=mqttc.MQTTv5)

    # Assign the callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe

    # Add the message callbecks by topic
    client.message_callback_add(STATUS_TOPIC_SUB,on_user_status)
    client.message_callback_add(MSG_TOPIC_SUB,on_message)
    
    # Set the last will and testament
    client.will_set(
        STATUS_TOPIC_PUB, 
        payload="disconnected",
        qos=1,
        retain=True
    )

    # Connect and start the non-blocking loop
    client.connect(HOST,PORT)
    client.loop_start()

    # Run an infinite loop 
    while True:
        try:
            # Get user input
            message = input()
        except KeyboardInterrupt:
            # Break the loop if CTL+C is pressed 
            logger.info("Ctrl+C pressed")
            break
        # Publish the user input
        client.publish(
            MSG_TOPIC_PUB,
            payload=message,
            qos=2,
        )

    # Publish our disconnect status and try to for successful publish
    client.publish(
        STATUS_TOPIC_PUB,
        "disconnected",
        qos=1,
        retain=True
    ).wait_for_publish(5)
    # Disconnect the client and stop the loop
    client.disconnect()
    client.loop_stop()
    logger.info("Exiting")

if __name__ == "__main__":
    main()
