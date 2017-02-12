#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-

__author__ = "Kyle Gordon, Heikki Hannikainen"
__copyright__ = "Copyright (C) Kyle Gordon, Heikki Hannikainen"

import os
import logging
import signal
import socket
import time
import sys

import mosquitto
import ConfigParser
import setproctitle

from Adafruit_BME280 import *

# Read the config file
config = ConfigParser.RawConfigParser()
config.read("mqtt-bme280.cfg")

# Use ConfigParser to pick out the settings
DEBUG = config.getboolean("global", "debug")
LOGFILE = config.get("global", "logfile")
MQTT_HOST = config.get("global", "mqtt_host")
MQTT_PORT = config.getint("global", "mqtt_port")
MQTT_SUBTOPIC = config.get("global", "MQTT_SUBTOPIC")
MQTT_TOPIC = "raw/" + socket.getfqdn() + MQTT_SUBTOPIC

SENDINTERVAL = config.getint("global", "sendinterval")
DEVICESFILE = config.get("global", "devicesfile")

APPNAME = "mqtt-bme280"
PRESENCETOPIC = "clients/" + socket.getfqdn() + "/" + APPNAME + "/state"
setproctitle.setproctitle(APPNAME)
client_id = APPNAME + "_%d" % os.getpid()
mqttc = mosquitto.Mosquitto(client_id)

LOGFORMAT = '%(asctime)-15s %(message)s'

if DEBUG:
    logging.basicConfig(filename=LOGFILE,
                        level=logging.DEBUG,
                        format=LOGFORMAT)
else:
    logging.basicConfig(filename=LOGFILE,
                        level=logging.INFO,
                        format=LOGFORMAT)

logging.info("Starting " + APPNAME)
logging.info("INFO MODE")
logging.debug("DEBUG MODE")

# All the MQTT callbacks start here

def on_publish(mosq, obj, mid):
    """
    What to do when a message is published
    """
    logging.debug("MID " + str(mid) + " published.")


def on_subscribe(mosq, obj, mid, qos_list):
    """
    What to do in the event of subscribing to a topic"
    """
    logging.debug("Subscribe with mid " + str(mid) + " received.")


def on_unsubscribe(mosq, obj, mid):
    """
    What to do in the event of unsubscribing from a topic
    """
    logging.debug("Unsubscribe with mid " + str(mid) + " received.")


def on_connect(mosq, obj, result_code):
    """
    Handle connections (or failures) to the broker.
    This is called after the client has received a CONNACK message
    from the broker in response to calling connect().
    The parameter rc is an integer giving the return code:

    0: Success
    1: Refused – unacceptable protocol version
    2: Refused – identifier rejected
    3: Refused – server unavailable
    4: Refused – bad user name or password (MQTT v3.1 broker only)
    5: Refused – not authorised (MQTT v3.1 broker only)
    """
    logging.debug("on_connect RC: " + str(result_code))
    if result_code == 0:
        logging.info("Connected to %s:%s", MQTT_HOST, MQTT_PORT)
        # Publish retained LWT as per
        # http://stackoverflow.com/q/97694
        # See also the will_set function in connect() below
        mqttc.publish(PRESENCETOPIC, "1", retain=True)
        process_connection()
    elif result_code == 1:
        logging.info("Connection refused - unacceptable protocol version")
        cleanup()
    elif result_code == 2:
        logging.info("Connection refused - identifier rejected")
        cleanup()
    elif result_code == 3:
        logging.info("Connection refused - server unavailable")
        logging.info("Retrying in 30 seconds")
        time.sleep(30)
    elif result_code == 4:
        logging.info("Connection refused - bad user name or password")
        cleanup()
    elif result_code == 5:
        logging.info("Connection refused - not authorised")
        cleanup()
    else:
        logging.warning("Something went wrong. RC:" + str(result_code))
        cleanup()


def on_disconnect(mosq, obj, result_code):
    """
    Handle disconnections from the broker
    """
    if result_code == 0:
        logging.info("Clean disconnection")
        sys.exit(0)
    else:
        logging.info("Unexpected disconnection! Reconnecting in 5 seconds")
        logging.debug("Result code: %s", result_code)
        time.sleep(5)


def on_message(mosq, obj, msg):
    """
    What to do when the client recieves a message from the broker
    """
    logging.debug("Received: " + msg.payload +
                  " received on topic " + msg.topic +
                  " with QoS " + str(msg.qos))
    process_message(msg)


def on_log(mosq, obj, level, string):
    """
    What to do with debug log output from the MQTT library
    """
    logging.debug(string)

# End of MQTT callbacks

am_stopping = False

def cleanup(signum, frame):
    """
    Signal handler to ensure we disconnect cleanly
    in the event of a SIGTERM or SIGINT.
    """
    logging.info("Disconnecting from broker")
    # Publish a retained message to state that this client is offline
    mqttc.publish(PRESENCETOPIC, "0", retain=True)
    logging.info("Disconnecting")
    mqttc.disconnect()
    time.sleep(1)
    logging.info("Stopping loop")
    mqttc.loop_stop(True)
    logging.info("Exiting on signal %d", signum)
    sys.exit(signum)

def connect():
    """
    Connect to the broker, define the callbacks, and subscribe
    This will also set the Last Will and Testament (LWT)
    The LWT will be published in the event of an unclean or
    unexpected disconnection.
    """
    logging.debug("Connecting to %s:%s", MQTT_HOST, MQTT_PORT)
    # Set the Last Will and Testament (LWT) *before* connecting
    mqttc.will_set(PRESENCETOPIC, "0", qos=0, retain=True)
    result = mqttc.connect(MQTT_HOST, MQTT_PORT, 60, True)
    if result != 0:
        logging.info("Connection failed with error code %s. Retrying", result)
        time.sleep(10)
        connect()

    # Define the callbacks
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe
    mqttc.on_unsubscribe = on_unsubscribe
    mqttc.on_message = on_message
    if DEBUG:
        mqttc.on_log = on_log

    mqttc.loop_start()

def process_connection():
    """
    What to do when a new connection is established
    """
    logging.debug("Processing connection")


def process_message(mosq, obj, msg):
    """
    What to do with the message that's arrived
    """
    logging.debug("Received: %s", msg.topic)


def find_in_sublists(lst, value):
    for sub_i, sublist in enumerate(lst):
        try:
            return (sub_i, sublist.index(value))
        except ValueError:
            pass

    raise ValueError("%s is not in lists" % value)

class DevicesList():
    """
    Read the list of devices, and expand to include publishing state and current value
    """
    import csv
    datafile = open(DEVICESFILE, "r")
    datareader = csv.reader(datafile)
    data = {}
    for row in datareader:
        input = int(row[0])
        data[input] = row[1:]
        
    logging.info("read devices: %r" % data)

class SampleStore:
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.samples = 0
        self.sum = 0.0
        self.max = None
        self.min = None
    
    def add(self, value):
        self.samples += 1
        self.sum += value
        
        if self.max == None or value > self.max:
            self.max = value
        
        if self.min == None or value < self.min:
            self.min = value
    
    def average(self):
        if self.samples < 1:
            return None
            
        return self.sum / self.samples
        
    def deviation(self):
        return abs(self.max - self.min)

def main_loop():
    """
    The main loop in which we stay connected to the broker
    """
    while True:
        sensor = BME280(standby=BME280_STANDBY_1000,
            t_mode=BME280_OSAMPLE_8, p_mode=BME280_OSAMPLE_8, h_mode=BME280_OSAMPLE_8,
            address=0x76)

        logging.info("opened sensor")
        buf = ''
        going = True
        last_send = time.time()
        
        temp_store = SampleStore()
        press_store = SampleStore()
        hum_store = SampleStore()
        
        while going:
            try:
                temperature = sensor.read_temperature()
                pascals = sensor.read_pressure()
                pressure = pascals / 100.0
                humidity = sensor.read_humidity()
            except Exception, e:
                logging.error("Sensor reading exception: %r" % e)
                going = False
                break
            
            logging.debug("read: temp %.1f C, pressure %.1f mbar, humidity %.1f %%", temperature, pressure, humidity)
            
            temp_store.add(temperature)
            press_store.add(pressure)
            hum_store.add(humidity)
            
            now = time.time()
            if now - last_send >= SENDINTERVAL or now < last_send:
                last_send = now
                
                input = 67
                c = DevicesList.data.get(input)
                name = c[0]
                features = c[1:]
                
                logging.debug("sending, features: %r", features)
                
                for k in features:
                    if k == 'instant':
                        mqttc.publish(MQTT_TOPIC + name + "/temp", round(temperature, 2))
                        mqttc.publish(MQTT_TOPIC + name + "/press", round(pressure, 2))
                        mqttc.publish(MQTT_TOPIC + name + "/hum", round(humidity, 1))
                    if k == 'avg':
                        mqttc.publish(MQTT_TOPIC + name + "/temp_avg", round(temp_store.average(), 2))
                        mqttc.publish(MQTT_TOPIC + name + "/press_avg", round(press_store.average(), 2))
                        mqttc.publish(MQTT_TOPIC + name + "/hum_avg", round(hum_store.average(), 1))
                    if k == 'min':
                        mqttc.publish(MQTT_TOPIC + name + "/temp_min", round(temp_store.min, 2))
                        mqttc.publish(MQTT_TOPIC + name + "/press_min", round(press_store.min, 2))
                        mqttc.publish(MQTT_TOPIC + name + "/hum_min", round(hum_store.min, 1))
                    if k == 'max':
                        mqttc.publish(MQTT_TOPIC + name + "/temp_max", round(temp_store.max, 2))
                        mqttc.publish(MQTT_TOPIC + name + "/press_max", round(press_store.max, 2))
                        mqttc.publish(MQTT_TOPIC + name + "/hum_max", round(hum_store.max, 1))
                    if k == 'dev':
                        mqttc.publish(MQTT_TOPIC + name + "/temp_dev", round(temp_store.deviation(), 2))
                        mqttc.publish(MQTT_TOPIC + name + "/press_dev", round(press_store.deviation(), 2))
                        mqttc.publish(MQTT_TOPIC + name + "/hum_dev", round(hum_store.deviation(), 1))
                
                temp_store.reset()
                press_store.reset()
                hum_store.reset()
                
            time.sleep(5)
            
        time.sleep(4)
        
    
# Use the signal module to handle signals
signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT, cleanup)

# Connect to the broker and enter the main loop
connect()

# Try to start the main loop
try:
    main_loop()
except KeyboardInterrupt:
    logging.info("Interrupted by keypress")
    sys.exit(0)

