#!/usr/bin/env python3
# -*- coding: iso-8859-1 -*-

__author__ = "Kyle Gordon, Heikki Hannikainen"
__copyright__ = "Copyright (C) Kyle Gordon, Heikki Hannikainen"

import os
import logging
import signal
import socket
import time
import sys

import paho.mqtt.client as mqtt
import configparser
import setproctitle

from Adafruit_BME280 import *

# Read the config file
config = configparser.RawConfigParser()
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
mqttc = mqtt.Client(client_id=client_id)

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

connected = False

def on_mqtt_connect(client, userdata, flags, rc):
    logging.debug("on_connect RC: " + str(rc))
    logging.info("mqtt: Connected to %s:%s", MQTT_HOST, MQTT_PORT)
    mqttc.publish(PRESENCETOPIC, "1", retain=True)

def on_mqtt_disconnect(self, client, userdata, rc=0):
    logging.info("on_mqtt_disconnect")
    #global connected
    #connected = False
    if rc == 0:
        logging.info("mqtt: Clean disconnection")
    else:
        logging.info("mqtt: Unexpected disconnection! Reconnecting in 5 seconds")
        logging.debug("Result code: %s", rc)
        time.sleep(5)


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

def mqtt_setup():
    mqttc.loop_start()

def check_connection():
    # mqttc.is_connected() returns False at all times for some reason...
    global connected
    if not connected:
        logging.info("Connecting to %s:%s", MQTT_HOST, MQTT_PORT)
        try:
            mqttc.on_connect = on_mqtt_connect
            mqttc.on_disconnect = on_mqtt_disconnect
            mqttc.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            logging.info("connect() returned, connected to mqtt at %s:%s", MQTT_HOST, MQTT_PORT)
            connected = True
        except OSError as ex:
            logging.warning("Failed to connect (will try again later): %s", ex)

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
        try:
            sensor = BME280(standby=BME280_STANDBY_1000,
                t_mode=BME280_OSAMPLE_8, p_mode=BME280_OSAMPLE_8, h_mode=BME280_OSAMPLE_8,
                address=0x76)
        except Exception, e:
            logging.error("Sensor opening exception: %r" % e)
            time.sleep(10)
            continue

        logging.info("opened sensor")
        buf = ''
        going = True
        last_send = time.monotonic()
        
        temp_store = SampleStore()
        press_store = SampleStore()
        hum_store = SampleStore()
        
        while going:
            check_connection()
            try:
                temperature = sensor.read_temperature()
                pascals = sensor.read_pressure()
                pressure = pascals / 100.0
                humidity = sensor.read_humidity()
            except Exception as e:
                logging.error("Sensor reading exception: %r" % e)
                going = False
                break
            
            logging.debug("read: temp %.1f C, pressure %.1f mbar, humidity %.1f %%", temperature, pressure, humidity)
            
            temp_store.add(temperature)
            press_store.add(pressure)
            hum_store.add(humidity)
            
            now = time.monotonic()
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

mqtt_setup()

# Try to start the main loop
try:
    main_loop()
except KeyboardInterrupt:
    logging.info("Interrupted by keypress")
    sys.exit(0)

