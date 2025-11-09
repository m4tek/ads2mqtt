import pyads
from ctypes import sizeof
import paho.mqtt.client as mqtt
import logging
import atexit
import signal
import sys
from configlib import find_and_load
import time
import threading

logger = logging.getLogger(__name__)
formatter = logging.Formatter("%(levelname)s: %(message)s")
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.WARN)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)
logger.setLevel(logging.WARN)
logger.propagate = False  # "Overwrite" default handler

config_file = sys.argv[1]

def cleanup():
  logger.info("Exiting...")
  print(handles)
  for handle in handles:
    plc.del_device_notification(handle[0], handle[1])
  plc.close()
  mqttc.disconnect()
  logger.info("I'm done here. CU next time")

def sigHandler(signo, frame):
    sys.exit(0)


# define the MQTT callback which call ADS write_by_name for variables in the message
def mqttcallback(client, userdata, message):
    logger.info(f"Got message: {message.topic} with payload: {message.payload}")
    try:
        ads_var = message.topic.partition("plc/"+plc_host+"/set/")[2]
    except Exception as e:
        logger.warning(f"Failed extracting ADS variable name from topic {message.topic} with error: {e}")
    try:
        plc.write_by_name(ads_var, message.payload)
    except Exception as e:
        logger.warning(f"Failed ADS write of {message.payload} value to variable {ads_var} with error: {e}")


# define the ADS callback which extracts the value of the variable
def adscallback(notification, item):
    data_type = getattr(pyads, config.get('from_ads', item, 'length')) # data type is loaded from the lib based on the assosiated config value
    handle, timestamp, value = plc.parse_notification(notification, data_type)
    # print(item, ": ", value, "of type ", data_type)
    try:
        mqttc.publish(topic="plc/"+plc_host+"/"+item, payload=value)
    except Exception as e:
        logger.warning(f"Publishing failed with error: {e}")

# setting notification here, magic happen in the attrib
def setUpdate(item):
    try:
        attr = pyads.NotificationAttrib(
            length = sizeof(getattr(pyads,config.get('from_ads',item,'length'))), # load from pyads lib PLC variable type as indicated in the config 
            trans_mode = getattr(pyads.constants, config.get('from_ads',item,'trans_mode')),  # load from pyads lib contants object transation type as indicated in the config
            cycle_time = config.get('from_ads', item, 'cycle_time', fallback = config.get('main', 'cycle_time')),  # use cycle time as in the config of fallback to the defaul value from there
            max_delay = config.get('from_ads', item, 'max_delay', fallback = config.get('main', 'max_delay')) # dito
        )
        handles.append(plc.add_device_notification(item, attr, adscallback))
    except Exception as e:
        logger.warning(f"Creating notification for symbol {item} failed with error: {e}")


handles = [] # this is filled for every notification configured 
plc_host = '' # loaded from the config later

if __name__ == "__main__":
    
    # making sure we exist cleaning
    atexit.register(cleanup) 
    signal.signal(signal.SIGTERM, sigHandler)
    signal.signal(signal.SIGINT, sigHandler)

    # load config file
    config = find_and_load(config_file)

    # open MQTT connetions
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.connect(config.get('main', 'mqtt_host'), 1883, 60)
    mqttc.enable_logger()

    # using async MQTT mode, so starting the loop
    mqttc.loop_start()

    # setting the plc host name via ADS ID
    plc_host = config.get('main', 'plc_host')

    # subscribe to MQTT messages directed at the given plc
    try:
        mqttc.message_callback_add("plc/"+plc_host+"/set/#", mqttcallback)
        mqttc.subscribe("plc/"+plc_host+"/set/#")
    except Exception as e:
        logger.warning(f"Failed creating MQTT callback with error: {e}")
  

    # setup connection the host at runtime #1
    plc = pyads.Connection(plc_host+".1.1", config.get('main', 'plc_port'))

    # connection timeout - as indidated in the config
    plc.set_timeout(config.get('main', 'plc_timeout'))
   
    # open plc connection
    plc.open()

    # set notification for every symbol from the config file
    for item in config.get('from_ads'):
        setUpdate(item)
        time.sleep(0.100) # 100ms not to have all time-based notifications comming at once
  
    # everything is async here, but check every 10 sec if we are still talking to the PLC
    ticker = threading.Event()
    while not ticker.wait(10):
        try:
            if not plc.is_open:
                raise Exception("PLC connection down, exiting")
            expectedState = (5, 0)
            if plc.read_state() != expectedState:
                raise Exception("Invalid PLC state: ", plc.read_state())
        except Exception as e:
            logger.error(f"Heartbeat test failed with error: {e}")
            sys.exit(1)
