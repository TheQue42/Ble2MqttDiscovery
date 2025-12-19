#!/usr/bin/env -S python3 -u

"""Scan for iBeacons from Home Assistant Companion App.

The bluetooth code is based on the code by Koen Vervloesem found at:

http://koen.vervloesem.eu/blog/decoding-bluetooth-low-energy-advertisements-with-python-bleak-and-construct/

TQ-TODO: LOCALTIME, NOT UTC:
TQ-TODO: Check impact of using Message Expiry Interval?
TQ-TODO: Republish/Subscribe to Config part?

TQ_TODO: Log-level bugs, mismatch between CLI -v and .json config

"""

import asyncio, re, json, signal, sys, argparse,socket
import ssl
from pathlib import Path
import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe
import paho.mqtt.publish as publish
import atexit

from uuid import UUID
from time import sleep,time
from datetime import datetime,timezone,timedelta

from construct import Array, Byte, Const, Int8sl, Int16ub, Struct
from construct.core import ConstError

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData

ibeacon_format = Struct(
    "type_length" / Const(b"\x02\x15"),
    "uuid" / Array(16, Byte),
    "major" / Int16ub,
    "minor" / Int16ub,
    "power" / Int8sl,
)

ScriptVersion="0.3.4"
LastPublishTime = None
Devices = None
mqttClient = mqtt.Client
LogFileHandle = None # Not Impl
Cfg = { "log_level" : 0 }  # Before config is read, so logPrint works

def validateConfiguration():
    global Cfg, Devices

    Mandatory_Parameters = [ "mqtt_host", "uuids" ]
    for param in Mandatory_Parameters:
        if not param in Cfg:
            logPrint(0,
            f"Missing config parameter: {param} in json configuration file")
            logPrint(0, f"Dump: {json.dumps(Cfg)}")
            return -1

    if not isinstance(Cfg["uuids"], dict):
        logPrint(0, f"Error: The key [uuids] must be a dict()!")
        logPrint(0, f'Example: {{ "uuid" {"name": "Friendly_Name"} }}')
        return -1

    OptionalParams = {
        "log_level" : 0,
        "mqtt_port" : 1883,
        "mqtt_user" : "",
        "mqtt_pass" : "",
        "mqtt_tls" : False,
        "mqtt_keepalive" : 180,
        "awayTimeout" : 60,
        "publishInterval" : 60,
        "initialScanDelay" : 30,
        "scanDelay" : 15,
        "reduceTimers" : 1,
        "ca_cert": "/etc/ssl/certs/ca-certificates.crt"
    }

    for opt in OptionalParams:
        if not opt in Cfg:
            Cfg[opt] = OptionalParams[opt]
        logPrint(3, f"Optional value {opt} set to {Cfg[opt]}")

    Cfg["BaseDevTrackTopic"] = "homeassistant/device_tracker/"
    Cfg["NodeName"] = socket.gethostname().split('.')[0]
    Cfg["BleTrackerTopic"] = Cfg["BaseDevTrackTopic"] + "BleTracker/" + Cfg["NodeName"] + "/state"
    Cfg["BleTrackerSyncTopic"] = Cfg["BaseDevTrackTopic"] + "BleTracker/sync"

    Devices = Cfg["uuids"]  # Quick access
    Cfg["awayTimeout"] = Cfg["publishInterval"] / Cfg["reduceTimers"]
    Cfg["publishInterval"] = Cfg["publishInterval"] / Cfg["reduceTimers"]

def readConfig(fileName: str) -> str:

    if len(fileName) == 0 or not Path(fileName).is_file():
        logPrint(0, "Not a valid filename", fileName)
        return ""

    if isinstance(fileName, str) and not Path(fileName).is_file():
       logPrint(0, f"Cant find file: {fileName}")
       return ""

    with open(fileName, encoding="utf-8") as fHandle:
        fileContent = fHandle.read()

    logPrint(2, f"Read {len(fileContent)} bytes from {fileName}")
    if len(fileContent) > 0 :
        return fileContent
    else:
        return ""

def lastPublishTime(lastTime : datetime):
    """
    Should ensure we dont publish too often.

    ### TQ-TODO: Need to keep track of publish-time per device

    """
    now = datetime.now()
    secondsSinceLast = (now - lastTime).total_seconds()
    lastTime = now
    return round(secondsSinceLast, 2)


def logPrint(level, *args, **kwargs):
    """Centralize log-output, until we get a real logging API"""
    global Cfg

    LogLevel = Cfg["log_level"]
    if LogFileHandle is not None and level <= LogLevel:
        print(*args, file=LogFileHandle, **kwargs)
    else:
        if level <= LogLevel and level > 2:
            print(*args, file=sys.stderr, **kwargs)
        elif level <= LogLevel:
            print(*args, file=sys.stdout, **kwargs)

def PublishDeviceAvailability(forcePublish: bool = False):
    """
        General idea is to publish at least {Cfg["publishInterval"]} seconds
        but "not_home" publications are more seldom(x3-isch)

        After {Cfg["awayTimeout"]} sec, mobile is published as "not_home"

    """
    global Cfg

    logPrint(3, f'\nPublishLoop: DeviceCount: {len(Cfg["uuids"])}')
    for devItem in Cfg["uuids"].values():
        logPrint(3, f'- Name: {devItem["myId"]:<25} - [{devItem["state"]:^8}] @ {devItem["presence"]["location"]}')
    else:
        logPrint(3,"")
    minPublishInterval = Cfg["publishInterval"]
    for devItem in Cfg["uuids"].values():
        last_seen = devItem["last_seen"]
        last_published = devItem["last_published"]
        now = datetime.now()
        deviceAway = ( now - last_seen > timedelta(seconds=Cfg["awayTimeout"]) )

        devName = devItem["myId"]
        logPrint(4, f"\nPublish-DEBUG: {json.dumps(devItem, indent=2, default=str)}\n")

        # Is the device managed by us?
        currentOwner = devItem["presence"]["location"]
        takeOwnerShip = False
        if currentOwner != Cfg["NodeName"] and currentOwner is not None:

            if deviceAway :
                logPrint(3, f'[{devName}] managed by [{currentOwner}], and we havent seen it. Lets not touch.')
                return

            remoteRssi = devItem["presence"]["rssi"]
            ourRssi = devItem["rssi"]
            if remoteRssi < ourRssi:
                logPrint(0, f'TAKING OWNERSHIP of [{devName}], Rssi: {ourRssi} > {remoteRssi}, Old Master: {currentOwner}')
                takeOwnerShip = True
            else:
                #
                # Its not "our" device, but has the owner updated the presence topic in a while?
                #
                lastUpdate = datetime.fromtimestamp(devItem["presence"]["lastUpdate"])
                logPrint(2, f'[{devName}] @ {currentOwner} - Rssi: {remoteRssi} Our: {ourRssi}: Last Update: {lastUpdate} ')
                if ( now - lastUpdate ) > timedelta(seconds=minPublishInterval) :
                    logPrint(0, f'IMPORTANT: Missing update from [{devName}] @ {currentOwner}, FORCING TakeOver')
                    takeOwnerShip = True
                else:
                    return
        elif currentOwner is None and not deviceAway:
            logPrint(0, f'TAKING OWNERSHIP of [{devName}], No Master.')
            takeOwnerShip = True


        if deviceAway:
            if devItem["state"] == "home":
                logPrint(0, f'\n{devName} ==> Away! LastSeen: [{last_seen}]')
                devItem["state"] = "not_home"
                timeToPublish = True
            else:
                timeToPublish = (now - last_published > timedelta(seconds=minPublishInterval*4))
        else:
            if devItem["state"] == "not_home":
                logPrint(0, f'\n{devName} ==> Home LastPub:({last_published})')
                devItem["state"] = "home"
            timeToPublish = (now - last_published > timedelta(seconds=minPublishInterval))

        logPrint(3, f'CHECK: {devName}, deviceAway:{deviceAway}, Own:{currentOwner}, TTP: {timeToPublish}, takeOw: {takeOwnerShip}, \nPresence:{devItem["presence"]}\n')

        # Publish DeviceTracker State?
        rssiIfHome = "" if deviceAway else ":" + f'{devItem["rssi"]}'
        if timeToPublish or takeOwnerShip or (forcePublish and not deviceAway):
            fp = "Forced" if forcePublish else ""

            logPrint(1, f'{fp}Publish - [{devName}] - State: [{devItem["state"]}{rssiIfHome}] - dSec: {lastPublishTime(last_published)}')
            devItem["last_published"] = now
            publishDevice(devItem, takeOwnerShip)
        elif forcePublish:  # We're forcing, but device is away
            logPrint(1, f'(Forced)Skip:[{devName}] - State: [{devItem["state"]}{rssiIfHome}], LastPub: {(now-last_published).total_seconds()}')
        else:
            logPrint(2, f'Skip:[{devName}] - State: [{devItem["state"]}{rssiIfHome}], LastPub: {round((now-last_published).total_seconds(),0)}')
        # Publish BleTracker Presence Info
        #

def publishDevice(devItem : dict, takeOwnerShip : bool):
    """
        Publish a single client state and presence info
    """
    global Cfg, mqttClient

    props = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
    props.MessageExpiryInterval = int(Cfg["publishInterval"] * 3)

    deviceTopic = Cfg["BaseDevTrackTopic"] + devItem["myId"]
    devState = devItem["state"]

    # Always publish away/home state
    mqttClient.publish(deviceTopic + "/state", devState, qos=1, retain=True, properties=props)

    mqttPayload = { "location" : None }
    if devState == "home":
        mqttPayload["location"] = Cfg["NodeName"]
        mqttPayload["rssi"] = devItem["rssi"]
        mqttPayload["lastUpdate"] = datetime.now().timestamp()
        mqttPayload["nodeVersion"] = ScriptVersion
        lastSeenSeconds = round( (datetime.now() - devItem["last_seen"] ).total_seconds(), 0 )
        mqttPayload["last_seen"] = lastSeenSeconds
        devItem["presence"] = mqttPayload

    attributes = dict()
    attributes["rssi"] = devItem["rssi"]
    attributes["owner"] = mqttPayload["location"]
    attributes["nodeVersion"] = ScriptVersion
    mqttClient.publish(deviceTopic + "/attrs", json.dumps(attributes), qos=1, retain=True, properties=props)
    mqttClient.publish(deviceTopic + "/presence", json.dumps(mqttPayload), qos=1, retain=True, properties=props)


def mqtt_on_message(client, userdata, msg):
    """
    Handle messages received on subscriptions, which *should* only be
    messages published by other instances than this system.
    """
    global Cfg
    deviceName = msg.topic.replace(Cfg["BaseDevTrackTopic"], "").replace("/presence", "")
    try:
        jsonData = json.loads(msg.payload.decode())
    except Exception as e:
        logPrint(0, f'Invalid PUBLISH data: \"{msg.payload.decode()}\", Err: {e}')
        ### TQ-TODO: More Error Handling/Debug
        return
    if "location" in jsonData.keys():
        if jsonData["location"] == Cfg["NodeName"]:
            logPrint(3, f'Own publish. {deviceName}:{jsonData["location"]}')
        else:
            devItem = next((value for value in Cfg["uuids"].values() if value["myId"] == deviceName), None)
            if devItem is None:
                logPrint(0,f'Unable to find device for topic: {msg.topic}')
                logPrint(0,f'DeviceName(?): [{deviceName}], json: {jsonData}')
                logPrint(2,f'Device-list: [{Cfg["uuids"]}]')
            else:
                if jsonData["location"] is None:
                    # Someone reported device away,
                    # and we've got eyes on the device
                    if devItem["state"] == "home":
                        logPrint(0, f'\nBetter data: Update! {deviceName}:{devItem["state"]}, Json:{jsonData}, Our:{devItem["rssi"]}')
                        publishDevice(devItem, takeOwnerShip = True)
                else:
                    if jsonData["location"] == devItem["presence"]["location"]:
                        logPrint(2, f'Same master for ({deviceName}): {jsonData["location"]}/RSSI:{jsonData["rssi"]}. Our: {devItem["rssi"]}')
                    else:
                        logPrint(0, f'\nNew master for ({deviceName}): [{jsonData["location"]}] / RSSI: {jsonData["rssi"]}')
                    devItem["presence"] = jsonData

    elif msg.topic == Cfg["BleTrackerSyncTopic"]:
        logPrint(0, f'Startup sync requested by: {jsonData["node"]}')
        PublishDeviceAvailability(forcePublish = True)
    else:
        logPrint(0, f"Unexpected `{jsonData}` in message regarding `{deviceName}` ")



def device_found(device: BLEDevice, advData: AdvertisementData):
    """

    Decode iBeacon, and update internal structures until publish.

    """
    global Cfg
    DeviceList = Cfg["uuids"]
    try:
        apple_data = advData.manufacturer_data[0x004C]
        ibeacon = ibeacon_format.parse(apple_data)
        uuid = UUID(bytes=bytes(ibeacon.uuid))
        uuidAsString = str(uuid)
        now = datetime.now()
        if uuidAsString in DeviceList.keys() :
            DeviceList[uuidAsString]["last_seen"] = now
            DeviceList[uuidAsString]["rssi"] = advData.rssi
            logPrint(3, f'Beacon: {DeviceList[uuidAsString]["myId"]}, Rssi: {advData.rssi}')

    except KeyError:
        # Apple company ID (0x004c) not found
        if advData.local_name is not None:
            name = advData.local_name
            mac = f"Mac: {device.address}"
            logPrint(4, f"NonBLE   : {name} / {mac} / ({advData.rssi}) dBm")
    except ConstError:
        # No iBeacon (type 0x02 and length 0x15)
        if advData.local_name is not None:
            name = advData.local_name
            mac = f"Mac: {device.address}"
            logPrint(4, f"ConstErr : {name} / {mac} / ({advData.rssi}) dBm")
        pass


def mqtt_on_connect(client, userdata, flags, rc, properties):
    """
    Once we're connected, initialize datastructure for Devices,
    and publish the /config topic for each monitored device.

    """
    global Cfg
    if rc == 0:
        logPrint(0, "Connected to MQTT Broker!")
        publishMqttDeviceConfig()
    else:
        logPrint(0, f"Failed to connect, return code {rc}\n")
        exit(1)




def timeToTerminate():
    """
    Before terminating on CTRL+C and similar
    """
    logPrint(0,f'\nExiting. Cleaning up...DeviceCount: {len(Cfg["uuids"])}')
    for devItem in Cfg["uuids"].values():
        devId = devItem["myId"]
        logPrint(4, f"\nPublishDebug: {json.dumps(devItem, indent=2, default=str)}\n")

        # Is the device managed by us?
        currentOwner = devItem["presence"]["location"]
        if currentOwner == Cfg["NodeName"] :
          deviceTopic = Cfg["BaseDevTrackTopic"] + devItem["myId"]
          print(f'Publish - [{devId}] - Shutting Down')

          #logPrint(1, f'Zero out retained value for: {deviceTopic + "/state"}')
          #mqttClient.publish(deviceTopic + "/state", "", qos=1, retain=True)
          #mqttClient.publish(deviceTopic + "/presence", "", qos=1, retain=True)

          logPrint(1, f'Reset Topics for device: [{devId}]')
          mqttClient.publish(deviceTopic + "/state", "not_home", qos=1, retain=True)
          mqttClient.publish(deviceTopic + "/presence", '{ "location" : null }', qos=1, retain=True)
        else:
            logPrint(1, f'Skipping dev: [{devId}], Owner: {currentOwner}')

    sys.exit(0)

def handle_signal(signum, frame):
    """

    Signal Handler

    """
    global Cfg
    Devices = Cfg["uuids"]

    if signum in (signal.SIGTERM, signal.SIGINT):
        logPrint(0, f"\nReceived signal {signum}, time to die")
    elif signum == signal.SIGHUP:
        logPrint(0, "\nReceived SIGHUP, dumping state!")
        for device in Devices.values():
            name = device["name"]
            lastSeenSeconds = round((datetime.now() - device["last_seen"] ).total_seconds(),0)
            if lastSeenSeconds > 86400:
                lastSeenSeconds = "Never"
            logPrint(2, f'Found: {name}, LastSeen: {lastSeenSeconds}s. Dump: \n{json.dumps(device, indent=True, default=str)}\n')
        return
    else:
        logPrint(0, f"\nReceived UNEXPECTED signal {signum}!")

    timeToTerminate()

def populateDevDict():

    global Cfg
    neverTime = datetime(1970, 1, 1)

    for devUuid, devItem in Cfg["uuids"].items():
        devItem["myId"] = devItem["name"].replace(" ", "_")
        devItem["state"] = "not_home"
        devItem["last_seen"] = neverTime
        devItem["last_published"] = neverTime
        devItem["rssi"] = -1000
        devItem["presence"] = { "location" : None }
        logPrint(1, f'Configured Device: {devItem["myId"]}, uuid: {devUuid}')

    logPrint(1, f'Device data initialized, {len(Cfg["uuids"])} devices')


def mqttInit():

    ### TQ-TODO: TLS
    global Cfg, LastPublishTime, mqttClient
    LastPublishTime = datetime.now()

    mqttClient = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             client_id="BleTracker_" + Cfg["NodeName"],
                             transport="tcp", protocol=mqtt.MQTTv5)

    userName = Cfg["mqtt_user"]
    userPass = Cfg["mqtt_pass"]
    if  len(userName) > 0 and len(userPass) > 0 :
        logPrint(3, f'Will authenticate as user: {userName}')
        mqttClient.username_pw_set(userName, userPass)
    else:
        logPrint(3, f'Will skip user authentication, user and/or pass zero')

    if Cfg["mqtt_tls"] :
        logPrint(0, f'Enabling TLS')
        sslCtx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        sslCtx.minimum_version = ssl.TLSVersion.TLSv1_2
        sslCtx.check_hostname = True
        sslCtx.verify_mode = ssl.CERT_REQUIRED
        sslCtx.load_verify_locations(cafile=Cfg["ca_cert"])
        mqttClient.tls_set_context(sslCtx)

    mqttClient.on_connect = mqtt_on_connect
    mqttClient.on_message = mqtt_on_message
    mqttClient.will_set( topic=Cfg["BleTrackerTopic"],payload="offline", qos=1, retain=True)
    mqttClient.loop_start()
    mqttClient.connect(Cfg["mqtt_host"], port=Cfg["mqtt_port"], keepalive=Cfg["mqtt_keepalive"], clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY)
    sleep(2)




def publishMqttDeviceConfig():
    """

    Publish Device Config according to HA Device Discovery Format

    """
    global Cfg, mqttClient
    ### TQ-TODO: Skip publish if it already exists?
    for devUuid, devItem in Cfg["uuids"].items():

        deviceTopic = Cfg["BaseDevTrackTopic"] + devItem["name"].replace(" ", "_")
        mqttPayload = dict()
        mqttPayload["state_topic"] = deviceTopic + "/state"
        mqttPayload["json_attributes_topic"] = deviceTopic + "/attrs"
        mqttPayload["payload_home"] = "home"
        mqttPayload["payload_not_home"] = "not_home"
        mqttPayload["unique_id"] = devUuid
        mqttPayload["source_type"] = "bluetooth_le"
        mqttPayload["name"] = devItem["name"].replace(" BLE", "")

        logPrint(1, f'\nPublishing Device Config: {devItem["name"]}')
        logPrint(4, f'Device details:\n {json.dumps(mqttPayload,indent=2)}')
        mqttClient.publish(deviceTopic + "/config",json.dumps(mqttPayload), qos=1, retain=True)

        mqttClient.subscribe(deviceTopic + "/presence", options=mqtt.SubscribeOptions(qos=1,noLocal=True))
        mqttClient.subscribe(Cfg["BleTrackerSyncTopic"], options=mqtt.SubscribeOptions(qos=1,noLocal=True))
        logPrint(1, f'Subscribing for Topics:', deviceTopic + "/presence", ", ", Cfg["BleTrackerSyncTopic"])

        mqttClient.publish(Cfg["BleTrackerTopic"],"online", qos=1) ### TQ-TODO Add /config section for BLE trackers.
        syncPayload = { "node": Cfg["NodeName"] }
        mqttClient.publish(Cfg["BleTrackerSyncTopic"],json.dumps(syncPayload), qos=1) ### TQ-TODO Add /config section for BLE trackers.

def resetRetainedMqtt():
    global Cfg
    Devices = Cfg["uuids"]

    for devItem in Devices.values():
        devId = devItem["myId"]
        logPrint(3, f"\nPublishDebug: {json.dumps(devItem, indent=2, default=str)}\n")

        deviceTopic = Cfg["BaseDevTrackTopic"] + devId
        logPrint(1, f'Publish - [{devId}] - Shutting Down')

        logPrint(1, f'ForcePublish Topics for device: {devItem["myId"]}')
        publish.single(deviceTopic + "/presence", '{ "location" : null }', qos=1, retain=True,
                       client_id="BleTrackerInit-" + Cfg["NodeName"],
                       hostname=Cfg["mqtt_host"], port=Cfg["mqtt_port"], keepalive=5)
                       ###TQ-TODO: TLS+AUTH #auth = {"username": "", "password" : ""}

def createArgParser():
    cli = argparse.ArgumentParser(
            prog="Ble Mobile Scanner",
            description = "Scan for BLE iBeacons and publish to MQTT", epilog="")

    cli.add_argument('--version', "-V", action='version', version=F'%(prog)s {ScriptVersion}')
    cli.add_argument('--port', "-p", type=int, help="Port", default=-1)
    cli.add_argument("-f", "--file", required=True, help="configuration file")
    cli.add_argument('--fixRetained', action=argparse.BooleanOptionalAction)
    loggingDebug = cli.add_mutually_exclusive_group() # required=true if either cli-param is required
    loggingDebug.add_argument("--verbose", "-v", help="Increase verbosity", action='count', default=0)
    loggingDebug.add_argument("--quiet", "-q", help="Reduce verbosity", action='count', default=0)

    return cli

async def publishToMqtt():
    """
    State Publishing Loop
    """
    global Cfg

    publish_interval = int(Cfg["initialScanDelay"]) - 5
    sleep(5)
    logPrint(0, f'\nStarting PublishLoop with {len(Cfg["uuids"])} devices. Inital delay: {publish_interval}s')
    while True:
        await asyncio.sleep(publish_interval)
        PublishDeviceAvailability()
        # optionally update the interval if it can change at runtime
        publish_interval = Cfg["publishInterval"]

async def bleScanner():
    """
    BLE Scanning Loop
    """
    global Cfg
    scanningInterval = int(Cfg["initialScanDelay"])

    logPrint(0, f'\nStarting BLE Scanner with {len(Cfg["uuids"])} devices. Inital delay: {scanningInterval}s')
    scanner = BleakScanner(detection_callback=device_found)
    while True:
        await scanner.start()
        await asyncio.sleep(scanningInterval)
        await scanner.stop()
        scanningInterval = Cfg["scanDelay"]

async def startAsyncTasks():

    scanner_task = asyncio.create_task(bleScanner())
    publisher_task = asyncio.create_task(publishToMqtt())

    await asyncio.gather(scanner_task, publisher_task)


def main(argv = None):
    global Cfg, mqttClient

    cliArgs = createArgParser()
    args = cliArgs.parse_args()

    configFile = readConfig(args.file)

    try:
        readCfg = json.loads(configFile)
    except Exception as e:
        logPrint(0, f"Error: Bad json configuration file: {configFile}. Error: {e}")
        configJson = None
        exit(1)

    Cfg = dict({ "log_level" : 0 }) | dict(readCfg)

    validateConfiguration()
    logPrint(2, f"ArgsDebug: {args}")
    Cfg["log_level"] = args.verbose - args.quiet

    tempConfig = dict(Cfg)
    tempConfig["mqtt_pass"] = "<>"
    logPrint(2, f"Running Configuration:\n {json.dumps(tempConfig, indent=2, default=str)}")

    logPrint(0, f'Continuing with LogLevel: {Cfg["log_level"]}, version {ScriptVersion}')
    atexit.register(timeToTerminate)
    for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(s, handle_signal)

    populateDevDict()

    if args.fixRetained :
        # Try to fix issues with retained mqtt topic data
        logPrint(0, "Will - ONLY - Init Devices")
        resetRetainedMqtt()
        exit(0)

    mqttInit()

    asyncio.run(startAsyncTasks())
    mqttClient.disconnect()

if __name__ == "__main__":
    sys.exit(main())

