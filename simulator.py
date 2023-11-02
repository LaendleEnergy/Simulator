import json
import time
import datetime
import paho.mqtt.client as mqtt
import struct
import ssl
import os

# MQTT broker settings
broker_address = os.getenv("BROKER_ADDRESS", "45.145.224.10")
broker_port = str(os.getenv("BROKER_PORT", "1883"))
topic = os.getenv("ROOT_TOPIC", "smartmeter")
filename = os.getenv("DATA_FILE_NAME", "mqtt_messages_2.json")

OBIS_CODE = {
    "1.7.0": "Momentane Wirkleistung P+ [W]",
    "1.8.0": "Zählerstand Energie A+ [Wh]",
    "2.7.0": "Momentane Wirkleistung P- [W]",
    "2.8.0": "Zählerstand Energie A- [Wh]",
    # "3.8.0": "Zählerstand +Q [VAh]",
    # "4.8.0": "Zählerstand -Q [VAh]",
    # "16.7.0": "Momentane Wirkleistung P+- [W]",
    "31.7.0": "Momentaner Strom L1 [A]",
    "32.7.0": "Momentane Spannung L1 [V]",
    "51.7.0": "Momentaner Strom L2 [A]",
    "52.7.0": "Momentane Spannung L2 [V]",
    "71.7.0": "Momentaner Strom L3 [A]",
    "72.7.0": "Momentane Spannung L3 [V]",
}

def packBinary(data):
    utc_time = datetime.datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%S")
    epoch_time = (utc_time - datetime.datetime(1970, 1, 1)).total_seconds()
    return struct.pack("Iffffffffff",
        int(epoch_time),
        float(data["1.7.0"]),
        float(data["1.8.0"]),
        float(data["2.7.0"]),
        float(data["2.8.0"]),
        float(data["31.7.0"]),
        float(data["32.7.0"]),
        float(data["51.7.0"]),
        float(data["52.7.0"]),
        float(data["71.7.0"]),
        float(data["72.7.0"])
    )

last_new_message = None 
counter_whole = 0
counter_send = 0
def send_if_new_data(id, new_message):
    global last_new_message
    global counter_whole
    global counter_send
    offset = 0.05
    send = False
    for key in new_message.keys():
        if "timestamp" not in key and "uptime" not in key:
            if last_new_message is not None and key in new_message.keys() and key in last_new_message.keys():
                datapoint_new = new_message[key]
                datapoint_old = last_new_message[key]
                if abs(float(datapoint_new) - float(datapoint_old)) > float(datapoint_old)*offset:
                    send = True
            else:
                send = True

    if send:
        client.publish(id, packBinary(new_message))
        last_new_message = new_message
        counter_send+=1

    #file.write(f"{last_new_message['timestamp']};{new_message['31.7.0']};{last_new_message['31.7.0']}\n")
    counter_whole+=1



if __name__ == "__main__":
    print("broker_address:", broker_address)
    print("broker_port:", broker_port)
    print("topic:", topic)
    print("filename:", filename)

    # Create an MQTT client
    client = mqtt.Client()

    # Connect to the MQTT broker
    print("connecting to broker...")
    client.tls_set(ca_certs="./certs/ca.crt", certfile="./certs/client.crt", keyfile="./certs/client.key", cert_reqs=ssl.CERT_REQUIRED)
    client.tls_insecure_set(True)
    result = client.connect(broker_address, int(broker_port, 10), keepalive=60)
    print("connected")

    # Start the MQTT client loop (non-blocking)
    client.loop_start()

    print("opening file", filename)
    input = open(filename, 'r')
    print("file opened")

    print("start publishing simulatordata from file " + filename + " to topic " + topic)

    while True:
        line = input.readline()
        if "message" in line:
            try:
                line = line.replace('"message": ', '')
                data_obj = json.loads(line)
                print("sent data", json.dumps(data_obj))
                send_if_new_data(topic, data_obj)
            except:
                pass
        time.sleep(5)


