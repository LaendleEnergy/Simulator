import json
import time
import datetime
import paho.mqtt.client as mqtt


# MQTT broker settings
broker_address = "45.145.224.10"  # Replace with your MQTT broker's address
broker_port = 1883  # Default MQTT port
topic = "smartmeter"  # Replace with the desired MQTT topic

OBIS_CODE = {
    "1.7.0": "Momentane Wirkleistung P+ [W]",
    "1.8.0": "Zählerstand Energie A+ [Wh]",
    "2.7.0": "Momentane Wirkleistung P- [W]",
    "2.8.0": "Zählerstand Energie A- [Wh]",
    "3.8.0": "Zählerstand +Q [VAh]",
    "4.8.0": "Zählerstand -Q [VAh]",
    "16.7.0": "Momentane Wirkleistung P+- [W]",
    "31.7.0": "Momentaner Strom L1 [A]",
    "32.7.0": "Momentane Spannung L1 [V]",
    "51.7.0": "Momentaner Strom L2 [A]",
    "52.7.0": "Momentane Spannung L2 [V]",
    "71.7.0": "Momentaner Strom L3 [A]",
    "72.7.0": "Momentane Spannung L3 [V]",
}

last_msg = None
loop = False

def on_message(client, userdata, msg):
    global last_msg
    global loop
    loop = True
    data_obj = json.loads(msg.payload.decode())
    last_msg = data_obj["message"]


def on_connect(client, userdata, flags, rc):
    client.subscribe(topic)

if __name__ == "__main__":

    data_file = open("data.txt", "w")
    # Create an MQTT client
    client = mqtt.Client()

    # Connect to the MQTT broker
    client.connect(broker_address, broker_port, keepalive=60)

    client.on_connect = on_connect
    client.on_message = on_message

    client.loop_start()

    while True:
        if last_msg is not None:
            value = last_msg['31.7.0'].replace(".", ",")
            timestamp = datetime.datetime.strptime(last_msg['timestamp'],"%Y-%m-%dT%h:%m:%s")
            data_string = f"{timestamp};{value}"
            print(data_string)
            time.sleep(5)

    client.loop_stop()


