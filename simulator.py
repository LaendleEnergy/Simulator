import json
import time
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

def receive_mbus(filename, interval=5):
    with open(filename, 'r') as file:
        while True:
            data = ""
            line = ""
            while "}," not in line:
                line=file.readline()
                if "[" in line:
                    continue
                data+=line
            data = data[:-2]
            try:
                data_obj = json.loads(data)
                yield data_obj
            except Exception as e:
                print("JSON Error", e)
                pass
            time.sleep(interval)
            if not line:
                break  # End of file

last_new_message = {}
last_new_message["31.7.0"] = "-" 
counter_whole = 0
counter_send = 0
def send_if_new_data(data_new):
    global last_new_message
    global counter_whole
    global counter_send
    offset = 0.05
    send = False
    for key in data_new.keys():
        if "timestamp" not in key and "uptime" not in key:
            if last_new_message is not None and data_new[key] is not None and last_new_message[key] is not None:
                datapoint_new = data_new[key]
                datapoint_old = last_new_message[key]
                if abs(float(datapoint_new) - float(datapoint_old)) > float(datapoint_old)*offset:
                    print(abs(float(datapoint_new) - float(datapoint_old)), float(datapoint_old)*offset)
                    send = True
            else:
                send = True

    if send:
        client.publish(topic, json.dumps(data_new))
        last_new_message = data_new
        counter_send+=1

    file.write(f"{last_new_message['31.7.0']}\n")
    counter_whole+=1



if __name__ == "__main__":
    filename = 'mqtt_messages_2.json'

    # Create an MQTT client
    client = mqtt.Client()

    # Connect to the MQTT broker
    client.connect(broker_address, broker_port, keepalive=60)

    # Start the MQTT client loop (non-blocking)
    client.loop_start()

    file = open("data.txt", "w")

    try:
        while True:
            for mbus_data in receive_mbus(filename, 5):
                send_if_new_data(mbus_data["message"])
                print(json.dumps(mbus_data, indent=2))

    except Exception as e:
        print(e)
        print(f"Data points whole: {counter_whole}, data points send 5%: {counter_send}")

    finally:
        # Disconnect from the MQTT broker
        client.disconnect()
        client.loop_stop()

