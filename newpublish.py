import paho.mqtt.client as mqtt
from time import sleep
import threading
import time
import re
import sys
import mysql.connector
import paho.mqtt.client as mqttClient
import json
import time
from datetime import datetime
from time import time, sleep
def on_connect(client, userdata, flags, rc):
   if rc == 0:

        print("Connected to broker")

        global Connected  # Use global variable
        Connected = True  # Signal connection

   else:

        print("Connection failed")



# import datetime
# ts = time.time()

# import time
# import datetime
# ts = time.time()



    
Connected = False  # global variable for the state of the connection
    
broker_address = "192.168.0.10"
port = 1883
user = "txt"
password = "xtx"

client = mqttClient.Client("Python")  # create new instance
client.username_pw_set(user, password=password)  # set username and password
client.on_connect = on_connect  # attach function to callback
client.connect(broker_address, port=port)  # connect to broker
client.loop_start()  # start the loop

while Connected != True:  # Wait for connection


    try:
        while True:
            sleep(240 - time() % 240)
            z=datetime.utcnow().isoformat()[:-3]+'Z'
            
            print(z)
            value = {"ts":z, "type":"WHITE"}
            value=json.dumps(value)

            client.publish("f/o/order", value)
            break
           
        while True:
            sleep(240 - time() % 240)
            z=datetime.utcnow().isoformat()[:-3]+'Z'
            
            print(z)
            value = {"ts":z, "type":"BLUE"}
            value=json.dumps(value)

            client.publish("f/o/order", value)
            break
             
        while True:
            sleep(240 - time() % 240)
            z=datetime.utcnow().isoformat()[:-3]+'Z'
            
            print(z)
            value = {"ts":z, "type":"RED"}
            value=json.dumps(value)
            client.publish("f/o/order", value)
            break
        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "WHITE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "BLUE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "RED"}
            value = json.dumps(value)
            client.publish("f/o/order", value)
            break
        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "WHITE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "BLUE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "RED"}
            value = json.dumps(value)
            client.publish("f/o/order", value)
            break
        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "WHITE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "BLUE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "RED"}
            value = json.dumps(value)
            client.publish("f/o/order", value)
            break
        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "WHITE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "BLUE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "RED"}
            value = json.dumps(value)
            client.publish("f/o/order", value)
            break
        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "WHITE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "BLUE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "RED"}
            value = json.dumps(value)
            client.publish("f/o/order", value)
            break
        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "WHITE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "BLUE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "RED"}
            value = json.dumps(value)
            client.publish("f/o/order", value)
            break
        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "WHITE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "BLUE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "RED"}
            value = json.dumps(value)
            client.publish("f/o/order", value)
            break
        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "WHITE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "BLUE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "RED"}
            value = json.dumps(value)
            client.publish("f/o/order", value)
            break
        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "WHITE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "BLUE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "RED"}
            value = json.dumps(value)
            client.publish("f/o/order", value)
            break
        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "WHITE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "BLUE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "RED"}
            value = json.dumps(value)
            client.publish("f/o/order", value)
            break
        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "WHITE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "BLUE"}
            value = json.dumps(value)

            client.publish("f/o/order", value)
            break

        while True:
            sleep(240 - time() % 240)
            z = datetime.utcnow().isoformat()[:-3] + 'Z'

            print(z)
            value = {"ts": z, "type": "RED"}
            value = json.dumps(value)
            client.publish("f/o/order", value)
            break
    except KeyboardInterrupt:

        client.disconnect()
        client.loop_stop()


# import time
# import datetime
# ts = time.time()
           