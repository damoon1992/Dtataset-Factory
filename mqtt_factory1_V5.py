#MQTT TestmqttClient GUI
import paho.mqtt.client as mqtt
from time import sleep
import threading
import time
import re
import sys
import mysql.connector
import paho.mqtt.client as mqttClient
import paho

#Connect to Server
def process_connect2Server():
    ##connect to MQTTServer:  
    # print("start connect")
    mqttClient.connect("192.168.0.10", 1883, 60)
    mqttClient.loop_start()

#Connect to Server2
def process_connect2Server2():
    ##connect to MQTTServer:  
    # print("start connect2")
    mqttClient2.connect("broker.hivemq.com", 1883, 60)
    mqttClient2.loop_start()
    

#after connection was established
def on_connect(mqttClient, userdata, flags, rc):
    # print("connected")
    #subscribe for topics
    mqttClient.subscribe("i/bme680", 0)
    # print("subscribed for topic i/bme680")
    
    mqttClient.subscribe("i/ldr", 0)
    # print("subscribed for topic i/ldr")

    mqttClient.subscribe("i/cam", 0)
    # print("subscribed for topic i/cam")

    mqttClient.subscribe("i/ptu/pos", 0)
    # print("subscribed for topic i/ptu/pos")
    
    mqttClient.subscribe("i/alert", 0)
    # print("subscribed for topic i/alert")
    
    mqttClient.subscribe("f/i/state/hbw", 0)
    # print("subscribed for topic f/i/state/hbw")
    
    mqttClient.subscribe("f/i/state/vgr", 0)
    # print("subscribed for topic f/i/state/vgr")
    
    mqttClient.subscribe("f/i/state/mpo", 0)
    # print("subscribed for topic f/i/state/mpo")
    
    mqttClient.subscribe("f/i/state/sld", 0)
    # print("subscribed for topic f/i/state/sld")
    
    mqttClient.subscribe("f/i/state/dsi", 0)
    # print("subscribed for topic f/i/state/dsi")
    
    mqttClient.subscribe("f/i/state/dso", 0)
    # print("subscribed for topic f/i/state/dso")
    
    mqttClient.subscribe("f/i/stock", 0)
    # print("subscribed for topic f/i/stock")
    
    mqttClient.subscribe("f/i/order", 0)
    # print("subscribed for topic f/i/order")

    mqttClient.subscribe("f/i/nfc/ds", 0)
    # print("subscribed for topic f/i/nfc/ds")
    


 

#after connection was established 2
def on_connect2(mqttClient2, userdata, flags, rc):
    print("connected2")
    

#Reseive message and send with latence a response
def on_message(mosq, obj, msg):

    try:


        if msg.topic.startswith("i/bme680"):


            s= str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            s = s.replace('{', '')
            s = s.replace('}', '')


            s = re.split(r"\s*[,;]\s*", s.strip())

            # s.decode('utf-8', 'ignore')
            # print(s)
            s[0] = float(re.search(r'\d+', s[0]).group(0))
            print("air quality score 0-3",s[0])
            ff=str(s[0])
            s[1]=float(re.search(r'\d+\.\d+', s[1]).group(0))
            print("gas resistance [Ohm]",s[1])
            s[2]=float(re.search(r'\d+\.\d+', s[2]).group(0))
            print("relative humidity compensated",s[2])
            s[3]=float(re.search(r'\d+\.\d+', s[3]).group(0))
            print("index air quality 0-500",s[3])
            s[4]=float(re.search(r'\d+\.\d+', s[4]).group(0))
            print("air pressure",s[4])
            s[5]=float(re.search(r'\d+\.\d+', s[5]).group(0))
            print("relative humidity compensated / raw value",s[5])
            s[6]=float(re.search(r'\d+\.\d+', s[6]).group(0))
            print("temperature compensated/raw value [°C]",s[6])
            s[7]=float(re.search(r'\d+\.\d+', s[7]).group(0))
            print("temperature compensated ",s[7])
            s[8] = s[8].replace('T', ' ')
            s[8] = s[8].replace('Z', '')
            z=re.findall(r'"([^"]*)"', s[8])
            s[8]=z[1]

            print("timestamp",s[8])
            print(s[8].count(s[8]))
            s=[s[0],s[1],s[2],s[3],s[4],s[5],s[6],s[7],s[8]]
            print(s)
            #
            try:
                connection = mysql.connector.connect(
                    host='192.168.0.106',
                    database='fischertechnik',
                    username='pi',
                    password='pi')
                cursor = connection.cursor()
                # sql = "DELETE FROM EnvironmentSensor"


                sql = "INSERT INTO EnvironmentSensor (airQualityScore, gasResistance,humidityCompensated,indexAirQuality,airPressure,humidityRaw,temperatureRaw,temperatureCompensated,time_stamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, s)
                # cursor.execute(sql)

                connection.commit()
                print(cursor.rowcount, "Record inserted successfully into Laptop table")
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            #
            #
            #
            #





            # print(str(msg.payload))
            # ok = mqttClient2.publish("hnu/factory1/i/bme680", msg.payload)
            # print("_"*60)
        if msg.topic.startswith("i/ldr"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            s = s.replace('{', '')
            s = s.replace('}', '')
            #
            s = re.split(r"\s*[,;]\s*", s.strip())
            s[0] = float(re.search(r'\d+', s[0]).group(0))
            print("brightness 0-100.0 [%]",s[0])
            s[1] = float(re.search(r'\d+', s[1]).group(0))
            print("value resistance 0-15000 [Ohm]",s[1])
            s[2] = s[2].replace('T', ' ')
            s[2] = s[2].replace('Z', '')
            z=re.findall(r'"([^"]*)"', s[2])
            s[2]=z[1]

            print("timestamp",s[2])
            print(str(msg.payload))
            ok = mqttClient2.publish("hnu/factory1/i/ldr", msg.payload)
            try:
                connection = mysql.connector.connect(
                    host='192.168.0.106',
                    database='fischertechnik',
                    username='pi',
                    password='pi')
                cursor = connection.cursor()

                sql = "INSERT INTO BrightnessSensor (brightness, ohm,time_stamp) VALUES (%s, %s, %s)"
                ##### for delete ############
                # sql = "DELETE FROM BrightnessSensor"
                # cursor.execute(sql)
                cursor.execute(sql,s)

                connection.commit()

                print(cursor.rowcount, "Record inserted successfully into Laptop table")
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            print("_"*60)

        if msg.topic.startswith("i/cam"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            # print("i/cam", s)
            s = s.replace('{', '')
            s = s.replace('}', '')

            s = re.split(r"\s*[,;]\s*", s.strip())
            # print("i/cam",s[2])
            s[3] = s[3].replace('T', ' ')
            s[3] = s[3].replace('Z', '')
            z=re.findall(r'"([^"]*)"', s[3])
            s[3]=z[1]

            # print("timestamp",s[3])
            # print(str(msg.payload))
            print("_"*60)
            s=[s[2],s[3]]
            try:
                connection = mysql.connector.connect(
                    host='192.168.0.106',
                    database='fischertechnik',
                    username='pi',
                    password='pi')
                cursor = connection.cursor()

                sql = "INSERT INTO CameraPicture (base64,time_stamp) VALUES (%s, %s)"
                ##### for delete ############
                # sql = "DELETE FROM BrightnessSensor"
                # cursor.execute(sql)
                cursor.execute(sql, s)

                connection.commit()

                print(cursor.rowcount, "Record inserted successfully into Laptop table")
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))

            ok = mqttClient2.publish("hnu/factory1/i/cam", msg.payload)
        if msg.topic.startswith("i/ptu/pos"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            s = s.replace('{', '')
            s = s.replace('}', '')

            s = re.split(r"\s*[,;]\s*", s.strip())
            print("relative position pan: -1.000...0.000...1.000", s[0])
            print("relative position tilt: -1.000...0.000...1.000", s[1])
            print("i/ptu/pos",s)
            print(str(msg.payload))
            ok = mqttClient2.publish("hnu/factory1/i/ptu/pos", msg.payload)
            print("_"*60)

        if msg.topic.startswith("i/alert"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            s = s.replace('{', '')
            s = s.replace('}', '')

            s = re.split(r"\s*[,;]\s*", s.strip())
            # s = s.split()
            print("i/alert",s)

            # print("air quality score 0-3",s[0])
            # print("gas resistance [Ohm]",s[1])
            # print("relative humidity compensated",s[2])
            # print("index air quality 0-500",s[3])
            # print("air pressure",s[4])
            # print(str(msg.payload))
            # print("_"*60)

            ok = mqttClient2.publish("hnu/factory1/i/alert", msg.payload)
        if msg.topic.startswith("f/i/state/hbw"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            s = s.replace('{', '')
            s = s.replace('}', '')


            s = re.split(r"\s*[,;]\s*", s.strip())
            s[0] = float(re.search(r'\d+', s[0]).group(0))
            print("active",s[0])
            s[1]=float(re.search(r'\d+', s[1]).group(0))
            print("code",s[1])
            z=re.findall(r'"([^"]*)"', s[2])
            s[2]=z[1]

            print("description",s[2])
            z=re.findall(r'"([^"]*)"', s[3])
            s[3]=z[1]

            print("station",s[3])
            s[4] = s[4].replace('T', ' ')
            s[4] = s[4].replace('Z', '')
            z=re.findall(r'"([^"]*)"', s[4])
            s[4]=z[1]
            print(s[4])

            try:
                connection = mysql.connector.connect(
                    host='192.168.0.106',
                    database='fischertechnik',
                    username='pi',
                    password='pi')
                cursor = connection.cursor()
                # sql = "DELETE FROM HighBayWarehouse"


                sql = "INSERT INTO HighBayWarehouse (active, code,description,station,time_stamp) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql, s)
                # cursor.execute(sql)
                connection.commit()
                print(cursor.rowcount, "Record inserted successfully into Laptop table")
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            print("_"*60)

        if msg.topic.startswith("f/i/state/vgr"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("f/i/state/vgr",s)
            s = s.replace('{', '')
            s = s.replace('}', '')

            s = re.split(r"\s*[,;]\s*", s.strip())
            s[0] = float(re.search(r'\d+', s[0]).group(0))
            print("active_vgr", s[0])
            s[1] = float(re.search(r'\d+', s[1]).group(0))
            print("code", s[1])
            z = re.findall(r'"([^"]*)"', s[2])
            s[2] = z[1]

            print("description", s[2])
            z = re.findall(r'"([^"]*)"', s[3])
            s[3] = z[1]

            print("station", s[3])
            z1 = re.findall(r'"([^"]*)"', s[4])
            s[4] = z1[1]

            print("target", s[4])
            s[5] = s[5].replace('T', ' ')
            s[5] = s[5].replace('Z', '')
            z = re.findall(r'"([^"]*)"', s[5])
            s[5] = z[1]
            print(s[5])
            try:
                connection = mysql.connector.connect(
                    host='192.168.0.106',
                    database='fischertechnik',
                    username='pi',
                    password='pi')
                cursor = connection.cursor()
                # sql = "DELETE FROM VacuumGripperRobot"


                sql = "INSERT INTO VacuumGripperRobot (active, code,description,station,target,time_stamp) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, s)
                # cursor.execute(sql)
                connection.commit()
                print(cursor.rowcount, "Record inserted successfully into Laptop table")
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            # print(str(msg.payload))
            # ok = mqttClient2.publish("hnu/factory1/f/i/state/vgr", msg.payload)
            # print("msg.payload",msg.payload)
            # print("_"*60)

        if msg.topic.startswith("f/i/state/mpo"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("f/i/state/mpo",s)
            s = s.replace('{', '')
            s = s.replace('}', '')

            s = re.split(r"\s*[,;]\s*", s.strip())
            s[0] = float(re.search(r'\d+', s[0]).group(0))
            print("active_mpo", s[0])
            s[1] = float(re.search(r'\d+', s[1]).group(0))
            print("code", s[1])
            z = re.findall(r'"([^"]*)"', s[2])
            s[2] = z[1]

            print("description", s[2])
            z = re.findall(r'"([^"]*)"', s[3])
            s[3] = z[1]

            print("station", s[3])

            s[4] = s[4].replace('T', ' ')
            s[4] = s[4].replace('Z', '')
            z = re.findall(r'"([^"]*)"', s[4])
            s[4] = z[1]
            print(s[4])
            print("f/i/state/mpo",s)
            try:
                connection = mysql.connector.connect(
                    host='192.168.0.106',
                    database='fischertechnik',
                    username='pi',
                    password='pi')
                cursor = connection.cursor()
                # sql = "DELETE FROM MultiProcessingStation"

                sql = "INSERT INTO MultiProcessingStation (active, code,description,station,time_stamp) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql, s)
                # cursor.execute(sql)

                connection.commit()
                print(cursor.rowcount, "Record inserted successfully into Laptop table")
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            print(str(msg.payload))
            ok = mqttClient2.publish("hnu/factory1/f/i/state/mpo", msg.payload)
            print("_"*60)

        if msg.topic.startswith("f/i/state/sld"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("f/i/state/sld",s)
            s = s.replace('{', '')
            s = s.replace('}', '')

            s = re.split(r"\s*[,;]\s*", s.strip())
            s[0] = float(re.search(r'\d+', s[0]).group(0))
            print("active_sld", s[0])
            s[1] = float(re.search(r'\d+', s[1]).group(0))
            print("code", s[1])
            z = re.findall(r'"([^"]*)"', s[2])
            s[2] = z[1]

            print("description", s[2])
            z = re.findall(r'"([^"]*)"', s[3])
            s[3] = z[1]

            print("station", s[3])

            s[4] = s[4].replace('T', ' ')
            s[4] = s[4].replace('Z', '')
            z = re.findall(r'"([^"]*)"', s[4])
            s[4] = z[1]
            print(s[4])
            print("f/i/state/sld",s)
            try:
                connection = mysql.connector.connect(
                    host='192.168.0.106',
                    database='fischertechnik',
                    username='pi',
                    password='pi')
                cursor = connection.cursor()
                # sql = "DELETE FROM SortingLine"
                #
                sql = "INSERT INTO SortingLine (active, code,description,station,time_stamp) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql, s)
                # cursor.execute(sql)

                connection.commit()
                print(cursor.rowcount, "Record inserted successfully into Laptop table")
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            print(str(msg.payload))
            ok = mqttClient2.publish("hnu/factory1/f/i/state/sld", msg.payload)
            print("_"*60)

        if msg.topic.startswith("f/i/state/dsi"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("f/i/state/dsi", s)
            s = s.replace('{', '')
            s = s.replace('}', '')

            s = re.split(r"\s*[,;]\s*", s.strip())
            s[0] = float(re.search(r'\d+', s[0]).group(0))
            print("active_dsi", s[0])
            s[1] = float(re.search(r'\d+', s[1]).group(0))
            print("code", s[1])
            z = re.findall(r'"([^"]*)"', s[2])
            s[2] = z[1]

            print("description", s[2])
            z = re.findall(r'"([^"]*)"', s[3])
            s[3] = z[1]

            print("station", s[3])

            s[4] = s[4].replace('T', ' ')
            s[4] = s[4].replace('Z', '')
            z = re.findall(r'"([^"]*)"', s[4])
            s[4] = z[1]
            print(s[4])
            print("f/i/state/dsi",s)
            try:
                connection = mysql.connector.connect(
                    host='192.168.0.106',
                    database='fischertechnik',
                    username='pi',
                    password='pi')
                cursor = connection.cursor()
                # sql = "DELETE FROM StateDSIVGR"

                sql = "INSERT INTO StateDSIVGR (active, code,description,station,time_stamp) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql, s)
                # cursor.execute(sql)

                connection.commit()
                print(cursor.rowcount, "Record inserted successfully into Laptop table")
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            print(str(msg.payload))
            ok = mqttClient2.publish("hnu/factory1/f/i/state/dsi", msg.payload)
            print("_"*60)

        if msg.topic.startswith("f/i/state/dso"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("f/i/state/dso", s)
            s = s.replace('{', '')
            s = s.replace('}', '')

            s = re.split(r"\s*[,;]\s*", s.strip())
            s[0] = float(re.search(r'\d+', s[0]).group(0))
            print("active_dsi", s[0])
            s[1] = float(re.search(r'\d+', s[1]).group(0))
            print("code", s[1])
            z = re.findall(r'"([^"]*)"', s[2])
            s[2] = z[1]

            print("description", s[2])
            z = re.findall(r'"([^"]*)"', s[3])
            s[3] = z[1]

            print("station", s[3])

            s[4] = s[4].replace('T', ' ')
            s[4] = s[4].replace('Z', '')
            z = re.findall(r'"([^"]*)"', s[4])
            s[4] = z[1]
            print(s[4])
            print("f/i/state/dso", s)
            print("f/i/state/dso",s)
            try:
                connection = mysql.connector.connect(
                    host='192.168.0.106',
                    database='fischertechnik',
                    username='pi',
                    password='pi')
                cursor = connection.cursor()
                # sql = "DELETE FROM StateDSOVGR"

                sql = "INSERT INTO StateDSOVGR (active, code,description,station,time_stamp) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql, s)
                # cursor.execute(sql)

                connection.commit()
                print(cursor.rowcount, "Record inserted successfully into Laptop table")
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            print(str(msg.payload))
            print("_"*60)

            ok = mqttClient2.publish("hnu/factory1/f/i/state/dso", msg.payload)
        if msg.topic.startswith("hnu/factory1/f/i/state/dso"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("f/i/state/dso", s)
            s = s.replace('{', '')
            s = s.replace('}', '')

            s = re.split(r"\s*[,;]\s*", s.strip())
            s[0] = float(re.search(r'\d+', s[0]).group(0))
            print("active_dso", s[0])
            s[1] = float(re.search(r'\d+', s[1]).group(0))
            print("code", s[1])
            z = re.findall(r'"([^"]*)"', s[2])
            s[2] = z[1]

            print("description", s[2])
            z = re.findall(r'"([^"]*)"', s[3])
            s[3] = z[1]

            print("station", s[3])

            s[4] = s[4].replace('T', ' ')
            s[4] = s[4].replace('Z', '')
            z = re.findall(r'"([^"]*)"', s[4])
            s[4] = z[1]
            print(s[4])
            print("hnu/factory1/f/i/state/dso",s)
            print(str(msg.payload))
            ok = mqttClient2.publish("hnu/factory1/f/i/stock", msg.payload)
            print("_"*60)
        if msg.topic.startswith("f/i/stock"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("f/i/state/stock",s)
            s = s.replace('{', '')
            s = s.replace('}', '')
            s = re.split(r"\s*[,;]\s*", s.strip())
            print("f/i/stock",s)
            s[-1] = s[-1].replace('T', ' ')
            s[-1] = s[-1].replace('Z', '')
            z = re.findall(r'"([^"]*)"', s[-1])
            s[-1] = z[1]
            print(s[-1])
            print("time_stamp",s[-1])
            matches_location = [match for match in s if "workpiece" in match]
            print("work_LOCA",matches_location)
            matches_id = [match for match in matches_location if "id" in match]
            print("work",matches_id)
            index=[]

            for i in range(len(matches_id)) :
                index=s.index(matches_id[i])





                index=index-1
                print(s[index])
                z = re.findall(r'"([^"]*)"', s[index])
                s[index] = z[1]


                # print("location",s[index])
                print("iii",index)
                if s[index]=="A1":
                    print("A1",s[index])
                    index_a1_id = index + 1
                    za1 = re.findall(r'"([^"]*)"', s[index_a1_id])
                    s[index_a1_id] = za1[2]
                    print("indexa2", s[index_a1_id])
                    index_a1_state = index + 2
                    za1_state = re.findall(r'"([^"]*)"', s[index_a1_state])
                    s[index_a1_state] = za1_state[1]
                    print("index_a1_state", s[index_a1_state])
                    index_a1_type = index + 3
                    za1_type = re.findall(r'"([^"]*)"', s[index_a1_type])
                    s[index_a1_type] = za1_type[1]
                    print("index_a1_type", s[index_a1_type])
                    SA1=[s[index],s[index_a1_id],s[index_a1_state],s[index_a1_type],s[-1]]
                if s[index]=="A2":
                    print("A2",s[index])
                    index_a2_id = index + 1
                    za2 = re.findall(r'"([^"]*)"', s[index_a2_id])
                    s[index_a2_id] = za2[2]
                    print("indexa2", s[index_a2_id])
                    index_a2_state = index + 2
                    za2_state = re.findall(r'"([^"]*)"', s[index_a2_state])
                    s[index_a2_state] = za2_state[1]
                    print("index_a2_state", s[index_a2_state])
                    index_a2_type = index + 3
                    za2_type = re.findall(r'"([^"]*)"', s[index_a2_type])
                    s[index_a2_type] = za2_type[1]
                    print("index_a2_type", s[index_a2_type])
                    SA2=[s[index],s[index_a2_id],s[index_a2_state],s[index_a2_type],s[-1]]
                    print("SA2",SA2)

                if s[index]=="A3":
                    print("A3",s[index])
                    index_a3_id = index + 1
                    za3 = re.findall(r'"([^"]*)"', s[index_a3_id])
                    s[index_a3_id] = za3[2]
                    print("indexa2", s[index_a3_id])
                    index_a3_state = index + 2
                    za3_state = re.findall(r'"([^"]*)"', s[index_a3_state])
                    s[index_a3_state] = za3_state[1]
                    print("index_a3_state", s[index_a3_state])
                    index_a3_type = index + 3
                    za3_type = re.findall(r'"([^"]*)"', s[index_a3_type])
                    s[index_a3_type] = za3_type[1]
                    print("index_a3_type", s[index_a3_type])
                    SA3=[s[index],s[index_a3_id],s[index_a3_state],s[index_a3_type],s[-1]]
                    print("SA3",SA3)
                if s[index]=="B1":
                    print("B1",s[index])
                    index_b1_id=index+1
                    zb1= re.findall(r'"([^"]*)"', s[index_b1_id])
                    s[index_b1_id] = zb1[2]
                    print("indexb1",  s[index_b1_id])
                    index_b1_state=index+2
                    zb1_state = re.findall(r'"([^"]*)"', s[index_b1_state])
                    s[index_b1_state] = zb1_state[1]
                    print("index_b1_state",  s[index_b1_state])
                    index_b1_type=index+3
                    zb1_type = re.findall(r'"([^"]*)"', s[index_b1_type])
                    s[index_b1_type] = zb1_type[1]
                    print("index_b1_type",  s[index_b1_type])
                    SB1=[s[index],s[index_b1_id],s[index_b1_state],s[index_b1_type],s[-1]]
                    print("SB1",SB1)
                    try:
                        connection = mysql.connector.connect(
                            host='192.168.0.106',
                            database='fischertechnik',
                            username='pi',
                            password='pi')
                        cursor = connection.cursor()
                        # sql = "DELETE FROM Stock"

                        sql = "INSERT INTO Stock (location, idWorkpiece,state1,type1,time_stamp) VALUES (%s, %s, %s, %s, %s)"

                        cursor.execute(sql, SB1)
                        # cursor.execute(sql)

                        connection.commit()
                        print(cursor.rowcount, "Record inserted successfully into Laptop table")
                        cursor.close()

                    except mysql.connector.Error as error:
                        print("Failed to insert record into Laptop table {}".format(error))

                if s[index]=="B2":
                    print("B2",s[index])
                    index_b2_id=index+1
                    zb2= re.findall(r'"([^"]*)"', s[index_b2_id])
                    s[index_b2_id] = zb2[2]
                    print("indexb2",  s[index_b2_id])
                    index_b2_state=index+2
                    zb2_state = re.findall(r'"([^"]*)"', s[index_b2_state])
                    s[index_b2_state] = zb2_state[1]
                    print("index_b2_state",  s[index_b2_state])
                    index_b2_type=index+3
                    zb2_type = re.findall(r'"([^"]*)"', s[index_b2_type])
                    s[index_b2_type] = zb2_type[1]
                    print("index_b2_type",  s[index_b2_type])
                    SB2=[s[index],s[index_b2_id],s[index_b2_state],s[index_b2_type],s[-1]]
                    print("SB2",SB2)
                    try:
                        connection = mysql.connector.connect(
                            host='192.168.0.106',
                            database='fischertechnik',
                            username='pi',
                            password='pi')
                        cursor = connection.cursor()
                        # sql = "DELETE FROM Stock"

                        sql = "INSERT INTO Stock (location, idWorkpiece,state1,type1,time_stamp) VALUES (%s, %s, %s, %s, %s)"

                        cursor.execute(sql, SB2)
                        # cursor.execute(sql)

                        connection.commit()
                        print(cursor.rowcount, "Record inserted successfully into Laptop table")
                        cursor.close()

                    except mysql.connector.Error as error:
                        print("Failed to insert record into Laptop table {}".format(error))
                if s[index]=="B3":
                    print("B3", s[index])
                    index_b3_id = index + 1
                    zb3 = re.findall(r'"([^"]*)"', s[index_b3_id])
                    s[index_b3_id] = zb3[2]
                    print("indexb3", s[index_b3_id])
                    index_b3_state = index + 2
                    zb3_state = re.findall(r'"([^"]*)"', s[index_b3_state])
                    s[index_b3_state] = zb3_state[1]
                    print("index_b3_state", s[index_b3_state])
                    index_b3_type = index + 3
                    zb3_type = re.findall(r'"([^"]*)"', s[index_b3_type])
                    s[index_b3_type] = zb3_type[1]
                    print("index_b3_type", s[index_b3_type])
                    SB3=[s[index],s[index_b3_id],s[index_b3_state],s[index_b3_type],s[-1]]
                    print("SB3",SB3)
                    try:
                        connection = mysql.connector.connect(
                            host='192.168.0.106',
                            database='fischertechnik',
                            username='pi',
                            password='pi')
                        cursor = connection.cursor()
                        # sql = "DELETE FROM Stock"

                        sql = "INSERT INTO Stock (location, idWorkpiece,state1,type1,time_stamp) VALUES (%s, %s, %s, %s, %s)"

                        cursor.execute(sql, SB3)
                        # cursor.execute(sql)

                        connection.commit()
                        print(cursor.rowcount, "Record inserted successfully into Laptop table")
                        cursor.close()

                    except mysql.connector.Error as error:
                        print("Failed to insert record into Laptop table {}".format(error))
                if s[index]=="C1":
                    print("C1",s[index])
                    index_c1_id = index + 1
                    zc1 = re.findall(r'"([^"]*)"', s[index_c1_id])
                    s[index_c1_id] = zc1[2]
                    print("indexb2", s[index_c1_id])
                    index_c1_state = index + 2
                    zc1_state = re.findall(r'"([^"]*)"', s[index_c1_state])
                    s[index_c1_state] = zc1_state[1]
                    print("index_c1_state", s[index_c1_state])
                    index_c1_type = index + 3
                    zc1_type = re.findall(r'"([^"]*)"', s[index_c1_type])
                    s[index_c1_type] = zc1_type[1]
                    print("index_c1_type", s[index_c1_type])
                    SC1=[s[index],s[index_c1_id],s[index_c1_state],s[index_c1_type],s[-1]]
                    print("SC1",SC1)
                    try:
                        connection = mysql.connector.connect(
                            host='192.168.0.106',
                            database='fischertechnik',
                            username='pi',
                            password='pi')
                        cursor = connection.cursor()
                        # sql = "DELETE FROM Stock"

                        sql = "INSERT INTO Stock (location, idWorkpiece,state1,type1,time_stamp) VALUES (%s, %s, %s, %s, %s)"

                        cursor.execute(sql, SC1)
                        # cursor.execute(sql)

                        connection.commit()
                        print(cursor.rowcount, "Record inserted successfully into Laptop table")
                        cursor.close()

                    except mysql.connector.Error as error:
                        print("Failed to insert record into Laptop table {}".format(error))
                if s[index]=="C2":
                    print("C2",s[index])
                    index_c2_id = index + 1
                    zc2 = re.findall(r'"([^"]*)"', s[index_c2_id])
                    s[index_c2_id] = zc2[2]
                    print("indexc2", s[index_c2_id])
                    index_c2_state = index + 2
                    zc2_state = re.findall(r'"([^"]*)"', s[index_c2_state])
                    s[index_c2_state] = zc2_state[1]
                    print("index_c2_state", s[index_c2_state])
                    index_c2_type = index + 3
                    zc2_type = re.findall(r'"([^"]*)"', s[index_c2_type])
                    s[index_c2_type] = zc2_type[1]
                    print("index_c2_type", s[index_c2_type])
                    SC2=[s[index],s[index_c2_id],s[index_c2_state],s[index_c2_type],s[-1]]
                    print("SC2",SC2)
                    try:
                        connection = mysql.connector.connect(
                            host='192.168.0.106',
                            database='fischertechnik',
                            username='pi',
                            password='pi')
                        cursor = connection.cursor()
                        # sql = "DELETE FROM Stock"

                        sql = "INSERT INTO Stock (location, idWorkpiece,state1,type1,time_stamp) VALUES (%s, %s, %s, %s, %s)"

                        cursor.execute(sql, SC2)
                        # cursor.execute(sql)

                        connection.commit()
                        print(cursor.rowcount, "Record inserted successfully into Laptop table")
                        cursor.close()

                    except mysql.connector.Error as error:
                        print("Failed to insert record into Laptop table {}".format(error))
                if s[index]=="C3":
                    print("C3",s[index])
                    index_c3_id = index + 1
                    zc3 = re.findall(r'"([^"]*)"', s[index_c3_id])
                    s[index_c3_id] = zc3[2]
                    print("indexc3", s[index_c3_id])
                    index_c3_state = index + 2
                    zc3_state = re.findall(r'"([^"]*)"', s[index_c3_state])
                    s[index_c3_state] = zc3_state[1]
                    print("index_c3_state", s[index_c3_state])
                    index_c3_type = index + 3
                    zc3_type = re.findall(r'"([^"]*)"', s[index_c3_type])
                    s[index_c3_type] = zc3_type[1]
                    print("index_c3_type", s[index_c3_type])
                    SC3=[s[index],s[index_c3_id],s[index_c3_state],s[index_c3_type],s[-1]]
                    print("SC3",SC3)
                    try:
                        connection = mysql.connector.connect(
                            host='192.168.0.106',
                            database='fischertechnik',
                            username='pi',
                            password='pi')
                        cursor = connection.cursor()
                        # sql = "DELETE FROM Stock"

                        sql = "INSERT INTO Stock (location, idWorkpiece,state1,type1,time_stamp) VALUES (%s, %s, %s, %s, %s)"

                        cursor.execute(sql, SC3)
                        # cursor.execute(sql)

                        connection.commit()
                        print(cursor.rowcount, "Record inserted successfully into Laptop table")
                        cursor.close()

                    except mysql.connector.Error as error:
                        print("Failed to insert record into Laptop table {}".format(error))
                if s[index]=="location" :
                    s[index]=z[2]
                    print("zzz", s[index])
                    index_a1_id = index + 1
                    za1 = re.findall(r'"([^"]*)"', s[index_a1_id])
                    s[index_a1_id] = za1[2]
                    print("indexa2", s[index_a1_id])
                    index_a1_state = index + 2
                    za1_state = re.findall(r'"([^"]*)"', s[index_a1_state])
                    s[index_a1_state] = za1_state[1]
                    print("index_a1_state", s[index_a1_state])
                    index_a1_type = index + 3
                    za1_type = re.findall(r'"([^"]*)"', s[index_a1_type])
                    s[index_a1_type] = za1_type[1]
                    print("index_a1_type", s[index_a1_type])
                    SA1=[s[index],s[index_a1_id],s[index_a1_state],s[index_a1_type],s[-1]]
                    print("SA1",SA1)
                    try:
                        connection = mysql.connector.connect(
                            host='192.168.0.106',
                            database='fischertechnik',
                            username='pi',
                            password='pi')
                        cursor = connection.cursor()
                        # sql = "DELETE FROM Stock"

                        sql = "INSERT INTO Stock (location, idWorkpiece,state1,type1,time_stamp) VALUES (%s, %s, %s, %s, %s)"

                        cursor.execute(sql, SA1)
                        # cursor.execute(sql)

                        connection.commit()
                        print(cursor.rowcount, "Record inserted successfully into Laptop table")
                        cursor.close()

                    except mysql.connector.Error as error:
                        print("Failed to insert record into Laptop table {}".format(error))





            print(str(msg.payload))
            ok = mqttClient2.publish("hnu/factory1/f/i/stock", msg.payload)

        if msg.topic.startswith("f/i/order"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("f/i/order",s)
            s = s.replace('{', '')
            s = s.replace('}', '')

            s = re.split(r"\s*[,;]\s*", s.strip())
            z = re.findall(r'"([^"]*)"', s[0])
            s[0] = z[1]
            print("state",s[0])

            s[1] = s[1].replace('T', ' ')
            s[1] = s[1].replace('Z', '')
            z = re.findall(r'"([^"]*)"', s[1])
            s[1] = z[1]
            print("time", s[1])
            z = re.findall(r'"([^"]*)"', s[2])
            s[2] = z[1]

            print("type", s[2])

            print("f/i/order",s)
            s=[s[0],s[1],s[2]]
            try:
                connection = mysql.connector.connect(
                    host='192.168.0.106',
                    database='fischertechnik',
                    username='pi',
                    password='pi')
                cursor = connection.cursor()
                # sql = "DELETE FROM Order1"


                sql = "INSERT INTO Order1 (state1, time_stamp,type1) VALUES (%s, %s, %s)"
                cursor.execute(sql, s)
                # cursor.execute(sql)

                connection.commit()
                print(cursor.rowcount, "Record inserted successfully into Laptop table")
                cursor.close()

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            print(str(msg.payload))
            ok = mqttClient2.publish("f/o/order", str(msg.payload)) #json format #f/o/order

            print("okk",ok)
            print("_"*60)

        if msg.topic.startswith("f/i/nfc/ds"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("f/nfc/ds",s)
            print(str(msg.payload))
            # ok = mqttClient2.publish("hnu/factory1/f/i/nfc/ds", msg.payload)
            print("_"*60)



    except:
        print("Fehler")
    
#Reseive message and send with latence a response 2
def on_message2(mosq, obj, msg):
    try: 
        if msg.topic.startswith("hnu/factory1/i/bme680"): 
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')

            print("hnu/factory1/i/bme680",s)
            print(str(msg.payload))
            msg.topic = "hnu/factory1/i/bme680"
            ok = mqttClient2.publish(msg.topic, msg.payload)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/i/ldr"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/i/ldr",s)
            print(str(msg.payload))
            topic1 = "hnu/factory1/i/ldr"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/i/cam"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            # print("hnu/factory1/i/cam",s)
            # print(str(msg.payload))
            topic1 = "hnu/factory1/i/cam"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/i/ptu/pos"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/i/cam",s)
            print(str(msg.payload))
            topic1 = "hnu/factory1/i/ptu/pos"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/i/alert"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/i/alert",s)
            print(str(msg.payload))
            topic1 = "hnu/factory1/i/alert"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/f/i/state/hbw"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/f/i/state/hbw",s)
            print(str(msg.payload))
            topic1 = "hnu/factory1/f/i/state/hbw"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/f/i/state/vgr"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/f/i/state/vgr",s)
            print(str(msg.payload))
            topic1 = "hnu/factory1/f/i/state/vgr"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("vgr_ok",ok)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/f/i/state/mpo" ):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/f/i/state/mpo",s)
            print(str(msg.payload))
            topic1 = "hnu/factory1/f/i/state/mpo"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/f/i/state/sld"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/f/i/state/sld",s)
            print(str(msg.payload))
            topic1 = "hnu/factory1/f/i/state/sld"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/f/i/state/dsi"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/f/i/state/dsi",s)
            print(str(msg.payload))
            topic1 = "hnu/factory1/f/i/state/dsi"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/f/i/state/dso"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/f/i/state/dsi",s)
            print(str(msg.payload))
            topic1 = "hnu/factory1/f/i/state/dso"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/f/i/stock"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/f/i/stock",s)
            print(str(msg.payload))
            topic1 = "hnu/factory1/f/i/stock"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/f/i/order" ):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/f/i/order",s)
            print(str(msg.payload))
            topic1 = "hnu/factory1/f/i/order"
            ok = mqttClient2.publish(topic1, msg.payload)
            print("oderok",ok)
            print("_"*60)

        if msg.topic.startswith("hnu/factory1/f/i/nfc/ds"):
            s = str(msg.payload).replace('b', '')
            s = s.replace('\'', '')
            print("hnu/factory1/f/i/nfc/ds",s)
            print(str(msg.payload))
            ok = mqttClient2.publish("hnu/factory1/f/i/nfc/ds", msg.payload)
            print("_"*60)

    except:
        print("Fehler")

#Main-Program
print("START")
mqttClient = mqtt.Client()
mqttClient.on_connect = on_connect
mqttClient.on_message = on_message

mqttClient2 = mqtt.Client()
mqttClient2.on_connect = on_connect2
mqttClient2.on_message = on_message2



process_connect2Server()
process_connect2Server2()
counter = 0
while True:
    time.sleep(2)


# client.publish("topic/test", "ts":"YYYY-MM-DDThh:mm:ss.fffZ", "type":"BLUE");

