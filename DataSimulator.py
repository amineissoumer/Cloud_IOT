# Databricks notebook source
from azure.eventhub import EventHubProducerClient, EventData    #Documentation here: https://pypi.org/project/azure-eventhub/#publish-events-to-an-event-hub
from time           import sleep
from datetime       import datetime

import random as random


connection_str ="Endpoint=sb://eventhubshoyen.servicebus.windows.net/;SharedAccessKeyName=SASPolicy_SendToEventHub;SharedAccessKey=5Myf8He0daPULEKLfXSUyy0neia/atBHnLhFKOIcJc0=;EntityPath=channel1"
client = EventHubProducerClient.from_connection_string(connection_str)

#end less loop to send each 5 sec a batch of temperature value
while (True):
  
  event_data_batch = client.create_batch()
  
  #build batch of message in a loop
  for i in range (0,5):
  
    JSON_Message = {"SensorName":       "Sensor" + str(i),
                    "TimeStamp" :       float(datetime.utcnow().timestamp()),
                    "TemperatureValue": float(40.0 * random.uniform(0, 1))}
    print(str(JSON_Message))
    event_data_batch.add(EventData(str(JSON_Message)))  

  # end of loop for   
    
  client.send_batch(event_data_batch)
  print(" ****** data has been sent  ******* ")
  sleep(5.0)

# <  ----    end of loop while (True) ----
