import json
import random
from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
from random import randint

producer=KafkaProducer(bootstrap_servers=['citiaps1.diinf.usach.cl:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))


data={
	"who": "Techo para Chile",
	"where": {
		"type": "FeatureCollection",
		"features": [{
			"type": "Feature",
			"properties": {},
			"geometry": {
				"type": "Point",
				"coordinates": [
					-71.61300659179686,
					-33.05298993518391
				]
			}
		}]
	},
	"how-many": 23,
	"what": "Recoletando Viveres",
	"timestamp": 1546437965
}

ONGS=["Techo","Cruz Roja","INJUV","Scouts","Red de Voluntarios de Chile"]
misiones=["recolectar viveres","remover escombros","rescate de animales","atender heridos"]

while True:
	data["who"]=ONGS[randint(0,len(ONGS)-1)]
	data["timestamp"]=int(time.time()*1000)
	data["how-many"]=randint(10,100)
	data["what"]=misiones[randint(0,len(misiones)-1)]
	data["where"]["features"][0]["geometry"]["coordinates"][0]=random.uniform(-71.62227630615234,-71.60717010498047)
	data["where"]["features"][0]["geometry"]["coordinates"][1]=random.uniform(-33.05737819069966,-33.05111946471409)
	producer.send('rimay',value=data)
	sleep(randint(1,5))
	
