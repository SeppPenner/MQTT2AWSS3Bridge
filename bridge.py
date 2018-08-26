import paho.mqtt.client as mqtt
import os
import subprocess
import sys
import datetime
import traceback

broker_source = "127.0.0.1"
broker_source_port = 1883
client_source = mqtt.Client("YourClientId")
client_source.username_pw_set("YourUser", "YourPassword")

os.environ['HTTPS_PROXY'] = 'http://YourProxyIfNeeded:8080'
os.environ['AWS_ACCESS_KEY_ID'] = 'YourKeyHere'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'YourSecretHere'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'
os.environ['AWS_SESSION_TOKEN'] ='YourSessionTokenHere'
yourAWSBucket = 'YourAWSBucketHere'

resultsFile = ""

def on_message(client, userdata, message):
	"Evaluated when a new message is received on a subscribed topic"
	print("Received message '" + str(message.payload)[2:][:-1] + "' on topic '"
		+ message.topic + "' with QoS " + str(message.qos))
	writeToResultsFile(message)

def writeToResultsFile(message):
	"Writes to the result file or creates a new one if needed"
	print("Resultsfile is " + resultsFile)
	if(os.stat(resultsFile).st_size >= 50000000): #Splits the file at 50 MB size
		copyFileToS3(resultsFile)
		createNewFile()
	with open(resultsFile, 'a') as f:
		f.write('"' + str(message.payload)[2:][:-1] + '","' + str(message.topic) + '","' + str(message.qos) + '"\n')

def createNewFile():
	"Creates a new results file"
	global resultsFile
	resultsFile = getCurrentDateTimeFormatted() + '.txt'
	with open(resultsFile, 'w') as f:
		f.write('"Payload", "Topic", "QoS"\n')
	print("New file " + resultsFile + " created")

def setup():
	"Runs the setup procedure for the client"
	print("Setting up the onMessage handler")
	client_source.on_message = on_message
	print("Connecting to source")
	client_source.connect(broker_source, broker_source_port)
	client_source.subscribe("#", qos=1)
	print("Setup finished, waiting for messages...")

def copyFileToS3(fileName):
	"Copies the given file to the given Amazon S3 bucket"
	print("Copying file to " + yourAWSBucket + str(fileName.replace("\\", "/")))
	subprocess.call(["aws", "s3", "cp", str(fileName), yourAWSBucket + str(fileName.replace("\\", "/"))])

def getCurrentDateTimeFormatted():
	"Returns a string for the output file with the current date and time"
	return datetime.datetime.now().isoformat().replace(":", "-").replace(".", "-")

try:
	createNewFile()
	setup()
	client_source.loop_forever()
except Exception as e:
	traceback.print_exc()