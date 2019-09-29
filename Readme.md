# MQTT2AWSS3Bridge

MQTT2AWSS3Bridge is a project to connect a locally running broker to AWS S3 and upload the messages as a simple .txt file. The project was written and tested in Python 3.7.4.

[![Build status](https://ci.appveyor.com/api/projects/status/q54cv976lrsclueu?svg=true)](https://ci.appveyor.com/project/SeppPenner/mqtt2awss3bridge)
[![GitHub issues](https://img.shields.io/github/issues/SeppPenner/MQTT2AWSS3Bridge.svg)](https://github.com/SeppPenner/MQTT2AWSS3Bridge/issues)
[![GitHub forks](https://img.shields.io/github/forks/SeppPenner/MQTT2AWSS3Bridge.svg)](https://github.com/SeppPenner/MQTT2AWSS3Bridge/network)
[![GitHub stars](https://img.shields.io/github/stars/SeppPenner/MQTT2AWSS3Bridge.svg)](https://github.com/SeppPenner/MQTT2AWSS3Bridge/stargazers)
[![GitHub license](https://img.shields.io/badge/license-AGPL-blue.svg)](https://raw.githubusercontent.com/SeppPenner/MQTT2AWSS3Bridge/master/License.txt)
[![Known Vulnerabilities](https://snyk.io/test/github/SeppPenner/MQTT2AWSS3Bridge/badge.svg)](https://snyk.io/test/github/SeppPenner/MQTT2AWSS3Bridge) 

## Adjust your settings:

* Adjust the broker to the address you want to use: `broker_source`
* Add your custom filters to `filterMessage()` if you want to filter messages
* Adjust your credentials (uncomment if anonymous): 

```python
client_source.username_pw_set("mqtt", "IoT")
```

* Install the AWS command line: https://docs.aws.amazon.com/cli/latest/userguide/installing.html

* Adjust your AWS S3 credentials and the proxy settings:

```python
os.environ['HTTPS_PROXY'] = 'http://YourProxyIfNeeded:8080'
os.environ['AWS_ACCESS_KEY_ID'] = 'YourKeyHere'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'YourSecretHere'
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'
os.environ['AWS_SESSION_TOKEN'] ='YourSessionTokenHere'
yourAWSBucket = 'YourAWSBucketHere'
```

* Add filters to the bridging like described in the `bridgeFiltering.py` file if needed:

```python
def filterMessage(payload, topic, qos):
	"Filters the messages depending on the configuration for the attributes payload, topic and QoS. 'True' means that the message is not forwarded."
	# Examples below:
	if(payload == "10 %"):
		print('Filtered: payload == "10 %"')
		return True
	if(topic == "humidity" and qos == 0):
		print('Filtered: topic == "humidity" and qos == 0')
		return True
	if(topic == "temperature" or qos == 2):
		print('Filtered: topic == "temperature" or qos == 2')
		return True
	#Add your filters here
```

## Setup on the Raspberry Pi

```bash
sudo apt-get install python3
sudo apt-get install python3-pip
sudo pip3 install paho-mqtt
sudo pip3 install awscli --upgrade --user
```

or

```bash
sudo apt-get install python3
sudo apt-get install python3-pip
sudo pip3 install -r requirements.txt
```

## Running the programms:

```bash
python3 bridge.py
python3 bridgeFiltering.py
```

## Installing the latest version of Python (Currently 3.7.4) on the Raspberry Pi:

https://gist.github.com/SeppPenner/6a5a30ebc8f79936fa136c524417761d

## Paho MQTT client documentation

* https://pypi.org/project/paho-mqtt/
* https://www.hivemq.com/blog/mqtt-client-library-paho-python

## See also

* [MQTT2MQTTBridge](https://github.com/SeppPenner/MQTT2MQTTBridge)
* [MQTT2PostgresBridge](https://github.com/SeppPenner/MQTT2PostgresBridge)
* [MQTT2MySQLBridge](https://github.com/SeppPenner/MQTT2MySQLBridge)

Change history
--------------

* **Version 1.0.0.1 (2019-09-29)** : Updated python version, updated requirements.
* **Version 1.0.0.0 (?)** : 1.0 release.