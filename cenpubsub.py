#!/usr/bin/python


import json


# Centralized pub/sub


# Some constants

BROKER_PORT = 8888
SUB_PORT = 9999

SUB_MSG_KEY = 'SUB'
PUB_MSG_KEY = 'PUB'
EVENT_MSG_KEY = 'EVENT'
PAYLOAD_KEY = 'PAYLOAD'

NO_SUB_ID = -1 # Special id value for new subscription requests

ID_KEY = 'ID'
TOPICS_KEY = 'TOPICS'

MAX_CONN = 5


# Message types
# Every message is just JSON



class SubscribeMsg():
    '''
    A subscription request message.

    This serves as both a 'join' request and/or a subscribe one.
    '''

    
    def __init__(self, topics):
        assert len(topics) > 0
        self.topics = topics

    
    def to_json(self):
        d = {
            SUB_MSG_KEY: {
                TOPICS_KEY: self.topics
            }
        }
        
        return json.dumps(d, indent=4)


class PublishMsg():
    '''
    A publish message
    '''

    
    def __init__(self, topic, msg):
        self.topics = [topic]
        self.msg = msg

    
    def to_json(self):
        d = {
            SUB_MSG_KEY: {
                TOPICS_KEY: self.topics
            }
        }

        return json.dumps(d, indent=4)


class EventMsg():
    '''
    An event (i.e. forwarded published msg) message
    '''
    

    def __init__(self, topic, payload):
        self.topics = [topic]
        self.payload = payload

    
    def to_json(self):
        d = {
            EVENT_MSG_KEY: {
                TOPICS_KEY: self.topics,
                PAYLOAD_KEY: self.payload
            }
        }

        return json.dumps(d, indent=4)
