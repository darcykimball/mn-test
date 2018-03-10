#!/usr/bin/python


import json


# Centralized pub/sub


# Some constants

CPUBSUB_PORT = 8888

SUB_MSG_KEY = 'SUB'
PUB_MSG_KEY = 'PUB'
EVENT_MSG_KEY = 'EVENT'

NO_SUB_ID = -1 # Special id value for new subscription requests

ID_KEY = 'ID'
TOPICS_KEY = 'TOPICS'



# Message types
# Every message is just JSON



class SubscribeMsg():
    '''
    A subscription request message.

    This serves as both a 'join' request and/or a subscribe one.
    '''

    
    def __init__(self, topics, sub_id=NO_SUB_ID):
        assert len(topics) > 0
        self.sub_id = sub_id    
        self.topics = topics

    
    def to_json(self):
        d = {
            SUB_MSG_KEY: {
                ID_KEY: self.sub_id,
                TOPICS_KEY: self.topics
            }
        }
        
        return json.dumps(d, indent=4)


class PublishMsg():
    '''
    A publish message
    '''

    
    def __init__(self, topic, msg, pub_id):
        self.pub_id = pub_id
        self.topics = [topic]
        self.msg = msg

    
    def to_json(self):
        d = {
            SUB_MSG_KEY: {
                ID_KEY: self.pub_id,
                TOPICS_KEY: self.topics
            }
        }

        return json.dumps(d, indent=4)


class EventMsg():
    '''
    An event (i.e. forwarded published msg) message
    '''
    

    def __init__(self, topic, msg, broker_id):
        self.broker_id = broker_id
        self.topics = [topic]
        self.msg = msg

    
    def to_json(self):
        d = {
            EVENT_MSG_KEY: {
                ID_KEY: self.broker_id,
                TOPICS_KEY: self.topics
            }
        }

        return json.dumps(d, indent=4)
