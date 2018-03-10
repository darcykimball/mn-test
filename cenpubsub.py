#!/usr/bin/python


# Centralized pub/sub


CPUBSUB_PORT = 8888


# Message types

class SubscribeMsg(topics):
    '''
    A subscription request message
    '''
    pass


def PublishMsg(topic, msg):
    '''
    A publish message
    '''
    pass

    
def EventMsg(topic, msg):
    '''
    An event (i.e. forwarded published msg) message
    '''
    pass
