#!/usr/bin/python


import argparse
import socket
import time

import cenpubsub


class Publisher():
    '''
    Event-publishing functionality
    '''

    
    def __init__(self, broker_ip, port):
        self.broker_ip = broker_ip
        self.port = port


    def publish(self, topic, payload):
        # Setup socket for publishing to broker
        self.pub_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.pub_sock.connect((self.broker_ip, self.port))

        self.pub_sock.send(EventMsg(topic, payload).to_json())


    # For testing
    def _periodic_bell(self, period=1):
        '''
        Publish a bell message every period
        '''

        while True:
            self.publish('bell', 'ring ring ring') 
            time.sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Subscribe to topics')
    parser.add_argument('broker_ip', type=str)
    parser.add_argument('port', type=int)

    args = parser.parse_args()


    # Ghetto bell test
    pub = Publisher(args.broker_ip, args.port)

    pub._periodic_bell()
