#!/usr/bin/python -u


import argparse
import json
import socket
import time

import cenpubsub as cps


class Publisher():
    '''
    Event-publishing functionality
    '''

    
    def __init__(self, broker_ip, broker_port):
        self.broker_ip = broker_ip
        self.broker_port = broker_port


    def publish(self, topic, payload):
        # Setup socket for publishing to broker
        print 'publishing to broker %s:%s' % (self.broker_ip, self.broker_port)
        self.pub_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.pub_sock.connect((self.broker_ip, self.broker_port))

        self.pub_sock.send(cps.PublishMsg(topic, payload).to_json())


    # For testing
    def _periodic_bell(self, n, period=1):
        '''
        Publish a bell message every period, n times
        '''

        print 'preparing to ring...'
        for i in xrange(n):
            try:
                self.publish('bell', 'ring ring ring') 
                time.sleep(1)
            except socket.error as e:
                print 'problem ringing: ', e


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Subscribe to topics')
    parser.add_argument('broker_ip', type=str)
    parser.add_argument('port', type=int)

    args = parser.parse_args()


    # Ghetto bell test
    pub = Publisher(args.broker_ip, args.port)

    pub._periodic_bell(3)
