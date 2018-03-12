#!/usr/bin/python


import argparse
import socket
import sys

import cenpubsub as cps


class Subscriber:
    '''
    Subscriber functionality
    '''


    def __init__(self, ip, broker_ip, port, topics, recv_action=sys.stdout.write):
        self.ip = ip
        self.broker_ip = broker_ip
        self.broker_port = port
        self.topics = topics
        self.recv_action = recv_action

        # Setup socket for receiving events
        self.event_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.event_sock.bind((ip, cps.SUB_PORT))
        self.event_sock.listen(5)


    def subscribe(self):
        '''
        Try to join a pub/sub network
        '''
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.broker_ip, self.broker_port))

        sub_msg = cps.SubscribeMsg(self.topics)
        s.send(sub_msg.to_json())


    def start(self):
        # Try to join as a subscriber
        self.subscribe()

        # Wait for messages
        while True:
            conn_sock = self.event_sock.accept()
            event = self.conn_sock.recv(cps.RECV_BUFSIZ)

            try:
                msg = json.loads(event)
                self.recv_action(msg)
            except KeyError, TypeError:
                print 'Got bogus message, ignoring...'
                continue

    
    class UnableToJoinError(Exception):
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Subscribe to topics')
    parser.add_argument('my_ip', type=str)
    parser.add_argument('broker_ip', type=str)
    parser.add_argument('port', type=int)
    parser.add_argument('topics', type=str, nargs='+')

    args = parser.parse_args()


    # Run as a listening subscriber
    sub = Subscriber(args.my_ip, args.broker_ip, args.port, args.topics)


    print 'Starting subscriber %s' % sub.ip
    sub.start()
