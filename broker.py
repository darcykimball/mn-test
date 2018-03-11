#!/usr/bin/python


import argparse
import json
import socket
import sys

import cenpubsub as cps


class Broker:
    '''
    Centralized (singleton) broker functionality
    '''

    RECV_BUFSIZ = 4096


    def __init__(self, ip, port, topics=[]):
        self.ip = ip 
        self.port = port
        self.topics = topics # Unused for now
        self.subscribers = dict() # Mapping of topics to subscriber addresses
        self.sub_addrs = dict() # Mapping of subscriber IDs to addresses (IP/port)
        # Listen for connections
        self.send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.send_sock.bind((self.ip, self.port))
        self.send_sock.listen(cps.MAX_CONN)


    def start(self):

        while True:
            conn_sock = self.send_sock.accept()
            received, sender_addr = conn_sock.recvfrom(RECV_BUFSIZ)
            sender_ip, _ = sender_addr

            msg = dict()
            try:
                msg = json.loads(received)
                reply_msg = self.handle_msg(msg)
            except TypeError:
                # Ignore this
                print 'Got bad JSON! Ignoring...'



    def forward_event(self, pub_msg):
        '''
        Forward published events to subscribers
        '''

        for t in pub_msg.topics:
            sub_ips = self.subscribers[t]
            for ip in sub_ips:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((ip, SUB_PORT))

                s.send(EventMsg(t, pub_msg.msg).to_json())
    

    def handle_msg(self, msg, sender_ip):
        '''
        Handle a publish or subscribe message.
        
        :param msg - An unpacked JSON (dict) message
        :return - A set of reply messages
        '''

        reply_msgs = list()
        
        try:
            if SUB_MSG_KEY in msg:
                # Add a new subscription or update subscriptions
                sub_msg = msg[SUB_MSG_KEY]
                
                # Update and/or add subscriptions
                # FIXME: check set difference of topics?
                for t in sub_msg[TOPICS_KEY]:
                    if self.subscribers.get(t):
                        self.subscribers[t].append(sender_ip)
                    else:
                        self.subscribers[t] = [sender_ip]

            elif PUB_MSG_KEY in msg:
                # Forward to those who are subscribed
                self.forward_event(msg)
            else:
                raise BadPubSubMsgError('Unknown message type!')

        except KeyError:
            raise BadPubSubMsgError('Malformed message!')


        return reply_msgs
    
    class BadPubSubMsgError(Exception):
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Broker subscription requests and publish events.')
    parser.add_argument('ip', type=str)
    parser.add_argument('port', type=int)

    args = parser.parse_args()


    # Run as a listening broker
    broker = Broker(args.ip, args.port)
    print 'Running broker on %s:%s' % (broker.ip, broker.port)
    broker.start()
