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

    def __init__(self, ip, port, topics=[]):
        self.ip = ip 
        self.port = port
        self.topics = topics # Unused for now
        self.subscribers = dict() # Mapping of topics to subscriber addresses
        self.sub_addrs = dict() # Mapping of subscriber IDs to addresses (IP/port)
        # Listen for connections
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.ip, self.port))
        self.sock.listen(cps.MAX_CONN)


    def start(self):

        while True:
            print 'waiting for connections..'
            conn_sock, (sender_ip, _) = self.sock.accept()
            print 'got a connection from %s' % sender_ip

            print 'receiving message...'
            received = conn_sock.recv(cps.RECV_BUFSIZ)
            print 'got something. message is:\n%s\n' % received

            msg = dict()
            try:
                msg = json.loads(received)
                print 'message is good: ', msg

                reply_msg = self.handle_msg(msg)
            except TypeError:
                # Ignore this
                print 'got bad JSON! Ignoring...'
            except BadPubSubMsg:
                # Ignore this too
                print 'got a malformed message! ignoring...'



    def forward_event(self, pub_msg):
        '''
        Forward published events to subscribers
        '''

        print 'going to forward publshed event...'
        for t in pub_msg.topics:
            print 'for topic %s...' % t
            sub_ips = self.subscribers[t]
            print '...we have subscribers' , sub_ips

            for ip in sub_ips:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                print 'attempting to connect to ', ip
                s.connect((ip, SUB_PORT))
                print 'connected!'

                print 'forwarding event...'
                s.send(EventMsg(t, pub_msg.msg).to_json())
                print 'forwared!'
    

    def handle_msg(self, msg, sender_ip):
        '''
        Handle a publish or subscribe message.
        
        :param msg - An unpacked JSON (dict) message
        '''
        
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
