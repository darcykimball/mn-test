#!/usr/bin/python -u


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

        print 'broker init'
        self.ip = ip 
        self.port = port
        self.topics = topics # Unused for now
        self.subscribers = dict() # Mapping of topics to subscriber addresses
        self.sub_addrs = dict() # Mapping of subscriber IDs to addresses (IP/port)
        # Listen for connections
        print 'huh'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.ip, self.port))

        print 'uh'
        self.sock.listen(cps.MAX_CONN)


    def start(self):
        print 'waiting for connections..'
        while True:
            print '...'
            conn_sock, (sender_ip, _) = self.sock.accept()
            print 'got a connection from %s' % sender_ip

            print 'receiving message...'
            received = conn_sock.recv(cps.RECV_BUFSIZ)
            print 'got something. message is:\n%s\n' % received

            msg = dict()
            try:
                msg = json.loads(received)
                print 'message is good: ', msg

                self.handle_msg(msg, sender_ip)
            except TypeError as e:
                # Ignore this
                print 'got bad JSON!: ', e
                print 'Ignoring...'
            except cps.BadPubSubMsgError as e:
                # Ignore this too
                print 'got a bad msg: ', e


    def forward_event(self, pub_msg):
        '''
        Forward published events to subscribers
        '''

        print 'going to forward publshed event...'
        for t in pub_msg[cps.TOPICS_KEY]:
            print 'for topic %s...' % t
            sub_ips = self.subscribers.get(t)

            if sub_ips is None:
                print 'no subs for this topic, moving on...'
                continue

            print '...we have subscribers' , sub_ips

            for ip in sub_ips:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                print 'attempting to connect to ', ip
                s.connect((ip, cps.SUB_PORT))
                print 'connected!'

                print 'forwarding event...'
                s.send(cps.EventMsg(t, pub_msg[cps.PAYLOAD_KEY]).to_json())
                print 'forwarded!'
    

    def handle_msg(self, msg, sender_ip):
        '''
        Handle a publish or subscribe message.
        
        :param msg - An unpacked JSON (dict) message
        '''
        
        try:
            if cps.SUB_MSG_KEY in msg:
                # Add a new subscription or update subscriptions
                sub_msg = msg[cps.SUB_MSG_KEY]
                
                # Update and/or add subscriptions
                # FIXME: check set difference of topics?
                for t in sub_msg[cps.TOPICS_KEY]:
                    if self.subscribers.get(t):
                        self.subscribers[t].append(sender_ip)
                    else:
                        self.subscribers[t] = [sender_ip]

            elif cps.PUB_MSG_KEY in msg:
                # Forward to those who are subscribed
                self.forward_event(msg[cps.PUB_MSG_KEY])
            else:
                raise cps.BadPubSubMsgError('Unknown message type!')

        except KeyError as e:
            print e
            raise cps.BadPubSubMsgError('Malformed message!')

    


if __name__ == '__main__':
    #FIXME: remove
    print 'BROKER: WTF'

    parser = argparse.ArgumentParser(description='Broker subscription requests and publish events.')
    parser.add_argument('ip', type=str)
    parser.add_argument('port', type=int)

    args = parser.parse_args()

    # FIXME remove
    print args

    # Run as a listening broker
    broker = Broker(args.ip, args.port)
    print 'Running broker on %s:%s' % (broker.ip, broker.port)
    broker.start()
