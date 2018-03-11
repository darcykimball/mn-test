#!/usr/bin/python


import argparse
import json
import socket
import sys

import cenpubsub


class Broker:
    '''
    Centralized (singleton) broker functionality
    '''

    MAX_CONN = 5
    RECV_BUFSIZ = 4096


    def __init__(self, ip, port, topics):
        self.broker_id = 777 # Arbitrary, and unused by any others
        self.ip = ip 
        self.port = port
        self.topics = topics
        self.subscribers = dict() # Mapping of topics to subscriber IDs
        self.sub_addrs = dict() # Mapping of subscriber IDs to addresses (IP/port)


    def start(self):
        # Listen for connections
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.ip, self.port))
        s.listen(MAX_CONN)

        while True:
            conn_sock = s.accept()
            (received, addr) = conn_sock.recvfrom(RECV_BUFSIZ)

            msg = dict()
            try:
                msg = json.loads(received)
            except TypeError:
                # Ignore this
                print 'Got bad JSON!'


        broker_msg = SubscribeMsg(self.broker_id, topics)
        s.send(broker_msg.to_json())


    def handle_msg(self, msg):
        '''
        Handle a publish or subscribe message.
        
        :param msg - An unpacked JSON (dict) message
        :return - A set of reply messages
        '''

        reply_msgs = list()
        
        # TODO/FIXME: totality of key existence checks?
        if SUB_MSG_KEY in msg:
            # Add a new subscription or update subscriptions
            sub_msg = msg[SUB_MSG_KEY]

            if ID_KEY in sub_msg:
                # Already in data base; just update subscriptions
                # FIXME: check set difference?
                self.subscribers[ID_KEY] = msg.[TOPICS_KEY] 

                # TODO

        elif PUB_MSG_KEY in msg:
            # Forward to those who are subscribed
            pub_msg = msg[PUB_MSG_KEY]
            assert len(pub_msg[TOPICS_KEY]) == 1


            topic = pub_msg[TOPICS_KEY][0]
            for sub in self.subscribers[topic]
                reply_msgs.append(EventMsg(self.broker_id, pub_msg.topic, pub_msg.msg).to_json())

        else:
            raise BadPubSubMsgError

        return reply_msgs
    
    class BadPubSubMsgError(Exception):
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Broker subscription requests and publish events.')
    parser.add_argument('ip', type=str)
    parser.add_argument('port', type=int)

    args = parser.parse_args()


    # Run as a listening brokerscriber
    broker = Broker(args.ip, args.port)

    broker.start()
