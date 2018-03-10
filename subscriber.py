#!/usr/bin/python


import argparse
import socket
import sys

import cenpubsub


class Subscriber:
    '''
    Subscriber functionality
    '''


    def __init__(self, broker_ip, port, topics, recv_action=print):
        self.sub_id = NO_SUB_ID
        self.broker_ip = broker_ip
        self.broker_port = port
        self.topics = topics
        self.recv_action = recv_action


    def join(self):
        '''
        Try to join a pub/sub network
        '''
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.broker_ip, self.broker_port))

        sub_msg = SubscribeMsg(self.sub_id, topics)
        s.send(sub_msg.to_json())

        # See if we were able to join
        s.settimeout(2) # Timeout after waiting a bit
        reply = s.recv()
        s.settimeout(None)

        if len(reply) == 0:
            raise UnableToJoinError('No reply from server')

        # FIXME: exception-safe socket close?


        # FIXME remove
        print 'reply: \n' + reply
        return reply


    def start(self):
        # Try to join as a subscriber
        reply = self.join()
        msg = json.loads(reply)

        assert msg[EVENT_MSG_KEY] is not None
        fields = msg[EVENT_MSG_KEY]

        assert fields[TOPICS_KEY] is not None
        assert fields[MSG_KEY] is not None
        self.recv_action(msg)


    
    class UnableToJoinError(Exception):
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Subscribe to topics')
    parser.add_argument('broker_ip', type=str)
    parser.add_argument('port', type=int)
    parser.add_argument('topics', type=str, nargs='+')

    args = parser.parse_args()


    # Run as a listening subscriber
    sub = Subscriber(args.broker_ip, args.port, args.topics)

    sub.start()
