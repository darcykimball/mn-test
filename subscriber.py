#!/usr/bin/python


import argparse
import socket
import sys


class Subscriber:
    '''
    Subscriber functionality
    '''


    def __init__(self, broker_ip, port, topics, recv_action=print):
        self.broker_ip = broker_ip
        self.broker_port = port
        self.topics = topics
        self.recv_action = recv_action
        self.sock = None


    def start():
        # TODO wrap in try block
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((args.broker_ip, args.port))

    
    def stop():
        


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Subscribe to topics')
    parser.add_argument('broker_ip', type=str)
    parser.add_argument('port', type=int)
    parser.add_argument('topics', type=str, nargs='+')

    args = parser.parse_args()
    print args # FIXME remove


    # Run as a listening subscriber
    sub = Subsc
    riber((args.broker_ip, args.port))


