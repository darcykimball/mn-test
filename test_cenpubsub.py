#!/usr/bin/python


import argparse
import os
import time

from mininet.net import Mininet
from mininet.topo import Topo
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.util import waitListening


from cenpubsub import BROKER_PORT
from star import StarTopo


def test_cenpubsub(script_dir, log_dir, n=5):
    '''
    Test centralized pub/sub on a star topology (what else?)
    '''

    assert n > 2

    topo = StarTopo(n)
    net = Mininet(topo)
    net.start()

    net.pingAll()
    
    broker_prog = os.path.join(script_dir, 'broker.py')
    subscriber_prog = os.path.join(script_dir, 'subscriber.py')
    publisher_prog = os.path.join(script_dir, 'publisher.py')


    # Setup 1 broker and 1 publisher on first 2 nodes.
    broker_node = net.hosts[0]

    print 'mn: starting broker.. with ', broker_prog
    broker_node.cmd('%s %s %s >%s 2>%s &' %
        (broker_prog, broker_node.IP(), BROKER_PORT, \
        os.path.join(log_dir, 'broker.out'), \
        os.path.join(log_dir, 'broker.err')))

    
    # Wait for broker to start
    time.sleep(1)
    #waitListening(server=broker_node, port=BROKER_PORT, timeout=1)

    # Setup subscribers
    print 'mn: starting hosts...'
    for host in net.hosts[2:]:
        host.cmd('%s %s %s %s bell > %s 2>%s &' %
            (subscriber_prog, host.IP(), broker_node.IP(), BROKER_PORT, \
            os.path.join(log_dir, host.name + '.out'), \
            os.path.join(log_dir, host.name + '.err')))
    
    time.sleep(1)

    
    pub_node = net.hosts[1]
    print 'mn: starting publisher'
    pub_node.cmd('%s %s %s > %s 2>%s &' % \
            (publisher_prog, broker_node.IP(), BROKER_PORT, \
            os.path.join(log_dir ,'pub.out'), \
            os.path.join(log_dir, 'pub.err')))

    # Wait for stuff to happen
    time.sleep(n)

    print 'killing broker...'
    broker_node.cmd('pkill broker.py') # FIXME robust?
    print 'broker cleaned up'

    for host in net.hosts[2:]:
        host.cmd('pkill subscriber.py') # FIXME robust?

    # FIXME: pub_node is hopefully dead already?
    print 'kiling publisher...'
    pub_node.cmd('pkill publisher.py')
    print 'publisher cleaned up'

    net.stop()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A small test')
    parser.add_argument('script_dir', type=str)
    parser.add_argument('log_dir', type=str)

    args = parser.parse_args()


    # FIXME clean up
    script_dir = os.path.abspath(args.script_dir)
    log_dir = os.path.abspath(args.log_dir)

    print script_dir
    print log_dir

    test_cenpubsub(script_dir, log_dir)
