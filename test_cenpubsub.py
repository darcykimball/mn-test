#!/usr/bin/python


import argparse
import os
import time

from mininet.net import Mininet
from mininet.topo import Topo
from mininet.link import TCLink
from mininet.log import setLogLevel


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

    # Setup 1 broker and 1 publisher on first 2 nodes.
    broker_node = net.hosts[0]
    print 'mn: starting broker..'
    broker_node.sendCmd('%sbroker.py %s %s > %s 2>%s' %
            (script_dir, broker_node.IP(), BROKER_PORT, log_dir + 'broker.out', log_dir + 'broker.err'))
    time.sleep(1)

    # Setup subscribers
    print 'mn: starting hosts...'
    for host in net.hosts[2:]:
        host.sendCmd('%ssubscriber.py %s %s %s bell > %s 2>%s' % (script_dir, host.IP(), broker_node.IP(), BROKER_PORT, log_dir + host.name + '.out', log_dir + host.name + '.err'))
    
    time.sleep(1)

    
    pub_node = net.hosts[1]
    print 'mn: starting publisher'
    pub_node.cmd('%spublisher.py %s %s > %s 2>%s' % (script_dir, broker_node.IP(), BROKER_PORT, log_dir + 'pub.out', log_dir + 'pub.err'))

    # Wait for stuff to happen
    time.sleep(n)

    
    broker_node.cmd('pkill broker.py') # FIXME robust?
    for host in net.hosts[2:]:
        host.cmd('pkill subscriber.py') # FIXME robust?

    # FIXME: pub_node is hopefully dead already?
    pub_node.cmd('pkill publisher.py')

    net.stop()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A small test')
    parser.add_argument('script_dir', type=str)
    parser.add_argument('log_dir', type=str)

    args = parser.parse_args()


    # FIXME clean up
    script_dir = os.path.abspath(args.script_dir) + '/'
    log_dir = os.path.abspath(args.log_dir) + '/'

    print script_dir
    print log_dir

    test_cenpubsub(script_dir, log_dir)
