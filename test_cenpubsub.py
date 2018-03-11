#!/usr/bin/python

import time

from mininet.net import Mininet
from mininet.topo import Topo
from mininet.link import TCLink
from mininet.log import setLogLevel


from cenpubsub import BROKER_PORT
from star import StarTopo

LOG_DIR = '/home/vagrant/mnlogs/'


def test_cenpubsub(n=5):
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
    broker_node.cmd('broker.py %s %s > %s 2>%s' %
            (broker_node.IP(), BROKER_PORT, LOG_DIR + 'broker.out', LOG_DIR + 'broker.err'))

    # Setup subscribers
    print 'mn: starting hosts...'
    for host in net.hosts[2:]:
        host.sendCmd('subscriber.py %s %s %s > %s 2>%s' % (host.IP(), broker_node.IP(), BROKER_PORT, LOG_DIR + host.name + '.out', LOG_DIR + host.name + '.err'))

    pub_node = net.hosts[1]
    print 'mn: starting publisher'
    pub_node.cmd('publisher.py %s %s > %s 2>%s' % (broker_node.IP(), BROKER_PORT, LOG_DIR + 'pub.out', LOG_DIR + 'pub.err'))

    # Wait for stuff to happen
    time.sleep(n + 1)

    
    broker_node.cmd('pkill broker.py') # FIXME robust?
    for host in net.hosts[2:]:
        host.cmd('pkill subscriber.py') # FIXME robust?

    # FIXME: pub_node is hopefully dead already?
    pub_node.cmd('pkill publisher.py')

    net.stop()
    

if __name__ == '__main__':
    test_cenpubsub()
