#!/usr/bin/python

from mininet.net import Mininet
from mininet.topo import Topo
from mininet.link import TCLink
from mininet.log import setLogLevel


import cenpubsub
import star


def test_cenpubsub():
    '''
    Test centralized pub/sub on a star topology (what else?)
    '''

    topo = StarTopo(n=5)
    net = Mininet(topo)
    net.start()

    # Setup 1 broker and 1 publisher on first 2 nodes.
    for host_name, host in net.items():



if __name__ == '__main__':
    test_cenpubsub()
