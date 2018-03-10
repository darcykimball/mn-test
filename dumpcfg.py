#!/usr/bin/python


from mininet.net import Mininet
from mininet.topo import Topo
from mininet.link import TCLink
from mininet.log import setLogLevel


class StarTopo(Topo):
    '''
    A simple star topology
    '''

    def build(self, n=3):
        switch = self.addSwitch('s1')
        
        for h in xrange(n):
            host = self.addHost('h%s' % (h + 1))
            self.addLink(host, switch)


def dumpConfigs(net):
    '''
    Run ifconfig on each host
    '''

    for hostName, host in net.items():
        ifcfgOut = host.cmd('ifconfig')
        print hostName + ':'
        print ifcfgOut


def testStar():
    '''
    Test dumpConfigs() on a star topology
    '''

    topo = StarTopo(n=5)
    net = Mininet(topo)
    net.start()
    print 'Here we go...'
    dumpConfigs(net)
    print 'Alright, done.'

    net.pingAll()
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    testStar()

    pass
