"""
Floodfill simulation of a single message
"""
import random
import statistics
from simpy import Environment
random.seed(42)
env = Environment()
forwards = [0]


class Message(object):

    def __init__(self, sender, receiver, num_bytes, hops=0, sent_at=0, delay=0):
        self.sender = sender
        self.receiver = receiver
        self.hops = hops
        self.size = num_bytes
        self.sent_at = sent_at
        self.received_at = sent_at + delay
        self.delay = delay

    def forward(self, sender, receiver, delay):
        forwards[0] += 1
        return Message(sender, receiver, self.size, self.hops + 1,
                       sent_at=self.received_at, delay=delay)

    def deliver(self):
        yield env.timeout(self.delay)
        self.receiver.receive(self)

    def __repr__(self):
        return 'Msg(from=%r to=%r received_at=%.2f)' % (self.sender, self.receiver, self.received_at)


class Node(object):
    ul_capacity = 0
    dl_capacity = 0
    base_latency = 0.05
    num_peers = 5

    def __init__(self, name):
        self.name = name
        self.peers = []
        self.msg = None
        self.shortest_path = None

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.name)

    @classmethod
    def delay(cls, other, num_peers, msg_size):
        bw = min(cls.ul_capacity / float(num_peers), other.dl_capacity)  # approximation
        return 8 * msg_size / float(bw) + cls.base_latency

    def broadcast(self, msg):
        peers = [p for p in self.peers if p is not msg.sender]
        for p in peers:
            p_msg = msg.forward(self, p, self.delay(p, len(self.peers), msg.size))
            env.process(p_msg.deliver())

    def receive(self, msg):
        # print self, 'received', env.now, msg
        if not self.shortest_path or msg.hops < self.shortest_path:
            self.shortest_path = msg.hops
        if not self.msg:
            self.msg = msg
            self.broadcast(msg)
        else:
            assert self.msg.received_at <= msg.received_at, msg.received_at


class Network(object):

    def __init__(self, num_nodes):
        self.num_nodes = num_nodes
        self.node_classes = []
        self.nodes = []

    def add_node_class(self, label, fraction, ul_capacity=Node.ul_capacity,
                       dl_capacity=Node.dl_capacity, num_peers=Node.num_peers,
                       base_latency=Node.base_latency
                       ):

        def create(ul, dl, np, bl, name):

            class N(Node):
                ul_capacity = ul  # bits/s
                dl_capacity = dl  # bits/s
                base_latency = bl  # secs
                num_peers = np
            N.__name__ = 'Node' + name
            return N

        klass = create(ul_capacity, dl_capacity, num_peers, base_latency, label)
        self.node_classes.append((klass, fraction))

    def nodes_by_class(self, cls):
        if issubclass(cls, Node):
            return [n for n in self.nodes if isinstance(n, cls)]
        else:
            assert isinstance(cls, str)
            return [n for n in self.nodes if n.__class__.__name__ == 'Node' + label]

    def create(self):
        "setup random network"
        norm = sum(x[1] for x in self.node_classes)
        node_classes = [(k, v / float(norm)) for k, v in self.node_classes]
        assert sum(x[1] for x in node_classes) == 1.

        nodes = []
        for klass, probability in node_classes:
            klass.num_peers = num_peers
            for i in xrange(int(self.num_nodes * probability)):
                nodes.append(klass(i))
        for j in range(self.num_nodes - len(nodes)):  # fill up rounding error
            nodes.append(klass(i + j + 1))
        assert len(nodes) == num_nodes

        # create random connections
        for n in nodes:
            while len(n.peers) < n.num_peers:
                p = random.choice(nodes)
                if p is not n:
                    n.peers.append(p)
        self.nodes = nodes

    def sim_broadcast(self, msg_size=256):
        assert self.nodes
        assert not [n for n in self.nodes if n.msg], 'sim was run already'
        initiator = random.choice(self.nodes)
        initiator.broadcast(Message(initiator, initiator, num_bytes=msg_size))

    def stats(self, nodes=None):
        nodes = nodes or self.nodes
        stats = dict(num_nodes=len(nodes))

        # inbound connections
        inbound_peers = dict()
        for n in nodes:
            for p in n.peers:
                inbound_peers.setdefault(p, []).append(n)
        num_inbounds = [len(p) for p in inbound_peers.values()]
        stats['max_inbound_peers'] = max(num_inbounds)
        stats['min_inbound_peers'] = min(num_inbounds)
        stats['avg_inbound_peers'] = statistics.mean(num_inbounds)

        receivers = [n for n in nodes if n.msg]

        stats['pct_received'] = 100. * len(receivers) / len(nodes)

        shortest_paths = [n.shortest_path for n in receivers]  # of all received messages
        stats['avg_shortest_path'] = statistics.mean(shortest_paths)
        stats['median_shortest_path'] = statistics.median(shortest_paths)
        stats['longest_shortest_path'] = max(shortest_paths)

        hops = [n.msg.hops for n in receivers]  # as received earliest
        stats['avg_quickest_path'] = statistics.mean(hops)
        stats['median_quickest_path'] = statistics.median(hops)
        stats['longest_quickest_path'] = max(hops)

        times = [n.msg.received_at for n in receivers]
        stats['avg_propagation_time'] = statistics.mean(times)
        stats['median_propagation_time'] = statistics.median(times)
        stats['longest_propagation_time'] = max(times)

        return stats


def pprint(d):
    assert isinstance(d, dict)
    m = max(len(k) for k in d)
    for k, v in d.items():
        if isinstance(v, float):
            v = '%.2f' % v
        print '%s%s' % (k.ljust(m + 2), v)

kbps = 1024  # bits per second
mbps = 1024 * kbps
gbps = 1024 * mbps
node_types = [
    dict(label='1gbps', fraction=.02, ul_capacity=gbps, dl_capacity=gbps),
    dict(label='50mbps', fraction=.20, ul_capacity=10 * mbps, dl_capacity=50 * mbps),
    dict(label='16mbps', fraction=.60, ul_capacity=2 * mbps, dl_capacity=16 * mbps),
    dict(label='6mbps', fraction=.10, ul_capacity=1 * mbps, dl_capacity=6 * mbps),
    dict(label='2mbps', fraction=.08, ul_capacity=256 * kbps, dl_capacity=2 * mbps),
]


def do_sim(num_nodes, msg_size, num_peers):
    # create network
    network = Network(num_nodes)
    for nt in node_types:
        network.add_node_class(**nt)
    network.create()
    # run sim
    env._now = 0
    network.sim_broadcast(msg_size=msg_size)
    # process events
    env.run()

    return network.stats()


if __name__ == '__main__':
    num_nodes = 10000

    stats = ('avg_propagation_time', 'median_propagation_time', 'longest_propagation_time',
             'avg_quickest_path', 'avg_shortest_path', 'longest_quickest_path', 'pct_received')
    msg_sizes = [1024 * x for x in [1, 2, 4, 8, 16, 32, 64, 128]]
    peer_nums = (3, 5, 7, 9, 11)

    # msg_sizes = [1024 * x for x in [1, 2]]
    # peer_nums = (3, 5)

    num_samples = 5

    table = []

    for msg_size in msg_sizes:
        row = []
        table.append(row)
        for num_peers in peer_nums:
            print 'simulating', msg_size, num_peers
            pstats = []
            for i in range(num_samples):  # do N sims for each set of params
                s = do_sim(num_nodes, msg_size, num_peers)
                pstats.append(s)
            cell = dict()
            for k in stats:
                cell[k] = statistics.mean(p[k] for p in pstats)
            row.append(cell)

    for key in stats:
        print
        print key
        # write header
        print 'msg_size / num_peers\t' + '\t'.join('%d' % p for p in peer_nums)
        for i, msg_size in enumerate(msg_sizes):
            print '%s\t' % msg_size,
            for j, num_peers in enumerate(peer_nums):
                v = table[i][j][key]
                if isinstance(v, float):
                    v = '%.2f' % v
                print '%s\t' % v,
            print
