"""
Floodfill simulation of a single message
"""
import random
import statistics
from simpy import Environment, Interrupt
import sys
random.seed(42)
env = Environment()
forwards = [0]


class Transfer(object):

    wan_latency = 0.01
    wait = None

    def __init__(self, msg, dl_cnx, ul_cnx, cb):
        self.msg = msg
        self.dl_cnx = dl_cnx
        self.ul_cnx = ul_cnx
        self.cb = cb
        env.process(self.run())

    def __repr__(self):
        return '%s %r' % (self.name, self.wait)

    def run(self):
        self.wait = env.process(self.deliver())
        yield self.wait

    def deliver(self, nbits=None, elapsed=0):
        self.dl_cnx.add_transfer(self)
        self.ul_cnx.add_transfer(self)

        nbits = nbits or self.msg.bit_size
        assert nbits > 0, 'n>0'
        duration = max(self.wan_latency - elapsed, 0)
        bw = min(self.dl_cnx.bandwidth, self.ul_cnx.bandwidth)
        duration += nbits / float(bw)
        # print duration, bw, nbits
        st = env.now
        try:
            yield env.timeout(duration)
        except Interrupt:
            elapsed = env.now - st
            if elapsed < duration:
                rbits = nbits - max(elapsed - self.wan_latency, 0) * bw
                self.wait = env.process(self.deliver(rbits, elapsed))
                return
        # done
        self.dl_cnx.remove_transfer(self)
        self.ul_cnx.remove_transfer(self)
        self.cb(self.msg)

    def update_bandwidth(self):
        self.wait.interrupt(cause='bw update')


class Channel(object):

    def __init__(self, capacity=100):
        self.capacity = capacity
        self.transfers = set()

    @property
    def bandwidth(self):
        return self.capacity / max(1, len(self.transfers))

    def add_transfer(self, t):
        if t in self.transfers:
            return
        self.transfers.add(t)
        for tt in self.transfers:
            if t is not tt:
                tt.update_bandwidth()

    def remove_transfer(self, t):
        if t not in self.transfers:
            return
        self.transfers.remove(t)
        for t in self.transfers:
            t.update_bandwidth()


class Message(object):

    received_at = 0  # timestamp on receive

    def __init__(self, sender, num_bytes, hops=0):
        self.sender = sender
        self.hops = hops
        self.byte_size = num_bytes
        self.bit_size = num_bytes * 8

    def forward(self, sender):
        forwards[0] += 1
        return Message(sender, self.byte_size, self.hops + 1)

    def __repr__(self):
        return 'Msg(hops=%s received_at=%.2f)' % (self.hops, self.received_at)


class Node(object):
    ul_capacity = 0
    dl_capacity = 0
    num_ul_peers = 0

    def __init__(self, name):
        self.name = name
        self.peers = []
        self.in_channel = Channel(self.dl_capacity)
        self.out_channel = Channel(self.ul_capacity)

    def reset(self):
        self.msg = None
        self.shortest_path = None

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.name)

    def p_broadcast(self, msg):
        "parallel uploads"
        assert len(self.peers) >= self.num_ul_peers
        peers = [p for p in self.peers if p is not msg.sender]
        for p in peers[:self.num_ul_peers]:  # forward to num_ul_peers
            p_msg = msg.forward(self)
            Transfer(p_msg, self.out_channel, p.in_channel, p.receive)

    def s_broadcast(self, msg, fastest_first=False):
        "non parallel uploads"
        assert len(self.peers) >= self.num_ul_peers
        peers = [p for p in self.peers if p is not msg.sender][:self.num_ul_peers]

        if fastest_first:
            peers.sort(lambda a, b: cmp(b.out_channel.capacity, a.out_channel.capacity))
            assert peers[0].out_channel.capacity >= peers[-1].out_channel.capacity

        def next_up():
            p = peers.pop(0)
            p_msg = msg.forward(self)
            Transfer(p_msg, self.out_channel, p.in_channel, mkcb(p))

        def mkcb(receiver):
            def cb(msg):
                receiver.receive(msg)
                if peers:
                    next_up()
            return cb

        next_up()

    def s_sorted_broadcast(self, msg):
        return self.s_broadcast(msg, fastest_first=True)

    def timeout_broadcast(self, msg, assumed_min_network_capacity=103537.):
        """
        broadcast to peers (fastest first) until a certain time passed
        """
        peers = sorted(self.peers, lambda a, b: cmp(b.out_channel.capacity, a.out_channel.capacity))
        st = env.now
        broadcast_duration = msg.bit_size / float(assumed_min_network_capacity)

        def next_up():
            p = peers.pop(0)
            p_msg = msg.forward(self)
            Transfer(p_msg, self.out_channel, p.in_channel, mkcb(p))

        def mkcb(receiver):
            def cb(msg):
                receiver.receive(msg)
                if peers:
                    if env.now - st < broadcast_duration:
                        next_up()
            return cb

        next_up()

    broadcast = s_sorted_broadcast

    def receive(self, msg):
        msg.received_at = env.now
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

    def add_node_class(self, label, fraction, ul_capacity=0, dl_capacity=0, num_ul_peers=0):

        def create(ul, dl, np, name):

            class N(Node):
                ul_capacity = ul  # bits/s
                dl_capacity = dl  # bits/s
                num_ul_peers = np
            N.__name__ = 'Node' + name
            return N

        klass = create(ul_capacity, dl_capacity, num_ul_peers, label)
        self.node_classes.append((klass, fraction))

    def set_num_ul_peers(self, num):
        assert self.nodes
        for n in self.nodes:
            n.num_ul_peers = num

    def nodes_by_class(self, cls):
        if issubclass(cls, Node):
            return [n for n in self.nodes if isinstance(n, cls)]
        else:
            assert isinstance(cls, str)
            return [n for n in self.nodes if n.__class__.__name__ == 'Node' + cls]

    def create(self, max_peers):
        "setup random network"
        norm = sum(x[1] for x in self.node_classes)
        node_classes = [(k, v / float(norm)) for k, v in self.node_classes]
        assert sum(x[1] for x in node_classes) == 1.

        # setup nodes
        nodes = []
        for klass, probability in node_classes:
            for i in xrange(int(self.num_nodes * probability)):
                nodes.append(klass(i))
        for j in range(self.num_nodes - len(nodes)):  # fill up rounding error
            nodes.append(klass(i + j + 1))
        assert len(nodes) == num_nodes
        self.nodes = nodes

        # create random connections
        not_paired = 0
        for i in range(max_peers):
            for a, b in zip(nodes, random.sample(nodes, len(nodes))):
                if a == b:
                    not_paired += 1
                else:
                    a.peers.append(b)
                    b.peers.append(a)

        self.initiator = random.choice(self.nodes)
        print 'initiating node', self.initiator

    def sim_broadcast(self, num_bytes=0):
        assert self.nodes
        for n in self.nodes:
            n.reset()
        # process events
        env._now = 0
        self.initiator.broadcast(Message(self.initiator, num_bytes=num_bytes))
        env.run()

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
        stats['avg_shortest_path_len'] = statistics.mean(shortest_paths)
        stats['median_shortest_path_len'] = statistics.median(shortest_paths)
        stats['longest_shortest_path_len'] = max(shortest_paths)

        hops = [n.msg.hops for n in receivers]  # as received earliest
        stats['avg_quickest_path_len'] = statistics.mean(hops)
        stats['median_quickest_path_len'] = statistics.median(hops)
        stats['longest_quickest_path_len'] = max(hops)

        times = [n.msg.received_at for n in receivers]
        stats['avg_propagation_time'] = statistics.mean(times)
        stats['median_propagation_time'] = statistics.median(times)
        stats['longest_propagation_time'] = max(times)

        # capacity bps
        msg_bit_size = receivers[0].msg.bit_size
        stats['avg_capacity'] = int(msg_bit_size / statistics.mean(times))
        stats['min_capacity'] = int(msg_bit_size / max(times))

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

# for nt in node_types:
#     print repr(nt)[1:-1]


def do_sim(num_nodes, num_bytes, num_ul_peers):
    # create network
    network = Network(num_nodes)
    for nt in node_types:
        network.add_node_class(**nt)
    network.create(num_ul_peers + 2)
    network.set_num_ul_peers(num_ul_peers)
    # run sim
    network.sim_broadcast(num_bytes=num_bytes)
    return network.stats()


if __name__ == '__main__':
    num_nodes = 10000
    num_samples = 2
    msg_byte_sizes = [1024 * x for x in [1, 4, 16, 64, 256, 1024]]  # bytes
    ul_peer_count = (5, 10, 15, 20)

    if False:
        num_nodes = 1000
        num_samples = 2
        msg_byte_sizes = [1024 * x for x in [1, 4, 16,  128, 512]]
        ul_peer_count = (5, 7, 11, 15, 25)
    if True:
        num_nodes = 10000
        num_samples = 2
        msg_byte_sizes = [1024 * x for x in [64]]
        ul_peer_count = (10, )

    max_peers = 200

    nsims = num_samples * len(msg_byte_sizes) * len(ul_peer_count)
    print 'running %d sims' % nsims
    # setup networks
    networks = []
    for i in range(num_samples):
        n = Network(num_nodes)
        for nt in node_types:
            n.add_node_class(**nt)
        n.create(max_peers=max_peers)
        networks.append(n)
    print 'created %d networks' % len(networks)

    stats = ['avg_propagation_time', 'median_propagation_time', 'longest_propagation_time',
             'avg_capacity', 'min_capacity', 'avg_quickest_path_len', 'avg_shortest_path_len',
             'longest_quickest_path_len', 'pct_received']

    table = []
    for msg_bytes in msg_byte_sizes:
        row = []
        table.append(row)
        for num_ul_peers in ul_peer_count:
            print 'simulating', msg_bytes, num_ul_peers
            pstats = []
            for i in range(num_samples):  # do N sims for each set of params
                network = networks[i]
                network.set_num_ul_peers(num_ul_peers)
                assert network.nodes[0].num_ul_peers == num_ul_peers
                assert (network.nodes[0].peers) >= num_ul_peers
                network.sim_broadcast(num_bytes=msg_bytes)  # resets env and nodes
                pstats.append(network.stats())
            cell = dict()
            rdevs = []
            for k in stats:
                m = statistics.mean(p[k] for p in pstats)
                s = statistics.stdev(p[k] for p in pstats)
                rdevs.append(s / m)
                if k.startswith('min_'):
                    cell[k] = min(p[k] for p in pstats)
                elif k.startswith('longest_'):
                    cell[k] = max(p[k] for p in pstats)
                else:
                    cell[k] = m
            cell['max_rstd_dev'] = max(rdevs)
            row.append(cell)

    for key in stats + ['max_rstd_dev']:
        print
        print key
        # write header
        print 'msg_size(bytes) / num_ul_peers\t' + '\t'.join('%d' % p for p in ul_peer_count)
        for i, msg_size in enumerate(msg_byte_sizes):
            print '%s\t' % msg_size,
            for j, num_ul_peers in enumerate(ul_peer_count):
                v = table[i][j][key]
                if isinstance(v, float):
                    v = '%.2f' % v
                print '%s\t' % v,
            print

    print 'forwards', forwards

"""
test bw adjusted num_nodes
"""
