#!/usr/bin/env python3
"""consistent_hash2 - Consistent hashing ring with virtual nodes and replication."""
import sys, hashlib, bisect

class ConsistentHash:
    def __init__(self, replicas=100):
        self.replicas = replicas
        self.ring = []  # sorted hashes
        self.hash_to_node = {}
        self.nodes = set()
    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    def add_node(self, node):
        self.nodes.add(node)
        for i in range(self.replicas):
            h = self._hash(f"{node}:{i}")
            bisect.insort(self.ring, h)
            self.hash_to_node[h] = node
    def remove_node(self, node):
        self.nodes.discard(node)
        self.ring = [h for h in self.ring if self.hash_to_node.get(h) != node]
        self.hash_to_node = {h: n for h, n in self.hash_to_node.items() if n != node}
    def get_node(self, key):
        if not self.ring: return None
        h = self._hash(key)
        idx = bisect.bisect_right(self.ring, h) % len(self.ring)
        return self.hash_to_node[self.ring[idx]]
    def get_nodes(self, key, n=3):
        if not self.ring: return []
        h = self._hash(key)
        idx = bisect.bisect_right(self.ring, h) % len(self.ring)
        seen = []
        for i in range(len(self.ring)):
            node = self.hash_to_node[self.ring[(idx + i) % len(self.ring)]]
            if node not in seen:
                seen.append(node)
                if len(seen) == n: break
        return seen

def test():
    ch = ConsistentHash(replicas=50)
    for n in ["node1", "node2", "node3"]:
        ch.add_node(n)
    assignments = {}
    for i in range(100):
        assignments[f"key{i}"] = ch.get_node(f"key{i}")
    assert all(v in {"node1", "node2", "node3"} for v in assignments.values())
    counts = {n: sum(1 for v in assignments.values() if v == n) for n in ["node1", "node2", "node3"]}
    assert all(c > 10 for c in counts.values())  # roughly balanced
    ch.remove_node("node2")
    moved = sum(1 for k, v in assignments.items() if ch.get_node(k) != v)
    assert moved < 80  # most keys stay
    replicas = ch.get_nodes("key1", n=2)
    assert len(replicas) == 2 and replicas[0] != replicas[1]
    print("consistent_hash2: all tests passed")

if __name__ == "__main__":
    test() if "--test" in sys.argv else print("Usage: consistent_hash2.py --test")
