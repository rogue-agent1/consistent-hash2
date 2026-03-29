#!/usr/bin/env python3
"""Consistent hashing ring with virtual nodes."""
import sys, hashlib, bisect

class ConsistentHash:
    def __init__(self, vnodes=150):
        self.vnodes = vnodes
        self.ring = []
        self.ring_map = {}
        self.nodes = set()
    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    def add_node(self, node):
        self.nodes.add(node)
        for i in range(self.vnodes):
            h = self._hash(f"{node}:{i}")
            self.ring_map[h] = node
            bisect.insort(self.ring, h)
    def remove_node(self, node):
        self.nodes.discard(node)
        to_remove = []
        for h, n in list(self.ring_map.items()):
            if n == node:
                to_remove.append(h)
        for h in to_remove:
            del self.ring_map[h]
            self.ring.remove(h)
    def get_node(self, key):
        if not self.ring: return None
        h = self._hash(key)
        idx = bisect.bisect_right(self.ring, h) % len(self.ring)
        return self.ring_map[self.ring[idx]]

def test():
    ch = ConsistentHash(50)
    for n in ["server1", "server2", "server3"]:
        ch.add_node(n)
    assignments = {}
    for i in range(100):
        node = ch.get_node(f"key{i}")
        assignments[f"key{i}"] = node
        assert node in {"server1", "server2", "server3"}
    # Deterministic
    for i in range(100):
        assert ch.get_node(f"key{i}") == assignments[f"key{i}"]
    # Remove a node — most keys stay
    ch.remove_node("server2")
    same = sum(1 for i in range(100) if ch.get_node(f"key{i}") == assignments[f"key{i}"])
    assert same > 50  # majority should stay
    print("  consistent_hash2: ALL TESTS PASSED")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test": test()
    else: print("Consistent hashing ring")
