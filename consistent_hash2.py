#!/usr/bin/env python3
"""consistent_hash2 - Consistent hashing ring with virtual nodes."""
import sys, hashlib

class ConsistentHash:
    def __init__(self, nodes=None, replicas=150):
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        self.nodes = set()
        for node in (nodes or []):
            self.add_node(node)
    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    def add_node(self, node):
        self.nodes.add(node)
        for i in range(self.replicas):
            h = self._hash(f"{node}:{i}")
            self.ring[h] = node
            self.sorted_keys.append(h)
        self.sorted_keys.sort()
    def remove_node(self, node):
        self.nodes.discard(node)
        for i in range(self.replicas):
            h = self._hash(f"{node}:{i}")
            del self.ring[h]
            self.sorted_keys.remove(h)
    def get_node(self, key):
        if not self.ring:
            return None
        h = self._hash(key)
        for k in self.sorted_keys:
            if k >= h:
                return self.ring[k]
        return self.ring[self.sorted_keys[0]]
    def get_nodes(self, key, n=3):
        if not self.ring:
            return []
        result = []
        h = self._hash(key)
        idx = 0
        for i, k in enumerate(self.sorted_keys):
            if k >= h:
                idx = i
                break
        seen = set()
        for i in range(len(self.sorted_keys)):
            node = self.ring[self.sorted_keys[(idx + i) % len(self.sorted_keys)]]
            if node not in seen:
                seen.add(node)
                result.append(node)
                if len(result) == n:
                    break
        return result

def test():
    ch = ConsistentHash(["server1", "server2", "server3"], replicas=100)
    # consistent mapping
    node1 = ch.get_node("user:123")
    node2 = ch.get_node("user:123")
    assert node1 == node2
    # distribution
    from collections import Counter
    dist = Counter(ch.get_node(f"key:{i}") for i in range(1000))
    assert len(dist) == 3
    assert all(c > 100 for c in dist.values())  # reasonably balanced
    # adding node: most keys stay
    old_mapping = {f"key:{i}": ch.get_node(f"key:{i}") for i in range(100)}
    ch.add_node("server4")
    moved = sum(1 for k, v in old_mapping.items() if ch.get_node(k) != v)
    assert moved < 50  # less than half should move
    # replicas
    nodes = ch.get_nodes("test", 2)
    assert len(nodes) == 2 and nodes[0] != nodes[1]
    print("OK: consistent_hash2")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test()
    else:
        print("Usage: consistent_hash2.py test")
