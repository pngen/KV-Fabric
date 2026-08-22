//! Reusable-prefix structure: a per-(namespace, compatibility) hashed trie.
//!
//! A reusable prefix is only reused when it is *provably* compatible and in the
//! same namespace, so every trie is keyed by both. Committed nodes are
//! immutable; a new branch only ever *adds* children. This guarantees that one
//! user or session can never mutate reusable state belonging to another active
//! branch, and that reuse preserves immutability / copy-on-write semantics.
//!
//! Supports exact-prefix and longest-prefix lookup, parent/child relationships,
//! branching conversations and shared system prompts.

use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::error::{KvError, KvResult};
use crate::id::{Generation, ObjectId, PrefixId, Token};

/// A committed reusable prefix at a trie node. Multiple objects may claim the
/// same token sequence (different sessions recomputed it); the highest
/// generation is the freshest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedPrefix {
    pub object_id: ObjectId,
    pub generation: Generation,
    pub committed_epoch: u64,
}

/// A single trie node.
#[derive(Debug, Clone)]
struct Node {
    id: PrefixId,
    /// Token that leads from parent to this node; the root uses u32::MAX.
    edge: Token,
    parent: Option<PrefixId>,
    children: BTreeMap<Token, PrefixId>,
    committed: Vec<CommittedPrefix>,
}

impl Node {
    fn root(id: PrefixId) -> Node {
        Node {
            id,
            edge: u32::MAX,
            parent: None,
            children: BTreeMap::new(),
            committed: Vec::new(),
        }
    }
    fn child(id: PrefixId, edge: Token, parent: PrefixId) -> Node {
        Node {
            id,
            edge,
            parent: Some(parent),
            children: BTreeMap::new(),
            committed: Vec::new(),
        }
    }
}

/// An immutable trie over tokens for one (namespace, compatibility) group.
#[derive(Debug)]
struct Trie {
    nodes: Vec<Node>,
    root: PrefixId,
}

impl Default for Trie {
    fn default() -> Self {
        Trie::new()
    }
}

impl Trie {
    fn new() -> Trie {
        let root = PrefixId(1);
        Trie {
            nodes: vec![Node::root(root)],
            root,
        }
    }

    fn node(&self, id: PrefixId) -> Option<&Node> {
        let idx = self.idx(id)?;
        self.nodes.get(idx)
    }
    fn node_mut(&mut self, id: PrefixId) -> Option<&mut Node> {
        let idx = self.idx(id)?;
        self.nodes.get_mut(idx)
    }
    fn idx(&self, id: PrefixId) -> Option<usize> {
        // Nodes are stored in insertion order; PrefixId equals index+1.
        let n = id.to_u64() as usize;
        if n >= 1 && n <= self.nodes.len() {
            Some(n - 1)
        } else {
            None
        }
    }
}

/// The shared prefix index map key.
pub type IndexKey = (String, [u8; 32]);

/// Inspectable node information (parent/child relationships, edge token and
/// committed prefixes). Public so that lineage and reuse can be audited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixNodeInfo {
    pub id: PrefixId,
    pub parent: Option<PrefixId>,
    pub edge_token: Option<Token>,
    pub children: Vec<Token>,
    pub committed: Vec<CommittedPrefix>,
}

/// A successful longest-prefix match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixMatch {
    pub prefix_id: PrefixId,
    pub token_len: usize,
    pub candidates: Vec<CommittedPrefix>,
}

/// A reusable-prefix structure keyed by namespace and compatibility identity.
#[derive(Default)]
pub struct PrefixStore {
    indexes: RwLock<BTreeMap<IndexKey, Trie>>,
}

impl PrefixStore {
    pub fn new() -> PrefixStore {
        PrefixStore::default()
    }

    /// Flatten the namespace+compat key.
    fn key(ns: &str, compat: &[u8; 32]) -> IndexKey {
        (ns.to_string(), *compat)
    }

    /// Commit a token sequence as a reusable prefix. Idempotent for the same
    /// object id; appends other producers as additional candidates.
    pub fn commit(
        &self,
        ns: &str,
        compat: &[u8; 32],
        tokens: &[Token],
        object_id: ObjectId,
        generation: Generation,
        epoch: u64,
    ) -> KvResult<PrefixId> {
        if tokens.len() > crate::bytes::MAX_TOKENS {
            return Err(KvError::invalid_arg("token sequence too long"));
        }
        let key = Self::key(ns, compat);
        let mut map = self.indexes.write().expect("prefix lock poisoned");
        let trie = map.entry(key).or_insert_with(Trie::new);
        let mut current = trie.root;
        for &tok in tokens {
            let next = {
                let node = trie.node(current).ok_or_else(|| KvError::internal("dangling node"))?;
                node.children.get(&tok).copied()
            };
            if let Some(next) = next {
                current = next;
            } else {
                let new_id = PrefixId(trie.nodes.len() as u64 + 1);
                trie.nodes.push(Node::child(new_id, tok, current));
                let c = trie
                    .node_mut(current)
                    .ok_or_else(|| KvError::internal("dangling node"))?;
                c.children.insert(tok, new_id);
                current = new_id;
            }
        }
        // Record the committed prefix at the terminal node.
        let node = trie
            .node_mut(current)
            .ok_or_else(|| KvError::internal("dangling terminal node"))?;
        if !node.committed.iter().any(|c| c.object_id == object_id) {
            node.committed.push(CommittedPrefix {
                object_id,
                generation,
                committed_epoch: epoch,
            });
        } else {
            // Refresh generation to the maximum seen.
            for c in node.committed.iter_mut() {
                if c.object_id == object_id && c.generation < generation {
                    c.generation = generation;
                    c.committed_epoch = epoch;
                }
            }
        }
        Ok(current)
    }

    /// Exact-prefix lookup: the committed sequence must consume all tokens.
    pub fn exact(&self, ns: &str, compat: &[u8; 32], tokens: &[Token]) -> Option<PrefixMatch> {
        let map = self.indexes.read().ok()?;
        let trie = map.get(&Self::key(ns, compat))?;
        let mut current = trie.root;
        for &tok in tokens {
            let node = trie.node(current)?;
            current = *node.children.get(&tok)?;
        }
        let node = trie.node(current)?;
        if node.committed.is_empty() {
            return None;
        }
        Some(PrefixMatch {
            prefix_id: current,
            token_len: tokens.len(),
            candidates: node.committed.clone(),
        })
    }

    /// Longest reusable-prefix lookup.
    pub fn longest_prefix(&self, ns: &str, compat: &[u8; 32], tokens: &[Token]) -> Option<PrefixMatch> {
        let map = self.indexes.read().ok()?;
        let trie = map.get(&Self::key(ns, compat))?;
        let mut current = trie.root;
        let mut best: Option<PrefixMatch> = None;
        let mut depth = 0usize;
        for &tok in tokens {
            let node = trie.node(current)?;
            let next = match node.children.get(&tok) {
                Some(n) => *n,
                None => break,
            };
            current = next;
            depth += 1;
            let node = trie.node(current)?;
            if !node.committed.is_empty() {
                best = Some(PrefixMatch {
                    prefix_id: current,
                    token_len: depth,
                    candidates: node.committed.clone(),
                });
            }
        }
        best
    }

    /// Inspect a specific prefix node. Returns None if unknown (ns/compat/id).
    pub fn node_info(&self, ns: &str, compat: &[u8; 32], prefix_id: PrefixId) -> Option<PrefixNodeInfo> {
        let map = self.indexes.read().ok()?;
        let trie = map.get(&Self::key(ns, compat))?;
        let node = trie.node(prefix_id)?;
        Some(PrefixNodeInfo {
            id: node.id,
            parent: node.parent,
            edge_token: if node.parent.is_some() { Some(node.edge) } else { None },
            children: node.children.keys().copied().collect(),
            committed: node.committed.clone(),
        })
    }

    /// Number of distinct prefix (namespace, compat) groups in the store.
    pub fn groups(&self) -> usize {
        self.indexes.read().map(|m| m.len()).unwrap_or(0)
    }

    /// Total committed prefix nodes across all groups.
    pub fn total_nodes(&self) -> usize {
        self.indexes
            .read()
            .map(|m| m.values().map(|t| t.nodes.len()).sum())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::ObjectId;

    const C: [u8; 32] = [7u8; 32];
    const C2: [u8; 32] = [8u8; 32];

    #[test]
    fn exact_and_longest() {
        let s = PrefixStore::new();
        let ns = "ns";
        let seq: Vec<Token> = vec![1, 2, 3, 4, 5];
        let oid = ObjectId::from_u128(0xabc);
        s.commit(ns, &C, &seq, oid, Generation::one(), 0).unwrap();

        let m = s.exact(ns, &C, &seq).unwrap();
        assert_eq!(m.token_len, 5);
        assert_eq!(m.candidates[0].object_id, oid);

        // Longest prefix of a longer sequence.
        let longer: Vec<Token> = vec![1, 2, 3, 4, 5, 6, 7];
        let l = s.longest_prefix(ns, &C, &longer).unwrap();
        assert_eq!(l.token_len, 5);

        // No match on a different prefix.
        let other: Vec<Token> = vec![9, 9];
        assert!(s.longest_prefix(ns, &C, &other).is_none());
    }

    #[test]
    fn branching_shares_prefix() {
        let s = PrefixStore::new();
        let ns = "ns";
        let shared: Vec<Token> = vec![1, 2, 3, 4];
        let parent_oid = ObjectId::from_u128(100);
        s.commit(ns, &C, &shared, parent_oid, Generation::one(), 0).unwrap();

        // Branch A extends with 5.
        let a: Vec<Token> = vec![1, 2, 3, 4, 5];
        let a_oid = ObjectId::from_u128(101);
        let a_stem = s.commit(ns, &C, &a, a_oid, Generation::one(), 0).unwrap();

        // Branch B extends with 6 (diverges at the shared prefix node).
        let b: Vec<Token> = vec![1, 2, 3, 4, 6];
        let b_oid = ObjectId::from_u128(102);
        let b_stem = s.commit(ns, &C, &b, b_oid, Generation::one(), 0).unwrap();

        assert_ne!(a_stem, b_stem);

        // Longest prefix of a full B asks only for the B branch, never A.
        let q: Vec<Token> = vec![1, 2, 3, 4, 6, 7];
        let l = s.longest_prefix(ns, &C, &q).unwrap();
        assert_eq!(l.token_len, 5);
        assert_eq!(l.candidates[0].object_id, b_oid);

        // The shared prefix is still reusable by both.
        let sp = s.longest_prefix(ns, &C, &a).unwrap();
        assert_eq!(sp.token_len, 5);
        assert!(sp.candidates.iter().any(|c| c.object_id == a_oid));
    }

    #[test]
    fn namespace_and_compat_isolate() {
        let s = PrefixStore::new();
        let seq: Vec<Token> = vec![1, 2, 3];
        let oid = ObjectId::from_u128(1);
        s.commit("ns-a", &C, &seq, oid, Generation::one(), 0).unwrap();

        // Same tokens, different namespace -> no reuse.
        assert!(s.exact("ns-b", &C, &seq).is_none());
        // Same namespace, different compat -> no reuse.
        assert!(s.exact("ns-a", &C2, &seq).is_none());
        // Same everything -> reuse.
        assert!(s.exact("ns-a", &C, &seq).is_some());
    }

    #[test]
    fn parent_child_relationships_are_inspectable() {
        let s = PrefixStore::new();
        let seq: Vec<Token> = vec![1, 2, 3];
        let oid = ObjectId::from_u128(55);
        let p = s.commit("ns", &C, &seq, oid, Generation::one(), 0).unwrap();
        let info = s.node_info("ns", &C, p).unwrap();
        assert_eq!(info.id, p);
        assert_eq!(info.committed.len(), 1);
        assert_eq!(info.committed[0].object_id, oid);
        assert!(info.parent.is_some(), "non-root node should have a parent");
        let parent = info.parent.unwrap();
        let pinfo = s.node_info("ns", &C, parent).unwrap();
        assert!(pinfo.children.contains(&3), "parent should reference child via edge token");
    }

    #[test]
    fn idempotent_commit_does_not_duplicate() {
        let s = PrefixStore::new();
        let seq: Vec<Token> = vec![1, 2, 3];
        let oid = ObjectId::from_u128(5);
        let p1 = s.commit("ns", &C, &seq, oid, Generation::one(), 0).unwrap();
        let p2 = s.commit("ns", &C, &seq, oid, Generation::one(), 0).unwrap();
        assert_eq!(p1, p2);
        let m = s.exact("ns", &C, &seq).unwrap();
        assert_eq!(m.candidates.len(), 1);
    }

    #[test]
    fn concurrent_reads_are_safe() {
        use std::sync::Arc;
        let s = Arc::new(PrefixStore::new());
        let seq: Vec<Token> = (0..100).collect();
        let oid = ObjectId::from_u128(9);
        s.commit("ns", &C, &seq, oid, Generation::one(), 0).unwrap();

        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = s.clone();
            let q = seq.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    let m = s.longest_prefix("ns", &C, &q).unwrap();
                    assert_eq!(m.token_len, 100);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}
