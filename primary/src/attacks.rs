use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;

pub const ATTACKING_INTERVAL: u64 = 7;

pub const SPECULATIVE_MAX_TRIES: usize = 50;
pub const SLUGGISH_DELAY_MULTI_FACTOR: u64 = 2;

pub const HONEST_NODE: u32 = 0;
pub const FISSURE_ATTACK: u32 = 1; // the delegate of fissure arbitragers
pub const FISSURE_NOT_ACTIVE_ATTACK: u32 = 2;
pub const SLUGGISH_ATTACK: u32 = 3;
pub const SLUGGISH_NOT_ACTIVE_ATTACK: u32 = 4;
pub const SPECULATIVE_ATTACK: u32 = 5; // the delegate of speculative arbitragers
pub const SPECULATIVE_NOT_DELEGATE_ATTACK: u32 = 6;
pub const MONITOR_NOTHING: u32 = 10; // print monitor information but not attack, this is used for comparing the attack with non-attack

#[derive(Clone)]
pub struct Attacker {
    /// committee
    committee: Committee,
    /// The attacking type
    attack_type: u32,
    /// The size of primary block when launching the speculative front-running attack
    speculative_try_times: usize,
}

impl Attacker {
    pub fn new(committee: Committee, attack_type: u32, speculative_try_times: usize) -> Self {
        Self {
            committee,
            attack_type,
            speculative_try_times,
        }
    }
    pub fn attack_type(&self) -> u32 {
        self.attack_type
    }
    pub fn speculative_try_times(&self) -> usize {
        self.speculative_try_times
    }

    /// Returns the victims' addresses
    pub fn victims(&self) -> Vec<PublicKey> {
        self.committee
            .authorities
            .iter()
            .filter(|(_, authority)| authority.attack_type == 0)
            .map(|(name, _)| *name)
            .collect()
    }

    /// currently, we only front-run a node
    pub fn is_target_victim(&self, node: PublicKey) -> bool {
        if !self.victims().is_empty() {
            return self.victims()[0] == node;
        }
        false
    }

    /// Returns the delegate of speculative arbitragers
    pub fn target_victim(&self) -> Option<PublicKey> {
        if !self.victims().is_empty() {
            return Some(self.victims()[0]);
        }
        None
    }

    // Pick-minimum attack: calculate a header id (digest)
    pub fn get_header_id(
        &self,
        author: PublicKey,
        round: u64,
        ele_index: usize,
        payload: &Vec<(Digest, WorkerId)>,
        parents: &BTreeSet<Digest>,
    ) -> Digest {
        let btree_map: BTreeMap<Digest, WorkerId> =
            payload.iter().take(ele_index + 1).cloned().collect();
        let mut hasher = Sha512::new();
        hasher.update(&author);
        hasher.update(round.to_le_bytes());
        for (x, y) in &btree_map {
            hasher.update(x);
            hasher.update(y.to_le_bytes());
        }
        for x in parents {
            hasher.update(x);
        }
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }

    // Pick-minimum attack: calculate a the digest of a certificate
    // this digest will be used for local order in the next header
    pub fn get_certificate_digest(
        &self,
        author: PublicKey,
        round: u64,
        header_id: &Digest,
    ) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&header_id);
        hasher.update(round.to_le_bytes());
        hasher.update(&author);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}
