use crate::config::{Committee};
use crate::consensus::Round;
use crypto::PublicKey;
use std::net::{SocketAddr};
use std::collections::HashMap;

pub type LeaderElector = RRLeaderElector;

pub struct RRLeaderElector {
    committee: Committee,
}

impl RRLeaderElector {
    pub fn new(committee: Committee) -> Self {
        Self { committee }
    }
    pub fn get_leader(&self, round: Round, firewall: Vec<SocketAddr>, dns: HashMap<SocketAddr, SocketAddr>) -> PublicKey {
        let keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        let values: Vec<_> = self.committee.authorities.values().cloned().collect();
        let mut addresses: Vec<_> = values.iter().map(|x| dns[&x.address]).collect();
        addresses.sort();
        let mut keys_order = Vec::new();

        for address in addresses.iter(){
            for key in keys.iter() {
                if dns[&self.committee.address(&key).unwrap()] == *address {
                    keys_order.push(key.clone());
                }
            }
        }

        let mut indices = Vec::new(); 
        for _value in addresses.iter() {
            if firewall.contains(&_value){
                indices.push(false);
            } else {
                indices.push(true);
            }
        }
        let mut iter = indices.iter();
        keys_order.retain(|_| *iter.next().unwrap());
        keys_order[round as usize % (self.committee.size_by_firewall(firewall))]// - 1)]
    }
}
