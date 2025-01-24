use crate::aggregator::Aggregator;
use crate::config::{Committee};
use crate::consensus::{ConsensusMessage, Round, REQUEST_BLOCKS, REQUEST_CLIQUES};
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::{Block, Timeout, Vote, QC, TC, Blocks};
use crate::proposer::ProposerMessage;
use crate::synchronizer::Synchronizer;
use crate::timer::Timer;
use async_recursion::async_recursion;
use bytes::Bytes;
use crypto::Hash as _;
use crypto::{Digest};
use crypto::{PublicKey, SignatureService};
use log::{debug, error, info, warn};
use network::{DelayedSender, ReliableSender};
use std::cmp::max;
use std::collections::{VecDeque, HashMap};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;
use std::convert::{TryFrom, TryInto};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

// global variable to store who we are currently requesting blocks from Vec<PublicKey>
lazy_static! {
    // Stores the sent syncs and the received blocks
    pub static ref SENT_SYNCS: Arc<Mutex<HashMap<PublicKey, Vec<Block>>>> = Arc::new(Mutex::new(HashMap::new()));
    // Stores the tip of the blockchain. This is important to keep track of the received blocks
    // during the recovery protocol
    pub static ref MY_TIP: Arc<Mutex<Digest>> = Arc::new(Mutex::new(Block::default().digest()));
    // Stores the previous chain in case we switch and receive a request for the previous
    // blockchain
    pub static ref PREVIOUS_CHAIN: Arc<Mutex<HashMap<PublicKey, Vec<Block>>>> = Arc::new(Mutex::new(HashMap::new()));
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    store: Store,
    signature_service: SignatureService,
    leader_elector: LeaderElector,
    mempool_driver: MempoolDriver,
    synchronizer: Synchronizer,
    rx_message: Receiver<ConsensusMessage>,
    rx_loopback: Receiver<Block>,
    tx_proposer: Sender<ProposerMessage>,
    tx_commit: Sender<Block>,
    round: Round,
    last_voted_round: Round,
    last_committed_round: Round,
    high_qc: QC,
    timer: Timer,
    aggregator: Aggregator,
    network: DelayedSender,
    alt_tips: Vec<Digest>,
    dns: HashMap<SocketAddr, SocketAddr>,
    id: u64,
    identified_faults: Vec<SocketAddr>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        leader_elector: LeaderElector,
        mempool_driver: MempoolDriver,
        synchronizer: Synchronizer,
        timeout_delay: u64,
        rx_message: Receiver<ConsensusMessage>,
        rx_loopback: Receiver<Block>,
        tx_proposer: Sender<ProposerMessage>,
        tx_commit: Sender<Block>,
        firewall: HashMap<u64,Vec<SocketAddr>>,
        allow_communications_at_round: u64,
        network_delay: u64,
        dns: HashMap<SocketAddr, SocketAddr>,
        id: u64,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee: committee.clone(),
                signature_service,
                store,
                leader_elector,
                mempool_driver,
                synchronizer,
                rx_message,
                rx_loopback,
                tx_proposer,
                tx_commit,
                round: 1,
                last_voted_round: 0,
                last_committed_round: 0,
                high_qc: QC::genesis(),
                timer: Timer::new(timeout_delay),
                aggregator: Aggregator::new(committee),
                network: DelayedSender::new(firewall, allow_communications_at_round, network_delay, dns.clone()),
                alt_tips: Vec::new(),
                dns: dns.clone(),
                id,
                identified_faults: Vec::new(),
            }
            .run()
            .await
        });
    }

    async fn store_block(&mut self, block: &Block) {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        {
            let mut my_mutex = MY_TIP.lock().unwrap();
            *my_mutex = block.digest().clone();
        }
        self.store.write(key, value).await;
    }

    fn increase_last_voted_round(&mut self, target: Round) {
        self.last_voted_round = max(self.last_voted_round, target);
    }

    async fn make_vote(&mut self, block: &Block) -> Option<Vote> {
        // Check if we can vote for this block.
        let safety_rule_1 = block.round > self.last_voted_round;
        let mut safety_rule_2 = block.qc.round + 1 == block.round;
        if let Some(ref tc) = block.tc {
            let mut can_extend = tc.round + 1 == block.round;
            can_extend &= block.qc.round >= *tc.high_qc_rounds().iter().max().expect("Empty TC");
            safety_rule_2 |= can_extend;
        }
        if !(safety_rule_1 && safety_rule_2) {
            return None;
        }

        // Ensure we won't vote for contradicting blocks.
        self.increase_last_voted_round(block.round);
        // TODO [issue #15]: Write to storage preferred_round and last_voted_round.
        Some(Vote::new(block, self.name, self.signature_service.clone()).await)
    }

    async fn commit(&mut self, block: Block) -> ConsensusResult<()> {
        if self.last_committed_round >= block.round {
            return Ok(());
        }

        // Ensure we commit the entire chain. This is needed after view-change.
        let mut to_commit = VecDeque::new();
        let mut parent = block.clone();
        while self.last_committed_round + 1 < parent.round {
            let ancestor = self
                .synchronizer
                .get_parent_block(&parent)
                .await?
                .expect("We should have all the ancestors by now");
            to_commit.push_front(ancestor.clone());
            parent = ancestor;
        }
        to_commit.push_front(block.clone());

        // Save the last committed block.
        self.last_committed_round = block.round;

        // Send all the newly committed blocks to the node's application layer.
        while let Some(block) = to_commit.pop_back() {
            if !block.payload.is_empty() {
                info!("Committed {}", block);

                #[cfg(feature = "benchmark")]
                for x in &block.payload {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed {} -> {:?}", block, x);
                }
            }
            debug!("Committed {:?}", block);
            if let Err(e) = self.tx_commit.send(block).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }
        Ok(())
    }

    fn update_high_qc(&mut self, qc: &QC) {
        if !qc.clone().eq(&self.high_qc){
            if !self.alt_tips.is_empty(){
                if self.alt_tips.contains(&qc.hash.clone()){
                    info!("Not updating high qc {:?} because it is in alt_tips", qc.clone());
                    return;
                }
            }
        }
        if qc.round > self.high_qc.round {
            self.high_qc = qc.clone();
        }
    }

    async fn local_timeout_round(&mut self) -> ConsensusResult<()> {
        warn!("Timeout reached for round {}", self.round);

        // Increase the last voted round.
        self.increase_last_voted_round(self.round);

        // Make a timeout message.
        let timeout = Timeout::new(
            self.high_qc.clone(),
            self.round,
            self.name,
            self.signature_service.clone(),
        )
        .await;
        debug!("Created {:?}", timeout);

        // Reset the timer.
        self.timer.reset();

        // Broadcast the timeout message.
        debug!("Broadcasting {:?}", timeout);
        let addresses = self
            .committee
            .broadcast_addresses(&self.name)
            .into_iter()
            .map(|(_, x)| x)
            .collect();
        let message = bincode::serialize(&ConsensusMessage::Timeout(timeout.clone()))
            .expect("Failed to serialize timeout message");
        self.network
            .broadcast(addresses, Bytes::from(message), self.round)
            .await;

        // Process our message.
        self.handle_timeout(&timeout).await
    }

    #[async_recursion]
    async fn handle_vote(&mut self, vote: &Vote) -> ConsensusResult<()> {
        let mut author_address = self.committee.address(&vote.author).unwrap();
        let firewall_count = self.round/self.network.allow_communications_at_round;

        author_address = self.dns[&author_address]; 
        if author_address.to_string().find(':').map(|i| author_address.to_string()[i+1..].parse().ok()).flatten() < Some((self.committee.faults) + 10000 + 1)
            && self.network.firewall.get(&(firewall_count)).unwrap_or(&self.network.firewall[&((self.network.firewall.len()-1) as u64)]).clone().contains(&author_address)
        {
            return Ok(());
        }
        if self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().contains(&author_address) {
            return Ok(());
        }

        debug!("Processing {:?}", vote);
        if vote.round < self.round {
            return Ok(());
        }

        // Ensure the vote is well formed.
        vote.verify(&self.committee)?;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(qc) = self.aggregator.add_vote(vote.clone(), self.committee.quorum_threshold_firewall(self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().clone()))? {
            debug!("Assembled {:?}", qc);

            // Process the QC.
            self.process_qc(&qc).await;

            // Make a new block if we are the next leader.
            if self.name == self.leader_elector.get_leader(self.round, self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().clone(), self.dns.clone()) {
                self.generate_proposal(None).await;
            }
        }
        Ok(())
    }

    async fn handle_timeout(&mut self, timeout: &Timeout) -> ConsensusResult<()> {
        let mut author_address = self.committee.address(&timeout.author).unwrap();
        let firewall_count = self.round/self.network.allow_communications_at_round;

        author_address = self.dns[&author_address]; 
        if author_address.to_string().find(':').map(|i| author_address.to_string()[i+1..].parse().ok()).flatten() < Some((self.committee.faults) + 10000 + 1)
            && self.network.firewall.get(&(firewall_count)).unwrap_or(&self.network.firewall[&((self.network.firewall.len()-1) as u64)]).clone().contains(&author_address)
        {
            return Ok(());
        }
        if self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().contains(&author_address) {
            return Ok(());
        }
        if timeout.round < self.round {
            return Ok(());
        }

        // Ensure the timeout is well formed.
        timeout.verify(&self.committee, self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().clone())?;

        debug!("Processing {:?}", timeout);
        // Process the QC embedded in the timeout.
        self.process_qc(&timeout.high_qc).await;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(tc) = self.aggregator.add_timeout(timeout.clone(), self.committee.quorum_threshold_firewall(self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().clone()))? {
            debug!("Assembled {:?}", tc);

            // Try to advance the round.
            self.advance_round(tc.round).await;

            // Broadcast the TC.
            debug!("Broadcasting {:?}", tc);
            let addresses = self
                .committee
                .broadcast_addresses(&self.name)
                .into_iter()
                .map(|(_, x)| x)
                .collect();
            let message = bincode::serialize(&ConsensusMessage::TC(tc.clone()))
                .expect("Failed to serialize timeout certificate");
            debug!("Firewall here in TC is {:?}", self.network.firewall.clone());
            self.network
                .broadcast(addresses, Bytes::from(message), self.round)
                .await;

            // Make a new block if we are the next leader.
            if self.name == self.leader_elector.get_leader(self.round, self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().clone(), self.dns.clone()) {
                self.generate_proposal(Some(tc)).await;
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn advance_round(&mut self, round: Round) {
        if round < self.round {
            return;
        }
        // Reset the timer and advance round.
        self.timer.reset();
        self.round = round + 1;
        debug!("Moved to round {}", self.round);

        // Cleanup the vote aggregator.
        self.aggregator.cleanup(&self.round);
    }

    #[async_recursion]
    async fn generate_proposal(&mut self, tc: Option<TC>) {
        self.tx_proposer
            .send(ProposerMessage::Make(self.round, self.high_qc.clone(), tc))//, self.identified_faults.iter().map(|x| !self.announced_faults.contains(&x).collect())))
            .await
            .expect("Failed to send message to proposer");
    }

    async fn cleanup_proposer(&mut self, b0: &Block, b1: &Block, block: &Block) {
        let digests = b0
            .payload
            .iter()
            .cloned()
            .chain(b1.payload.iter().cloned())
            .chain(block.payload.iter().cloned())
            .collect();
        self.tx_proposer
            .send(ProposerMessage::Cleanup(digests))
            .await
            .expect("Failed to send message to proposer");
    }

    async fn process_qc(&mut self, qc: &QC) {
        self.advance_round(qc.round).await;
        self.update_high_qc(qc);
    }

    fn finish_sync_request(blocks: Blocks) -> Vec<Block> {
        let mut my_lock = REQUEST_BLOCKS.lock().unwrap();
        *my_lock = 2;
        let mut my_requests = SENT_SYNCS.lock().unwrap();
        let return_value;
        {
            let blocks_iterate: &mut Vec<Block> = match my_requests.get_mut(&blocks.sender) { 
                Some (vec_blocks) => vec_blocks,
                None => return Vec::new(),
            };

            // Pop the block from my_request so it is not duplicated
            // from the previous request
            blocks_iterate.pop();
            blocks_iterate.extend(blocks.blocks.clone());
            return_value = blocks_iterate.clone();
        }
        return_value
    }

    fn get_clique_from_sender(&mut self, sender: PublicKey) -> Vec<SocketAddr> {
        let sender_address = self.dns[&self.committee.authorities[&sender].address];
        let mut values: Vec<_> = self.committee.authorities.values().clone().map(|x| self.dns[&x.address].to_string()).collect::<Vec<_>>();
        values.sort();

        let honest = &values[..(usize::try_from(self.committee.faults).unwrap()+1)];
        let no_honest = &values[(usize::try_from(self.committee.faults).unwrap()+1)..];
        let cliques: Vec<_> = no_honest.chunks((2*self.committee.faults).try_into().unwrap()).collect();

        // Getting senders clique
        let senders_clique = match honest.into_iter().position(|x| x.contains(&sender_address.to_string())){
            Some(index) => index % cliques.len(),
            None => {
                let mut index = 0;
                for clique in &cliques{
                    if clique.contains(&sender_address.to_string()){
                        break;
                    }
                    index += 1;
                }
                index
            },
        };
        cliques[senders_clique].into_iter().map(|x| x.parse::<SocketAddr>().unwrap()).collect()
    }

    #[async_recursion]
    async fn process_blocks(&mut self, blocks: Blocks) -> ConsensusResult<()> {
        let mut middle;
        let mut left;
        let mut right;

        let mut alt_chain;
        if blocks.blocks[blocks.blocks.len() - 1].qc == QC::genesis() || blocks.blocks[blocks.blocks.len() - 1].digest().eq(&Block::genesis().digest()) {
            alt_chain = Self::finish_sync_request(blocks.clone());
            if !blocks.blocks[blocks.blocks.len() -1].digest().eq(&Block::genesis().digest()) {
                alt_chain.push(Block::genesis());
            }

        } else {
            alt_chain = match self.store.read(blocks.blocks[blocks.blocks.len() - 1].digest().to_vec()).await? {
                Some (_) => {
                    Self::finish_sync_request(blocks.clone())
                },
                None => {
                    // Checking if we already have the tip. Updating it if we do. TODO: improve index
                    // (remove it)
                    let mut index = 0;
                    for tip in self.alt_tips.iter() {
                        for block in blocks.blocks.iter() {
                            if tip.eq(&block.digest()) {
                                self.alt_tips[index] = blocks.blocks[0].digest();
                                let mut my_lock = REQUEST_BLOCKS.lock().unwrap();
                                *my_lock = 10;
                                //let mut my_requests = SENT_SYNCS.lock().unwrap();
                                //my_requests.remove(&blocks.sender);
                                return Ok(());
                            }
                        }
                        index += 1;
                    }
                    debug!("Asking for more blocks");
                    let num_blocks;
                    // We create a new scope to unlock our mutex. Mutex are automatically
                    // unlocked when they leave the current scope. This is necessary because
                    // we call an asynchronous function later in this scope
                    {
                        // We exponentially increase the number of requested blocks in case
                        // we do not have the last block of the received request
                        let mut my_lock = REQUEST_BLOCKS.lock().unwrap();
                        *my_lock = *my_lock * 2;
                        num_blocks = *my_lock;

                        // If the sender is not already in our tracked requests
                        let mut my_requests = SENT_SYNCS.lock().unwrap();
                        let mut received_blocks = my_requests[&blocks.sender].clone();
                        //Pop the block from my_request
                        received_blocks.pop();
                        received_blocks.extend(blocks.blocks.clone());
                        my_requests.insert(blocks.sender, received_blocks);
                    }
                    let message = ConsensusMessage::NewSyncRequest(blocks.blocks[blocks.blocks.len() - 1].digest(), num_blocks, self.name);
                    debug!("Sending new sync request with the following missing: {:?}, sender is {:?}",
                           blocks.blocks[blocks.blocks.len() - 1].digest(), blocks.sender);
                    let message = bincode::serialize(&message).expect("Failed to serialize new sync request");
                    let address = self.committee
                        .address(&blocks.sender)
                        .expect("Failed to get address");
                    let mut network = ReliableSender::new(self.network.firewall.clone(), self.network.allow_communications_at_round, self.network.network_delay, self.dns.clone());
                    let handler = network.send(address, Bytes::from(message)).await;
                    let _ = handler.await;
                    return Ok(());
                }
            };
        }
        

        let mut found = false;
        middle = alt_chain.len().div_ceil(2);
        left = 0;
        right = alt_chain.len()-1;
        while found == false {
            if left == right {
                found = true;
            }
            match self.store.read(alt_chain[middle].digest().to_vec()).await? {
                Some(_bytes) => {
                    right = middle;
                    middle = (left + right).div_ceil(2);
                },
                None => {
                    left = middle;
                    middle = (left + right).div_ceil(2);
                }
            }
            if right - 1 == left {
                left = right;
            }
        }

        let common_parent = alt_chain[left].digest();
        let child_qc = alt_chain[left-1].qc.hash.clone();
        debug!("Length of alt chain here is {:?}", alt_chain.len());
        debug!("Finished retrieval");

        // Get the child of the common_parent
        let mut local_representative: Block = Block::genesis();
        let local_tip: Digest;
        {
            let my_tip = MY_TIP.lock().unwrap();
            local_tip = my_tip.clone();
        }
        if let Some(bytes) = self.store.read(local_tip.to_vec()).await.expect("Failed to read block"){
            local_representative = bincode::deserialize(&bytes).expect("Failed to deserialize our own block");
        }
        if local_representative.digest().eq(&common_parent.clone()) {
            return Ok(());
        }
        while !child_qc.eq(local_representative.parent()) {
            if let Some(bytes) = self.store.read(local_representative.parent().to_vec()).await.expect("Failed to read block"){
                local_representative = bincode::deserialize(&bytes).expect("Failed to deserialize our own block");
            } else {
                break;
            }
        }


        if local_representative.digest() <= alt_chain[left - 1].digest() {
            // save the tip of the alternative chain
            debug!("Remaining at the same chain");
            if local_representative.digest() != alt_chain[left - 1].digest(){ 
                self.alt_tips.push(alt_chain[0].digest());
            }
            let my_clique = self.get_clique_from_sender(self.name);
            let parties_to_kick: &[std::net::SocketAddr];

            // Ideally, here we wouldn't have access to this information, we would check if their
            // public key is the same and the twins would count for one node
            let num_nodes = self.committee.nodes as usize;
            let num_parties_kick = (num_nodes - self.identified_faults.len()).div_ceil(3) + self.identified_faults.len();
            parties_to_kick = &my_clique[..num_parties_kick];
            self.identified_faults = Vec::<_>::new();
            self.identified_faults.extend(parties_to_kick);
            self.committee.update_num_of_twins((usize::try_from(self.committee.num_of_twins).unwrap() + parties_to_kick.len()).try_into().unwrap());
            for firewall in self.network.firewall.values_mut() {
                if !parties_to_kick.iter().all(|item| firewall.contains(item)){
                    firewall.extend(parties_to_kick);
                }
            }
            debug!("Updated firewall, which now is {:?}", self.network.firewall[&((self.network.firewall.len()-1) as u64)]);
            self.tx_proposer.send(ProposerMessage::UpdateFirewall(self.network.firewall.clone())).await.expect("Failed to update firewall");
            let local_tip: Digest;
            {
                let my_tip = MY_TIP.lock().unwrap();
                local_tip = my_tip.clone();
            }
            if let Some(bytes) = self.store.read(local_tip.to_vec()).await.expect("Failed to read block"){
                local_representative = bincode::deserialize(&bytes).expect("Failed to deserialize our own block");
            }
            self.high_qc = local_representative.qc.clone();
            let mut network = ReliableSender::new(self.network.firewall.clone(), self.network.allow_communications_at_round, self.network.network_delay, self.dns.clone());
            let addresses: Vec<_> = self
                .committee
                .broadcast_addresses(&self.name)
                .into_iter()
                .map(|(_, x)| self.dns[&x])
                .collect();
            let my_clique: Vec<_> = addresses.clone().into_iter().filter(|x| !self.network.firewall[&((self.network.firewall.len()-1) as u64)].contains(&x)).collect();
            let message = ConsensusMessage::ShiftedChain(self.name,my_clique.clone(), self.round);
            let message = bincode::serialize(&message)
                .expect("Failed to serialize timeout message");
            for address in my_clique.clone() {
                let send_address = self.dns.iter().find_map(|(key, &val)| if val == address { Some(key) } else { None }).unwrap();
                let handler = network.send(*send_address, Bytes::from(message.clone())).await;
                let _ = handler.await;
            }

            // Here, we check if the faults are at most 1/3
            let mut honest_count = my_clique.iter().filter(|&value| u32::from(value.port()) < 10000 + self.committee.faults + 1).count();
            if u32::from(self.dns[&self.committee.address(&self.name).expect("Could not find self address")].port()) < 10000 + self.committee.faults + 1{
                honest_count = honest_count + 1;
            }
            let num_faults = (self.committee.nodes as usize - self.identified_faults.len()) - honest_count;
            if num_faults as f64/(self.committee.nodes as usize - self.identified_faults.len()) as f64 >= 1 as f64/3 as f64{
                debug!("Not done yet. We still have {:?} faults, which is more than 1/3", num_faults);
                let current_firewall = self.round/self.network.allow_communications_at_round;
                let mut stages_to_shift: Vec<u64> = self.network.firewall.keys().cloned().filter(|&stage| stage >= current_firewall).collect();
                
                // Sort stages to shift in descending order to ensure we shift from higher to lower stages
                stages_to_shift.sort_by(|a, b| b.cmp(a));
                
                // Shift the stages
                for stage in stages_to_shift {
                    if let Some(stage_data) = self.network.firewall.remove(&stage) {
                        self.network.firewall.insert(stage + 1, stage_data);
                    }
                }
                
                {
                    let my_requests = SENT_SYNCS.lock().unwrap();
                    let mut received_blocks = my_requests[&blocks.sender].clone();
                    received_blocks = Vec::new();
                }

                // Create a new firewall stage at current_firewall + 1
                self.network.firewall.insert(current_firewall, self.network.firewall[&(current_firewall - 1)].clone());
            } else {  
                let current_firewall = self.round/self.network.allow_communications_at_round;
                let my_address = self.dns[&self.committee.address(&self.name).expect("Failed to get address")];
                let num_faults = self.committee.faults.clone();
                if u32::from(my_address.port()) >= 10000 + num_faults + 1 {
                    if let Some(firewall) = self.network.firewall.get_mut(&current_firewall){
                        firewall.retain(|x| u32::from(x.port()) > 10000 + num_faults + 1);
                    }
                }
                self.network.firewall.insert(current_firewall, self.network.firewall[&((self.network.firewall.len() - 1) as u64)].clone());
                self.network.firewall.insert(current_firewall - 1, self.network.firewall[&(current_firewall)].clone());
                debug!("We have removed enough faults. My firewall is {:?}", self.network.firewall.clone());
            }
            self.tx_proposer.send(ProposerMessage::UpdateFirewall(self.network.firewall.clone())).await.expect("Failed to update firewall");

        } else {
            debug!("Shifting chain");
            let mut local_chain_tip: Block = Block::genesis();
            let local_tip: Digest;
            {
                let my_tip = MY_TIP.lock().unwrap();
                local_tip = my_tip.clone();
            }
            if let Some(bytes) = self.store.read(local_tip.to_vec()).await.expect("Failed to read block"){
                local_chain_tip = bincode::deserialize(&bytes).expect("Failed to deserialize our own block");
            }
            self.alt_tips.push(local_chain_tip.qc.hash.clone());
            while !local_chain_tip.digest().eq(&local_representative.digest()){
                {
                    let mut previous_chain = PREVIOUS_CHAIN.lock().unwrap();
                    if !previous_chain.contains_key(&blocks.sender.clone()){
                        previous_chain.insert(blocks.sender.clone(), Vec::new());
                    }
                }
                self.store.delete(local_chain_tip.digest().to_vec()).await;
                if let Some(bytes) = self.store.read(local_chain_tip.parent().to_vec()).await.expect("Failed to read block"){
                    local_chain_tip = bincode::deserialize(&bytes).expect("Failed to deserialize our own block");
                }
                {
                    let mut my_requests = SENT_SYNCS.lock().unwrap();
                    if let Some(received_blocks) = my_requests.get_mut(&blocks.sender) {
                        received_blocks.clear();  // Removes all elements from the vector
                    }
                }
            }
            self.store.delete(local_representative.digest().to_vec()).await;
            
            while (left -1) != 0 {
                self.store_block(&alt_chain[left-1]).await;
                left -= 1;
            }
            self.store_block(&alt_chain[left-1]).await;

            // Update the local high_qc
            self.high_qc = alt_chain[0].qc.clone();
            self.last_voted_round = alt_chain[0].round.clone();
            self.last_committed_round = 0;
            
            let faulty_parties: Vec<PublicKey> = self.get_faulty_parties(blocks.sender.clone()).await;
            self.committee.update_num_of_twins((usize::try_from(self.committee.num_of_twins).unwrap() + faulty_parties.len()).try_into().unwrap());
            let faulty_parties_addresses: Vec<_> = faulty_parties.iter().map(|x| self.dns[&self.committee.address(x).expect("Failed to get node address")]).collect();
            for firewall in self.network.firewall.values_mut() {
                firewall.extend(faulty_parties_addresses.clone());
                debug!("Firewall update here is {:?}", firewall.clone());
            }
            self.tx_proposer.send(ProposerMessage::UpdateFirewall(self.network.firewall.clone())).await.expect("Failed to update firewall");

            let mut network = ReliableSender::new(self.network.firewall.clone(), self.network.allow_communications_at_round, self.network.network_delay, self.dns.clone());
            let addresses: Vec<_> = self
                .committee
                .broadcast_addresses(&self.name)
                .into_iter()
                .map(|(_, x)| self.dns[&x])
                .collect();
            let my_clique: Vec<_> = addresses.clone().into_iter().filter(|x| !self.network.firewall[&((self.network.firewall.len()-1) as u64)].contains(&x)).collect();
            let message = bincode::serialize(&ConsensusMessage::ShiftedChain(self.name, my_clique.clone(), self.round))
                .expect("Failed to serialize timeout message");
            for address in my_clique.clone() {
                let send_address = self.dns.iter().find_map(|(key, &val)| if val == address { Some(key) } else { None }).unwrap();
                let handler = network.send(*send_address, Bytes::from(message.clone())).await;
                let _ = handler.await;
            }
            
            // Here, we check if the faults are at most 1/3
            let mut honest_count = my_clique.iter().filter(|&value| u32::from(value.port()) < 10000 + self.committee.faults + 1).count();
            if u32::from(self.dns[&self.committee.address(&self.name).expect("Could not find self address")].port()) < 10000 + self.committee.faults + 1{
                honest_count = honest_count + 1;
            }
            let num_faults = (self.committee.nodes as usize - self.identified_faults.len()) - honest_count;
            if num_faults as f64/(self.committee.nodes as usize - self.identified_faults.len()) as f64 >= 1 as f64/3 as f64{
                debug!("Not done yet. We still have {:?} faults, which is more than 1/3", num_faults);
                // Add new firewall here
                let current_firewall = self.round/self.network.allow_communications_at_round;
                let mut stages_to_shift: Vec<u64> = self.network.firewall.keys().cloned().filter(|&stage| stage >= current_firewall).collect();
                
                // Sort stages to shift in descending order to ensure we shift from higher to lower stages
                stages_to_shift.sort_by(|a, b| b.cmp(a));
                
                // Step 2: Shift the stages
                for stage in stages_to_shift {
                    if let Some(stage_data) = self.network.firewall.remove(&stage) {
                        self.network.firewall.insert(stage + 1, stage_data);
                    }
                }

                // Step 3: Create a new empty firewall stage at current_firewall + 1
                self.network.firewall.insert(current_firewall, self.network.firewall[&(current_firewall - 1)].clone());
                let my_original_clique = self.get_clique_from_sender(self.name);
                let mut parties_to_keep_clique: Vec<_>;
                let _num_nodes_clique = self.committee.nodes as usize;

                let honest_count = my_original_clique.iter().filter(|&value| u32::from(value.port()) < 10000 + self.committee.faults + 1).count();
                let num_parties_kick = self.identified_faults.len();
                let num_to_keep = my_original_clique.len() - num_parties_kick - honest_count;
                parties_to_keep_clique = my_original_clique.iter().rev().collect();
                parties_to_keep_clique = (&parties_to_keep_clique[..num_to_keep]).to_vec();

                let my_shifted_clique = my_clique.clone();

                let num_faults = self.committee.faults.clone();
                let my_address = self.dns[&self.committee.address(&self.name).expect("Failed to get address")];
                if let Some(firewall) = self.network.firewall.get_mut(&current_firewall){
                    firewall.retain(|x| !parties_to_keep_clique.contains(&x));
                    if u32::from(my_address.port()) < 10000 + num_faults + 1 {
                        firewall.extend(my_shifted_clique.iter().filter(|&value| u32::from(value.port()) > 10000 + num_faults));
                    }
                };
                if let Some(firewall) = self.network.firewall.get_mut(&(current_firewall - 1)){
                    firewall.retain(|x| !parties_to_keep_clique.contains(&x));
                };

                self.network.firewall.insert(current_firewall + 1, self.network.firewall[&(current_firewall)].clone());
                if let Some(firewall) = self.network.firewall.get_mut(&(current_firewall + 1)){
                    firewall.retain(|x| u32::from(x.port()) > 10000 + num_faults + 1);
                };

                // Finally, I change the last stage of the firewall, which represents my clique
                self.network.firewall.get_mut(&((self.network.firewall.len()-1) as u64)).map(|firewall| {
                    firewall.retain(|x| !parties_to_keep_clique.contains(&x));
                    if u32::from(my_address.port()) < 10000 + num_faults + 1 {
                        firewall.extend(my_shifted_clique.iter().filter(|&value| u32::from(value.port()) > 10000 + num_faults));
                    }
                });

                {
                    let mut my_requests = SENT_SYNCS.lock().unwrap();
                    if let Some(received_blocks) = my_requests.get_mut(&blocks.sender) {
                        received_blocks.clear();  // Removes all elements from the vector
                    }
                }
                for address in parties_to_keep_clique.clone() {
                    let send_address = self.dns.iter().find_map(|(key, &val)| if val == *address { Some(key) } else { None }).unwrap();
                    let handler = network.send(*send_address, Bytes::from(message.clone())).await;
                    let _ = handler.await;
                }

            } else {  
                let current_firewall = self.round/self.network.allow_communications_at_round;
                self.network.firewall.insert((self.network.firewall.len() - 1 as usize) as u64, self.network.firewall[&(current_firewall)].clone());
                self.network.firewall.insert(current_firewall - 1, self.network.firewall[&(current_firewall)].clone());
                debug!("We have removed enough faults. Faults here is {:?} and nodes is {:?} and ident. faults is {:?}. My firewall is {:?}", num_faults, self.committee.nodes, self.identified_faults.len(), self.network.firewall.clone());
            }
            self.tx_proposer.send(ProposerMessage::UpdateFirewall(self.network.firewall.clone())).await.expect("Failed to update firewall");
        }
        debug!("Finished recovery procedure");

        Ok(())

        
    }

    async fn get_faulty_parties(&mut self, sender: PublicKey) -> Vec<PublicKey> {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        let values: Vec<_> = self.committee.authorities.values().cloned().collect();

        let mut indices = Vec::new(); 
        let mut senders_clique = Vec::new();
        {
            let received_clique = REQUEST_CLIQUES.lock().unwrap();
            if received_clique.contains_key(&sender){
                senders_clique = self.get_clique_from_sender(sender.clone());
            } else {
                senders_clique = self.get_clique_from_sender(sender.clone());
            }
        }
        let mut parties_to_keep: Vec<_>;
        let honest_count = senders_clique.iter().filter(|&value| u32::from(value.port()) < 10000 + self.committee.faults + 1).count();

        // There's some repeated code here. Clean it
        let num_nodes = self.committee.nodes as usize;
        let num_parties_kick = (num_nodes - self.identified_faults.len()).div_ceil(3);
        let num_to_keep = senders_clique.len() - self.identified_faults.len() - num_parties_kick - honest_count;
        parties_to_keep = senders_clique.iter().rev().collect();
        let parties_to_kick = (&parties_to_keep[num_to_keep..]).to_vec();
        
        self.identified_faults = Vec::<_>::new();
        self.identified_faults.extend(parties_to_kick.clone());

        parties_to_keep = (&parties_to_keep[..num_to_keep]).to_vec();
        let mut sent_sync;
        {
            sent_sync = SENT_SYNCS.lock().unwrap();
            let mut block = Vec::new();
            block.push(Block::default());
            let my_keys: Vec<_> = parties_to_keep.clone().into_iter().map(|x| self.committee.authorities.iter().find_map(|(key, &ref val)| if self.dns[&val.address] == *x { Some(key) } else { None })).collect();                        
            let _ = my_keys.into_iter().map(|x| sent_sync.insert(*x.unwrap(), block.clone())); 
        }
        let mut parties_to_keep_clone = Vec::new();
        for parties in parties_to_keep.iter(){
            parties_to_keep_clone.push(**parties);
        }
        for _value in values.iter() {
            let virtual_address = self.dns[&_value.address];
            if virtual_address.to_string().find(':').map(|i| virtual_address.to_string()[i+1..].parse().ok()).flatten() < Some((self.committee.faults) + 10000 + 1){
                indices.push(false);
            } else if self.network.firewall[&(self.round/self.network.allow_communications_at_round)].clone().contains(&virtual_address){
                indices.push(false);
            } else if parties_to_keep_clone.contains(&virtual_address){
                indices.push(false);
            } else {
                indices.push(true);
            }
        }
        for firewall in self.network.firewall.values_mut(){
            firewall.retain(|x| !parties_to_keep.contains(&x))
        }
        let mut iter = indices.iter();
        keys.retain(|_| *iter.next().unwrap());
        keys
    }


    #[async_recursion]
    async fn process_block(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!("Processing {:?}", block);

        let mut author_address = self.committee.address(&block.author).unwrap();
        let firewall_count = self.round/self.network.allow_communications_at_round;

        author_address = self.dns[&author_address]; 
        if author_address.to_string().find(':').map(|i| author_address.to_string()[i+1..].parse().ok()).flatten() < Some((self.committee.faults) + 10000 + 1)
            && self.network.firewall.get(&(firewall_count)).unwrap_or(&self.network.firewall[&((self.network.firewall.len()-1) as u64)]).clone().contains(&author_address)
        {
            return Ok(());
        }
        if self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().contains(&author_address) {
            return Ok(());
        }

        // Let's see if we have the last three ancestors of the block, that is:
        //      b0 <- |qc0; b1| <- |qc1; block|
        // If we don't, the synchronizer asks for them to other nodes. It will
        // then ensure we process both ancestors in the correct order, and
        // finally make us resume processing this block.
        let (b0, b1) = match self.synchronizer.get_ancestors(block).await? {
            Some(ancestors) => ancestors,
            None => {
                debug!("Processing of {} suspended: missing parent", block.digest());
                return Ok(());
            }
        };

        // Store the block only if we have already processed all its ancestors.
        self.store_block(block).await;

        self.cleanup_proposer(&b0, &b1, block).await;

        // Check if we can commit the head of the 2-chain.
        // Note that we commit blocks only if we have all its ancestors.
        if b0.round + 1 == b1.round {
            self.mempool_driver.cleanup(b0.round).await;
            self.commit(b0).await?;
        }

        // Ensure the block's round is as expected.
        // This check is important: it prevents bad leaders from producing blocks
        // far in the future that may cause overflow on the round number.
        if block.round != self.round {
            return Ok(());
        }

        // See if we can vote for this block.
        if let Some(vote) = self.make_vote(block).await {
            debug!("Created {:?}", vote);
            let next_leader = self.leader_elector.get_leader(self.round + 1,self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().clone(), self.dns.clone());
            if next_leader == self.name {
                self.handle_vote(&vote).await?;
            } else {
                debug!("Sending {:?} to {}", vote, next_leader);
                let address = self
                    .committee
                    .address(&next_leader)
                    .expect("The next leader is not in the committee");
                let message = bincode::serialize(&ConsensusMessage::Vote(vote))
                    .expect("Failed to serialize vote");
                self.network.send(address, Bytes::from(message), self.round).await;
            }
        }
        Ok(())
    }

    async fn make_new_sync_request(&mut self, block: &Block, author: PublicKey){
        let address = self.committee
            .address(&author)
            .expect("Author of valid block is not in the committee");

        let mut network = ReliableSender::new(self.network.firewall.clone(), self.network.allow_communications_at_round, self.network.network_delay, self.dns.clone()); 
        let mut blocks = 0;
        { 
            let my_lock = REQUEST_BLOCKS.lock().unwrap();
            blocks = *my_lock;
        }
        {
            let mut sent_syncs = SENT_SYNCS.lock().unwrap();
            if !sent_syncs.contains_key(&author.clone()){
                sent_syncs.insert(author, Vec::new());
            }
        }
        let message = ConsensusMessage::NewSyncRequest(block.parent().clone(), blocks, self.name);
        let message = bincode::serialize(&message)
            .expect("Failed to serialize sync request");
        debug!("Sending new sync request to this address {:?}", address);
        let handler = network.send(address, Bytes::from(message)).await;
        let _ = handler.await;
    }

    async fn handle_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        let digest = block.digest();

        let mut author_address = self.committee.address(&block.author).unwrap();
        let firewall_count = self.round/self.network.allow_communications_at_round;

        author_address = self.dns[&author_address]; 
        if author_address.to_string().find(':').map(|i| author_address.to_string()[i+1..].parse().ok()).flatten() < Some((self.committee.faults) + 10000 + 1)
            && self.network.firewall.get(&(firewall_count)).unwrap_or(&self.network.firewall[&((self.network.firewall.len()-1) as u64)]).clone().contains(&author_address)
        {
            return Ok(());
        }
        if self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().contains(&author_address) {
            return Ok(());
        }
        
        if self.network.allow_communications_at_round < self.round {
            let sync_to_sender;
            {
                let my_requests = SENT_SYNCS.lock().unwrap();
                sync_to_sender = my_requests.contains_key(&block.author);
            }
            if self.network.firewall.get(&((self.round/self.network.allow_communications_at_round)-1)).unwrap_or(&self.network.firewall[&((self.network.firewall.len()-3) as u64)]).contains(&author_address) && !self.network.firewall.get(&(self.round/self.network.allow_communications_at_round)).unwrap_or(&self.network.firewall[&((self.network.firewall.len()-2) as u64)]).contains(&author_address) 
                && !sync_to_sender {
                if self.alt_tips.contains(&block.parent()) {
                    let index = self.alt_tips.iter().position(|x| x.eq(block.parent())).unwrap();
                    self.alt_tips[index] = block.digest();
                } else {
                    let _ = self.make_new_sync_request(&block, block.author.clone()).await;
                    {
                        let mut my_requests = SENT_SYNCS.lock().unwrap();
                        my_requests.insert(block.author.clone(), Vec::new());
                    }
                    return Ok(());
                }
            }
        }

        // Ensure the block proposer is the right leader for the round.
        ensure!(
            block.author == self.leader_elector.get_leader(block.round,self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().clone(), self.dns.clone()),
            ConsensusError::WrongLeader {
                digest,
                leader: block.author,
                round: block.round
            }
        );

        // Check the block is correctly formed.
        block.verify(&self.committee, self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().clone())?;

        // Process the QC. This may allow us to advance round.
        self.process_qc(&block.qc).await;

        // Process the TC (if any). This may also allow us to advance round.
        if let Some(ref tc) = block.tc {
            self.advance_round(tc.round).await;
        }

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(block.clone()).await? {
            debug!("Processing of {} suspended: missing payload", digest);
            return Ok(());
        }

        // All check pass, we can process this block.
        self.process_block(block).await
    }

    async fn handle_new_sync_requests(&mut self, digest: Digest, num_blocks: u64, origin: PublicKey) -> ConsensusResult<()>{
        let mut i = 0;
        let mut block = Block::default();
        let mut parent;
        let mut vec_blocks = Vec::new();

        let in_requests;
        let block_digest;
        {
            let my_requests = SENT_SYNCS.lock().unwrap();
            in_requests = my_requests.contains_key(&origin.clone());
            let my_tip = MY_TIP.lock().unwrap();
            block_digest = my_tip.clone();
        }
        let block_ask = self.store.read(block_digest.to_vec()).await.unwrap().expect("Failed to get tip");
        let _block_request: Block = bincode::deserialize(&block_ask).expect("Failed to deserialize our own block");
        let address = self.committee.address(&origin.clone()).unwrap();
        parent = digest;
        
        while i < num_blocks {
            // Reply to the request (if we can).
            if let Some(bytes) = self
                .store
                .read(parent.to_vec())
                .await
                .expect("Failed to read from storage")
            {
                block =
                    bincode::deserialize(&bytes).expect("Failed to deserialize our own block");
                vec_blocks.push(block.clone());
            } else {
                let mut block_in_previous_chain = false;
                {
                    let previous_chain = PREVIOUS_CHAIN.lock().unwrap();
                    let mut block_position = None;
                    if previous_chain.contains_key(&origin.clone()) {
                        block_position = previous_chain[&origin.clone()].iter().position(|x| x.digest() == parent);
                    }
                    match block_position {
                        Some(index) => {
                            if index as u64 + num_blocks > previous_chain[&origin.clone()].len() as u64 {
                                let block_extend = &previous_chain[&origin.clone()][index..].to_vec();
                                vec_blocks = block_extend.clone();
                            }
                            else{
                                vec_blocks = previous_chain[&origin.clone()][index..(index + num_blocks as usize)].to_vec();
                            }
                            block_in_previous_chain = true;
                        },
                        None => {
                            block_in_previous_chain = false;
                        }
                    }
                }
                if block_in_previous_chain {
                    block = vec_blocks[vec_blocks.len()-1].clone();
                    break;
                }
                let last_block;
                {
                    let my_mutex = MY_TIP.lock().unwrap();
                    last_block = my_mutex.clone();
                }
                let bytes = self.store.read(last_block.to_vec()).await.unwrap().expect("Failed to read last block");
                block = 
                    bincode::deserialize(&bytes).expect("Failed to deserialize our own block");
                vec_blocks.push(block.clone());
            }
            if block.qc == QC::genesis() {
                vec_blocks.push(Block::genesis());
                i = num_blocks;
            } else {
                parent = block.clone().parent().clone();
            }
            i = i + 1;
        }
                    
        if vec_blocks.is_empty() == false {
            let message;
            if num_blocks == 10 {
                let values: Vec<_> = self.committee.authorities.values().cloned().collect();
                let my_authorities: Vec<_> = values.iter().filter(|x| !self.network.firewall.get(&(self.round/self.network.allow_communications_at_round)).unwrap_or(&self.network.firewall[&((self.network.firewall.len()-1) as u64)]).contains(&self.dns[&x.address]) && x.address != self.committee.address(&self.name).unwrap()).collect();
                let my_clique: Vec<_> = my_authorities.iter().map(|x| self.dns[&x.address]).collect();
                message = bincode::serialize(&ConsensusMessage::FirstBlocks(Blocks::new(self.name, vec_blocks.clone()), my_clique.clone())).expect("Failed to serialize vec of blocks");
            } else {
                message = bincode::serialize(&ConsensusMessage::Blocks(Blocks::new(self.name, vec_blocks.clone()))).expect("Failed to serialize vec of blocks");
            }
            let mut network = ReliableSender::new(self.network.firewall.clone(), self.network.allow_communications_at_round, self.network.network_delay, self.dns.clone());
            let handler = network.send(address, Bytes::from(message)).await;
            let _ = handler.await;
        }
        Ok(())
    }

    async fn handle_tc(&mut self, tc: TC) -> ConsensusResult<()> {
        let mut author_address = self.committee.address(&(tc.votes[0].0)).unwrap();
        let firewall_count = self.round/self.network.allow_communications_at_round;

        author_address = self.dns[&author_address]; 
        if author_address.to_string().find(':').map(|i| author_address.to_string()[i+1..].parse().ok()).flatten() < Some((self.committee.faults) + 10000 + 1)
            && self.network.firewall.get(&(firewall_count)).unwrap_or(&self.network.firewall[&((self.network.firewall.len()-1) as u64)]).clone().contains(&author_address)
        {
            return Ok(());
        }
        if self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().contains(&author_address) {
            return Ok(());
        }

        tc.verify(&self.committee, self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().clone())?;
        if tc.round < self.round {
            return Ok(());
        }
        self.advance_round(tc.round).await;
        if self.name == self.leader_elector.get_leader(self.round,self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().clone(), self.dns.clone()) {
            self.generate_proposal(Some(tc)).await;
        }
        Ok(())
    }

    fn get_my_current_clique (&mut self, node_to_add: PublicKey) -> Vec<SocketAddr> {
        let addresses: Vec<_> = self
                .committee
                .broadcast_addresses(&self.name)
                .into_iter()
                .map(|(_, x)| self.dns[&x])
                .collect();
        let mut my_clique: Vec<_> = addresses.clone().into_iter().filter(|x| !self.network.firewall[&((self.network.firewall.len()-1) as u64)].contains(&x)).collect();

        my_clique.push(self.dns[&self.committee.address(&self.name).unwrap()]);
        let mut return_value: Vec<_> = my_clique.into_iter().filter(|&x| x != self.dns[&(self.committee.address(&node_to_add).unwrap())]).collect();

        return_value.sort();
        return_value
    }

    async fn handle_shifted_chain (&mut self, author: PublicKey, shifted_chain: Vec<SocketAddr>, round_number: u64) -> ConsensusResult <()> {
        let mut author_address = self.committee.address(&author).unwrap();
        let firewall_count = self.round/self.network.allow_communications_at_round;

        author_address = self.dns[&author_address]; 
        if author_address.to_string().find(':').map(|i| author_address.to_string()[i+1..].parse().ok()).flatten() < Some((self.committee.faults) + 10000 + 1)
            && self.network.firewall.get(&(firewall_count)).unwrap_or(&self.network.firewall[&((self.network.firewall.len()-1) as u64)]).clone().contains(&author_address)
                && self.network.firewall.get(&(round_number/self.network.allow_communications_at_round)).unwrap_or(&self.network.firewall[&((self.network.firewall.len()-1) as u64)]).clone().contains(&author_address)
        {
            return Ok(());
        }
        if self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().contains(&author_address) {
            return Ok(());
        }
        self.round = round_number;
    
        let mut shift_chain_copy = shifted_chain.clone();
        shift_chain_copy.sort();
        if self.get_my_current_clique(author) == shift_chain_copy { 
            return Ok(()); 
        } 
        if author == self.name {
            return Ok(());
        } 
        let mut keys = Vec::new(); 
        for (key, value) in self.committee.authorities.iter() {
            if shifted_chain.contains(&self.dns[&value.address]){
                keys.push(key.clone())
            }
        } 
        let mut sent_sync = Vec::new(); 
        {
            let my_requests = SENT_SYNCS.lock().unwrap(); 
            sent_sync = keys.iter().map(|x| my_requests.contains_key(&x)).collect(); 
            let mut iter = sent_sync.iter(); 
            keys.retain(|_| *iter.next().unwrap());
        } 
        let my_tip_clone; 
        {
            my_tip_clone = MY_TIP.lock().unwrap().clone();
        } 
        let bytes = self.store.read(my_tip_clone.clone().to_vec()).await.unwrap().expect("Failed to read blocks"); 
        let block: Block = bincode::deserialize(&bytes).expect("Failed to deserialize our own block"); 
        if keys.is_empty() {
            self.make_new_sync_request(&block.clone(), author.clone()).await;
        } 
        Ok(())
    }

    pub async fn run(&mut self) {
        // Upon booting, generate the very first block (if we are the leader).
        // Also, schedule a timer in case we don't hear from the leader.
        self.timer.reset();
        if self.name == self.leader_elector.get_leader(self.round,self.network.firewall.get(&((self.network.firewall.len()-1) as u64)).unwrap().clone(), self.dns.clone()) {
            self.generate_proposal(None).await;
        }

        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.rx_message.recv() => match message {
                    ConsensusMessage::Propose(block) => self.handle_proposal(&block).await,
                    ConsensusMessage::Vote(vote) => self.handle_vote(&vote).await,
                    ConsensusMessage::Timeout(timeout) => self.handle_timeout(&timeout).await,
                    ConsensusMessage::TC(tc) => self.handle_tc(tc).await,
                    ConsensusMessage::Blocks(blocks) => self.process_blocks(blocks).await,
                    ConsensusMessage::FirstBlocks(blocks, committee) => { {let mut my_lock = REQUEST_CLIQUES.lock().unwrap(); if !my_lock.contains_key(&blocks.sender){ my_lock.insert(blocks.sender, committee); debug!("My lock here is {:?}", *my_lock);}} self.process_blocks(blocks).await},
                    ConsensusMessage::NewSyncRequest(digest, num_blocks, origin) => self.handle_new_sync_requests(digest, num_blocks, origin).await,
                    ConsensusMessage::ShiftedChain(author, shifted_chain, round_number) => self.handle_shifted_chain(author, shifted_chain, round_number).await,
                    _ => panic!("Unexpected protocol message")
                },
                Some(block) = self.rx_loopback.recv() => self.process_block(&block).await,
                () = &mut self.timer => self.local_timeout_round().await,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}
