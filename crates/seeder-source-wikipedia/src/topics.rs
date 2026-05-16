//! Curated Wikipedia topic list with Sephirot mapping.
//!
//! Spans all 10 Sephirot cognitive domains with ~230 topics. Slugs use
//! Wikipedia's title format (spaces become underscores; the API accepts
//! either form, and `redirects=1` handles aliasing).
//!
//! Re-runs of the seeder should explore different slices of the list,
//! which is why callers can construct a `WikipediaSource` over a rotated
//! view of `SEED_TOPICS` via `rotated_topics(offset)`.

use seeder_common::SephirotDomain;

pub const SEED_TOPICS: &[(&str, SephirotDomain)] = &[
    // ── Keter — meta-learning, AGI, intelligence (general) ──
    ("Artificial_general_intelligence", SephirotDomain::Keter),
    ("Meta-learning_(computer_science)", SephirotDomain::Keter),
    ("Cognitive_architecture", SephirotDomain::Keter),
    ("Intelligence", SephirotDomain::Keter),
    ("Theory_of_multiple_intelligences", SephirotDomain::Keter),
    ("Artificial_consciousness", SephirotDomain::Keter),
    ("Recursive_self-improvement", SephirotDomain::Keter),
    ("Universal_intelligence", SephirotDomain::Keter),
    ("AIXI", SephirotDomain::Keter),
    ("Solomonoff's_theory_of_inductive_inference", SephirotDomain::Keter),
    ("Kolmogorov_complexity", SephirotDomain::Keter),
    ("Algorithmic_information_theory", SephirotDomain::Keter),
    ("Self-aware_computing", SephirotDomain::Keter),
    ("Metacognition", SephirotDomain::Keter),
    ("Global_workspace_theory", SephirotDomain::Keter),

    // ── Chochmah — intuition, pattern, deep physics & insight ──
    ("Pattern_recognition", SephirotDomain::Chochmah),
    ("Heuristic", SephirotDomain::Chochmah),
    ("Insight", SephirotDomain::Chochmah),
    ("Intuition", SephirotDomain::Chochmah),
    ("Gestalt_psychology", SephirotDomain::Chochmah),
    ("Quantum_field_theory", SephirotDomain::Chochmah),
    ("Renormalization_group", SephirotDomain::Chochmah),
    ("Gauge_theory", SephirotDomain::Chochmah),
    ("Spontaneous_symmetry_breaking", SephirotDomain::Chochmah),
    ("Yang-Mills_theory", SephirotDomain::Chochmah),
    ("Quantum_chromodynamics", SephirotDomain::Chochmah),
    ("Path_integral_formulation", SephirotDomain::Chochmah),
    ("Loop_quantum_gravity", SephirotDomain::Chochmah),
    ("AdS/CFT_correspondence", SephirotDomain::Chochmah),
    ("Holographic_principle", SephirotDomain::Chochmah),
    ("Bose-Einstein_condensate", SephirotDomain::Chochmah),
    ("Quantum_simulator", SephirotDomain::Chochmah),
    ("Variational_quantum_eigensolver", SephirotDomain::Chochmah),
    ("Quantum_entanglement", SephirotDomain::Chochmah),
    ("Superposition_principle", SephirotDomain::Chochmah),
    ("Quantum_decoherence", SephirotDomain::Chochmah),
    ("Wave-particle_duality", SephirotDomain::Chochmah),
    ("Bell's_theorem", SephirotDomain::Chochmah),

    // ── Binah — logic, causality, mathematics ──
    ("Propositional_calculus", SephirotDomain::Binah),
    ("First-order_logic", SephirotDomain::Binah),
    ("Causal_inference", SephirotDomain::Binah),
    ("Mathematical_proof", SephirotDomain::Binah),
    ("Category_theory", SephirotDomain::Binah),
    ("Homotopy_type_theory", SephirotDomain::Binah),
    ("Model_theory", SephirotDomain::Binah),
    ("Proof_theory", SephirotDomain::Binah),
    ("Lambda_calculus", SephirotDomain::Binah),
    ("Curry-Howard_correspondence", SephirotDomain::Binah),
    ("Type_theory", SephirotDomain::Binah),
    ("Differential_geometry", SephirotDomain::Binah),
    ("Algebraic_topology", SephirotDomain::Binah),
    ("Number_theory", SephirotDomain::Binah),
    ("Riemann_hypothesis", SephirotDomain::Binah),
    ("Galois_theory", SephirotDomain::Binah),
    ("Stochastic_process", SephirotDomain::Binah),
    ("Markov_chain", SephirotDomain::Binah),
    ("Bayesian_inference", SephirotDomain::Binah),
    ("Probability_theory", SephirotDomain::Binah),
    ("Information_theory", SephirotDomain::Binah),
    ("Game_theory", SephirotDomain::Binah),
    ("Convex_optimization", SephirotDomain::Binah),
    ("Linear_algebra", SephirotDomain::Binah),
    ("Tensor", SephirotDomain::Binah),
    ("Gödel's_incompleteness_theorems", SephirotDomain::Binah),
    ("Turing_machine", SephirotDomain::Binah),
    ("Computability_theory", SephirotDomain::Binah),
    ("Computational_complexity_theory", SephirotDomain::Binah),

    // ── Chesed — exploration, expansion, RL exploration ──
    ("Reinforcement_learning", SephirotDomain::Chesed),
    ("Exploration-exploitation_dilemma", SephirotDomain::Chesed),
    ("Curiosity-driven_learning", SephirotDomain::Chesed),
    ("Intrinsic_motivation_(artificial_intelligence)", SephirotDomain::Chesed),
    ("Monte_Carlo_tree_search", SephirotDomain::Chesed),
    ("Simulated_annealing", SephirotDomain::Chesed),
    ("Genetic_algorithm", SephirotDomain::Chesed),
    ("Evolutionary_computation", SephirotDomain::Chesed),
    ("History_of_artificial_intelligence", SephirotDomain::Chesed),
    ("Dartmouth_workshop", SephirotDomain::Chesed),
    ("AI_winter", SephirotDomain::Chesed),
    ("History_of_cryptocurrency", SephirotDomain::Chesed),
    ("Cybernetic_revolutionaries", SephirotDomain::Chesed),
    ("Open-endedness", SephirotDomain::Chesed),
    ("Novelty_search", SephirotDomain::Chesed),
    ("Quality_diversity_algorithms", SephirotDomain::Chesed),

    // ── Gevurah — safety, constraints, security ──
    ("AI_safety", SephirotDomain::Gevurah),
    ("AI_alignment", SephirotDomain::Gevurah),
    ("Constitutional_AI", SephirotDomain::Gevurah),
    ("Adversarial_machine_learning", SephirotDomain::Gevurah),
    ("Existential_risk_from_artificial_general_intelligence", SephirotDomain::Gevurah),
    ("Reward_hacking", SephirotDomain::Gevurah),
    ("Mesa-optimization", SephirotDomain::Gevurah),
    ("Red_team", SephirotDomain::Gevurah),
    ("Model_collapse", SephirotDomain::Gevurah),
    ("Cryptography", SephirotDomain::Gevurah),
    ("Public-key_cryptography", SephirotDomain::Gevurah),
    ("Zero-knowledge_proof", SephirotDomain::Gevurah),
    ("Homomorphic_encryption", SephirotDomain::Gevurah),
    ("Secure_multi-party_computation", SephirotDomain::Gevurah),
    ("Side-channel_attack", SephirotDomain::Gevurah),
    ("Threat_model", SephirotDomain::Gevurah),
    ("Sandbox_(computer_security)", SephirotDomain::Gevurah),
    ("Differential_privacy", SephirotDomain::Gevurah),

    // ── Tiferet — synthesis, integration, multi-modal ──
    ("Multimodal_learning", SephirotDomain::Tiferet),
    ("Transfer_learning", SephirotDomain::Tiferet),
    ("Few-shot_learning", SephirotDomain::Tiferet),
    ("Zero-shot_learning", SephirotDomain::Tiferet),
    ("Meta-analysis", SephirotDomain::Tiferet),
    ("Hard_problem_of_consciousness", SephirotDomain::Tiferet),
    ("Qualia", SephirotDomain::Tiferet),
    ("Computational_theory_of_mind", SephirotDomain::Tiferet),
    ("Cybernetics", SephirotDomain::Tiferet),
    ("Systems_theory", SephirotDomain::Tiferet),
    ("Autopoiesis", SephirotDomain::Tiferet),
    ("Bayesian_brain", SephirotDomain::Tiferet),
    ("Self-organization", SephirotDomain::Tiferet),
    ("Emergence", SephirotDomain::Tiferet),
    ("Integrated_information_theory", SephirotDomain::Tiferet),
    ("Free_energy_principle", SephirotDomain::Tiferet),
    ("Holism", SephirotDomain::Tiferet),
    ("Holon_(philosophy)", SephirotDomain::Tiferet),
    ("Complex_system", SephirotDomain::Tiferet),

    // ── Netzach — RL, mechanism design, persistence ──
    ("Q-learning", SephirotDomain::Netzach),
    ("Policy_gradient_method", SephirotDomain::Netzach),
    ("Actor-critic", SephirotDomain::Netzach),
    ("Temporal_difference_learning", SephirotDomain::Netzach),
    ("Deep_reinforcement_learning", SephirotDomain::Netzach),
    ("Proximal_Policy_Optimization", SephirotDomain::Netzach),
    ("Mechanism_design", SephirotDomain::Netzach),
    ("Auction_theory", SephirotDomain::Netzach),
    ("Tokenomics", SephirotDomain::Netzach),
    ("Automated_market_maker", SephirotDomain::Netzach),
    ("Stablecoin", SephirotDomain::Netzach),
    ("Maximal_extractable_value", SephirotDomain::Netzach),
    ("Multi-armed_bandit", SephirotDomain::Netzach),
    ("Inverse_reinforcement_learning", SephirotDomain::Netzach),
    ("Markov_decision_process", SephirotDomain::Netzach),
    ("Bellman_equation", SephirotDomain::Netzach),

    // ── Hod — language, communication, software engineering ──
    ("Natural_language_processing", SephirotDomain::Hod),
    ("Transformer_(deep_learning_architecture)", SephirotDomain::Hod),
    ("Attention_(machine_learning)", SephirotDomain::Hod),
    ("Mixture_of_experts", SephirotDomain::Hod),
    ("Reinforcement_learning_from_human_feedback", SephirotDomain::Hod),
    ("Direct_preference_optimization", SephirotDomain::Hod),
    ("Retrieval-augmented_generation", SephirotDomain::Hod),
    ("Large_language_model", SephirotDomain::Hod),
    ("Word_embedding", SephirotDomain::Hod),
    ("Tokenization_(lexical_analysis)", SephirotDomain::Hod),
    ("Byte_pair_encoding", SephirotDomain::Hod),
    ("Federated_learning", SephirotDomain::Hod),
    ("Compiler", SephirotDomain::Hod),
    ("Type_system", SephirotDomain::Hod),
    ("Rust_(programming_language)", SephirotDomain::Hod),
    ("Concurrent_computing", SephirotDomain::Hod),
    ("Distributed_computing", SephirotDomain::Hod),
    ("Consensus_(computer_science)", SephirotDomain::Hod),
    ("Byzantine_fault", SephirotDomain::Hod),
    ("Practical_Byzantine_Fault_Tolerance", SephirotDomain::Hod),
    ("Proof_of_work", SephirotDomain::Hod),
    ("Proof_of_stake", SephirotDomain::Hod),
    ("Blockchain", SephirotDomain::Hod),
    ("Smart_contract", SephirotDomain::Hod),
    ("Speech_recognition", SephirotDomain::Hod),
    ("Machine_translation", SephirotDomain::Hod),

    // ── Yesod — memory, foundation, biology of cognition ──
    ("Long_short-term_memory", SephirotDomain::Yesod),
    ("Episodic_memory", SephirotDomain::Yesod),
    ("Knowledge_graph", SephirotDomain::Yesod),
    ("Semantic_network", SephirotDomain::Yesod),
    ("Hippocampus", SephirotDomain::Yesod),
    ("Long-term_potentiation", SephirotDomain::Yesod),
    ("Predictive_coding", SephirotDomain::Yesod),
    ("Active_inference", SephirotDomain::Yesod),
    ("Mirror_neuron", SephirotDomain::Yesod),
    ("Default_mode_network", SephirotDomain::Yesod),
    ("Brain-computer_interface", SephirotDomain::Yesod),
    ("Connectome", SephirotDomain::Yesod),
    ("Synaptic_plasticity", SephirotDomain::Yesod),
    ("Neural_oscillation", SephirotDomain::Yesod),
    ("Working_memory", SephirotDomain::Yesod),
    ("Recurrent_neural_network", SephirotDomain::Yesod),
    ("Gated_recurrent_unit", SephirotDomain::Yesod),
    ("Hopfield_network", SephirotDomain::Yesod),
    ("Vector_database", SephirotDomain::Yesod),
    ("Approximate_nearest_neighbor", SephirotDomain::Yesod),
    ("Hierarchical_Navigable_Small_World", SephirotDomain::Yesod),

    // ── Malkuth — action, embodiment, applied compute ──
    ("Robotics", SephirotDomain::Malkuth),
    ("Embodied_cognition", SephirotDomain::Malkuth),
    ("Sensor_fusion", SephirotDomain::Malkuth),
    ("CUDA", SephirotDomain::Malkuth),
    ("Tensor_processing_unit", SephirotDomain::Malkuth),
    ("Photonic_computing", SephirotDomain::Malkuth),
    ("Quantum_processor", SephirotDomain::Malkuth),
    ("Neuromorphic_engineering", SephirotDomain::Malkuth),
    ("Mechatronics", SephirotDomain::Malkuth),
    ("Computer_vision", SephirotDomain::Malkuth),
    ("Simultaneous_localization_and_mapping", SephirotDomain::Malkuth),
    ("Lidar", SephirotDomain::Malkuth),
    ("Inertial_measurement_unit", SephirotDomain::Malkuth),
    ("Actuator", SephirotDomain::Malkuth),
    ("Self-driving_car", SephirotDomain::Malkuth),
    ("Drone_(aircraft)", SephirotDomain::Malkuth),
    ("Industrial_robot", SephirotDomain::Malkuth),
    ("Manipulator_(device)", SephirotDomain::Malkuth),
    ("Haptic_technology", SephirotDomain::Malkuth),
    ("3D_printing", SephirotDomain::Malkuth),
    ("Field-programmable_gate_array", SephirotDomain::Malkuth),
    ("Application-specific_integrated_circuit", SephirotDomain::Malkuth),
];

/// Returns a `Vec` of `SEED_TOPICS` rotated left by `offset` so successive
/// seeder runs explore different slices first. Useful for keeping a
/// long-running swarm from always re-fetching the same head of the list.
///
/// Example: `rotated_topics(seed % SEED_TOPICS.len())`.
pub fn rotated_topics(offset: usize) -> Vec<(&'static str, SephirotDomain)> {
    if SEED_TOPICS.is_empty() {
        return Vec::new();
    }
    let n = SEED_TOPICS.len();
    let k = offset % n;
    let mut out = Vec::with_capacity(n);
    out.extend_from_slice(&SEED_TOPICS[k..]);
    out.extend_from_slice(&SEED_TOPICS[..k]);
    out
}

pub fn distribution() -> [usize; 10] {
    let mut d = [0usize; 10];
    for (_, domain) in SEED_TOPICS {
        d[*domain as usize] += 1;
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn covers_all_ten_sephirot_domains() {
        let d = distribution();
        for (i, count) in d.iter().enumerate() {
            assert!(*count > 0, "domain {i} has no topics");
        }
    }

    #[test]
    fn has_enough_topics() {
        assert!(SEED_TOPICS.len() >= 200, "got {}", SEED_TOPICS.len());
    }

    #[test]
    fn rotated_topics_is_a_permutation() {
        let r = rotated_topics(37);
        assert_eq!(r.len(), SEED_TOPICS.len());
        // Same multiset of slugs.
        let mut a: Vec<_> = SEED_TOPICS.iter().map(|(s, _)| *s).collect();
        let mut b: Vec<_> = r.iter().map(|(s, _)| *s).collect();
        a.sort();
        b.sort();
        assert_eq!(a, b);
    }

    #[test]
    fn rotated_topics_with_offset_zero_matches_seed_order() {
        let r = rotated_topics(0);
        for (i, (s, _)) in r.iter().enumerate() {
            assert_eq!(*s, SEED_TOPICS[i].0);
        }
    }
}
