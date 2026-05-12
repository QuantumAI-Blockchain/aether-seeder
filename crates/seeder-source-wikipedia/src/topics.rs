//! Curated Wikipedia topic list with Sephirot mapping.
//!
//! Intentionally complementary to GrokipediaSource — more advanced or
//! specialized articles that fill in domains Grokipedia is sparse on.
//! Slugs use Wikipedia's title format (spaces become underscores; the
//! API accepts either form, redirects=1 handles aliasing).

use seeder_common::SephirotDomain;

pub const SEED_TOPICS: &[(&str, SephirotDomain)] = &[
    // ── Chochmah — quantum/physics, deeper than Grokipedia's set ──
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

    // ── Binah — mathematics & logic ──
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

    // ── Hod — language, computer science, software ──
    ("Transformer_(deep_learning_architecture)", SephirotDomain::Hod),
    ("Attention_(machine_learning)", SephirotDomain::Hod),
    ("Mixture_of_experts", SephirotDomain::Hod),
    ("Reinforcement_learning_from_human_feedback", SephirotDomain::Hod),
    ("Direct_preference_optimization", SephirotDomain::Hod),
    ("Retrieval-augmented_generation", SephirotDomain::Hod),
    ("Federated_learning", SephirotDomain::Hod),
    ("Differential_privacy", SephirotDomain::Hod),
    ("Compiler_construction", SephirotDomain::Hod),
    ("Type_system", SephirotDomain::Hod),
    ("Rust_(programming_language)", SephirotDomain::Hod),
    ("Concurrency_(computer_science)", SephirotDomain::Hod),
    ("Distributed_consensus", SephirotDomain::Hod),
    ("Byzantine_fault", SephirotDomain::Hod),
    ("Practical_Byzantine_Fault_Tolerance", SephirotDomain::Hod),
    ("Proof_of_work", SephirotDomain::Hod),
    ("Proof_of_stake", SephirotDomain::Hod),

    // ── Yesod — memory, neuroscience, biology, cognition ──
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
    ("Embodied_cognition", SephirotDomain::Yesod),

    // ── Tiferet — synthesis, integration, philosophy of mind/science ──
    ("Hard_problem_of_consciousness", SephirotDomain::Tiferet),
    ("Qualia", SephirotDomain::Tiferet),
    ("Computational_theory_of_mind", SephirotDomain::Tiferet),
    ("Cybernetics", SephirotDomain::Tiferet),
    ("Systems_theory", SephirotDomain::Tiferet),
    ("Autopoiesis", SephirotDomain::Tiferet),
    ("Bayesian_brain", SephirotDomain::Tiferet),
    ("Self-organization", SephirotDomain::Tiferet),
    ("Emergence", SephirotDomain::Tiferet),

    // ── Gevurah — safety, alignment, ethics ──
    ("AI_alignment", SephirotDomain::Gevurah),
    ("AI_safety", SephirotDomain::Gevurah),
    ("Existential_risk_from_artificial_general_intelligence", SephirotDomain::Gevurah),
    ("Reward_hacking", SephirotDomain::Gevurah),
    ("Mesa-optimization", SephirotDomain::Gevurah),
    ("Constitutional_AI", SephirotDomain::Gevurah),
    ("Red_team", SephirotDomain::Gevurah),

    // ── Chesed — exploration, history of ideas, divergent thought ──
    ("History_of_artificial_intelligence", SephirotDomain::Chesed),
    ("Dartmouth_workshop", SephirotDomain::Chesed),
    ("AI_winter", SephirotDomain::Chesed),
    ("Cybernetic_revolutionaries", SephirotDomain::Chesed),
    ("History_of_cryptocurrency", SephirotDomain::Chesed),

    // ── Netzach — reward, reinforcement, economics, game theory ──
    ("Mechanism_design", SephirotDomain::Netzach),
    ("Auction_theory", SephirotDomain::Netzach),
    ("Tokenomics", SephirotDomain::Netzach),
    ("Automated_market_maker", SephirotDomain::Netzach),
    ("Stablecoin", SephirotDomain::Netzach),
    ("MEV_(blockchain)", SephirotDomain::Netzach),
    ("Multi-armed_bandit", SephirotDomain::Netzach),
    ("Q-learning", SephirotDomain::Netzach),
    ("Policy_gradient", SephirotDomain::Netzach),

    // ── Malkuth — applied / physical world / engineering ──
    ("CUDA", SephirotDomain::Malkuth),
    ("Tensor_processing_unit", SephirotDomain::Malkuth),
    ("Photonic_computing", SephirotDomain::Malkuth),
    ("Quantum_processor", SephirotDomain::Malkuth),
    ("Neuromorphic_engineering", SephirotDomain::Malkuth),
    ("Mechatronics", SephirotDomain::Malkuth),
    ("Robotics", SephirotDomain::Malkuth),
    ("Solid_oxide_fuel_cell", SephirotDomain::Malkuth),
    ("Smart_contract", SephirotDomain::Malkuth),

    // ── Keter — intentionally sparse (meta-cognition is emergent, not seeded) ──
];

pub fn distribution() -> [usize; 10] {
    let mut d = [0usize; 10];
    for (_, domain) in SEED_TOPICS {
        d[*domain as usize] += 1;
    }
    d
}
