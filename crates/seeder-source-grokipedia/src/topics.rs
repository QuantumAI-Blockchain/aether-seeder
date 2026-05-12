//! Curated seed topic list with Sephirot domain mapping.
//!
//! Each entry is a tuple of (slug, sephirot domain). The Python reference
//! implementation (`scripts/seed_from_grokipedia.py`) tagged articles with
//! coarse categories like `physics`, `mathematics`, `computer_science`,
//! etc. Those don't map 1:1 to the 10 Sephirot the Aether Mind actually
//! shards on; we re-tag them here using a deliberate mapping documented
//! in `category_to_sephirot()` below.

use seeder_common::SephirotDomain;

/// (slug, domain) pairs. ~100 articles spanning the 10 Sephirot.
pub const SEED_TOPICS: &[(&str, SephirotDomain)] = &[
    // ── Chochmah — intuition, patterns, quantum/physics ──
    ("Quantum_computing", SephirotDomain::Chochmah),
    ("Quantum_mechanics", SephirotDomain::Chochmah),
    ("Quantum_entanglement", SephirotDomain::Chochmah),
    ("General_relativity", SephirotDomain::Chochmah),
    ("Standard_Model", SephirotDomain::Chochmah),
    ("Supersymmetry", SephirotDomain::Chochmah),
    ("String_theory", SephirotDomain::Chochmah),
    ("Dark_matter", SephirotDomain::Chochmah),
    ("Higgs_boson", SephirotDomain::Chochmah),
    ("Thermodynamics", SephirotDomain::Chochmah),
    ("Electromagnetic_radiation", SephirotDomain::Chochmah),
    ("Wave-particle_duality", SephirotDomain::Chochmah),
    ("Black_hole", SephirotDomain::Chochmah),
    ("Big_Bang", SephirotDomain::Chochmah),
    ("Solar_System", SephirotDomain::Chochmah),
    ("Exoplanet", SephirotDomain::Chochmah),
    ("Milky_Way", SephirotDomain::Chochmah),

    // ── Binah — logic, causal inference, mathematics ──
    ("Golden_ratio", SephirotDomain::Binah),
    ("Fibonacci_sequence", SephirotDomain::Binah),
    ("Group_theory", SephirotDomain::Binah),
    ("Topology", SephirotDomain::Binah),
    ("Game_theory", SephirotDomain::Binah),
    ("Cryptography", SephirotDomain::Binah),
    ("Information_theory", SephirotDomain::Binah),
    ("Graph_theory", SephirotDomain::Binah),
    ("Bayesian_statistics", SephirotDomain::Binah),
    ("Chaos_theory", SephirotDomain::Binah),

    // ── Hod — language, semantics, computer science ──
    ("Artificial_intelligence", SephirotDomain::Hod),
    ("Machine_learning", SephirotDomain::Hod),
    ("Neural_network", SephirotDomain::Hod),
    ("Deep_learning", SephirotDomain::Hod),
    ("Natural_language_processing", SephirotDomain::Hod),
    ("Computer_vision", SephirotDomain::Hod),
    ("Blockchain", SephirotDomain::Hod),
    ("Distributed_computing", SephirotDomain::Hod),
    ("Algorithm", SephirotDomain::Hod),
    ("Turing_machine", SephirotDomain::Hod),
    ("Compiler", SephirotDomain::Hod),
    ("Operating_system", SephirotDomain::Hod),
    ("Linguistics", SephirotDomain::Hod),
    ("Semantics", SephirotDomain::Hod),
    ("Syntax", SephirotDomain::Hod),

    // ── Yesod — memory, fusion, neuroscience/biology ──
    ("Consciousness", SephirotDomain::Yesod),
    ("Neuroscience", SephirotDomain::Yesod),
    ("Integrated_information_theory", SephirotDomain::Yesod),
    ("Global_workspace_theory", SephirotDomain::Yesod),
    ("Neuroplasticity", SephirotDomain::Yesod),
    ("Memory", SephirotDomain::Yesod),
    ("Cognitive_science", SephirotDomain::Yesod),
    ("Artificial_general_intelligence", SephirotDomain::Yesod),
    ("Free_energy_principle", SephirotDomain::Yesod),
    ("Theory_of_mind", SephirotDomain::Yesod),
    ("Evolution", SephirotDomain::Yesod),
    ("DNA", SephirotDomain::Yesod),
    ("Cell_(biology)", SephirotDomain::Yesod),
    ("Genetics", SephirotDomain::Yesod),
    ("Photosynthesis", SephirotDomain::Yesod),
    ("Psychology", SephirotDomain::Yesod),
    ("Cognitive_bias", SephirotDomain::Yesod),

    // ── Tiferet — integration, synthesis, philosophy ──
    ("Philosophy_of_mind", SephirotDomain::Tiferet),
    ("Epistemology", SephirotDomain::Tiferet),
    ("Logic", SephirotDomain::Tiferet),
    ("Ontology", SephirotDomain::Tiferet),
    ("Existentialism", SephirotDomain::Tiferet),
    ("Phenomenology_(philosophy)", SephirotDomain::Tiferet),
    ("Philosophy_of_science", SephirotDomain::Tiferet),
    ("Emergence", SephirotDomain::Tiferet),
    ("Complexity_theory", SephirotDomain::Tiferet),

    // ── Gevurah — safety, constraints, ethics ──
    ("Ethics", SephirotDomain::Gevurah),

    // ── Chesed — exploration, divergent, history & culture ──
    ("History_of_science", SephirotDomain::Chesed),
    ("Scientific_revolution", SephirotDomain::Chesed),
    ("Industrial_Revolution", SephirotDomain::Chesed),
    ("Internet", SephirotDomain::Chesed),
    ("World_Wide_Web", SephirotDomain::Chesed),

    // ── Netzach — reinforcement, reward, economics ──
    ("Economics", SephirotDomain::Netzach),
    ("Cryptocurrency", SephirotDomain::Netzach),
    ("Decentralized_finance", SephirotDomain::Netzach),
    ("Supply_and_demand", SephirotDomain::Netzach),
    ("Monetary_policy", SephirotDomain::Netzach),
    ("Decision-making", SephirotDomain::Netzach),

    // ── Malkuth — action, world interaction, applied ──
    ("Electrical_engineering", SephirotDomain::Malkuth),
    ("Semiconductor", SephirotDomain::Malkuth),
    ("Transistor", SephirotDomain::Malkuth),
    ("Integrated_circuit", SephirotDomain::Malkuth),
    ("Immune_system", SephirotDomain::Malkuth),
    ("Vaccine", SephirotDomain::Malkuth),
    ("CRISPR", SephirotDomain::Malkuth),
    ("Nuclear_fusion", SephirotDomain::Malkuth),
    ("Renewable_energy", SephirotDomain::Malkuth),
    ("Entropy", SephirotDomain::Malkuth),
    ("Chemistry", SephirotDomain::Malkuth),
    ("Periodic_table", SephirotDomain::Malkuth),
    ("Chemical_bond", SephirotDomain::Malkuth),
    ("Organic_chemistry", SephirotDomain::Malkuth),
    ("Ecology", SephirotDomain::Malkuth),

    // ── Keter — meta-learning, goals ──
    // (Keter is reserved for self-referential / meta-cognitive topics;
    //  intentionally sparse here — those should be generated by Aether,
    //  not seeded.)
];

/// How many topics map to each Sephirot. Useful for debugging routing balance.
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
    fn topics_are_non_empty() {
        assert!(SEED_TOPICS.len() > 50, "expect at least 50 seed topics");
    }

    #[test]
    fn every_topic_has_a_slug() {
        for (slug, _) in SEED_TOPICS {
            assert!(!slug.is_empty(), "empty slug in SEED_TOPICS");
            assert!(!slug.contains(' '), "slug `{}` contains space — should use underscore", slug);
        }
    }

    #[test]
    fn most_sephirot_are_represented() {
        let d = distribution();
        let represented = d.iter().filter(|n| **n > 0).count();
        // Keter intentionally has zero; require at least 8 of 10 are populated.
        assert!(represented >= 8, "only {} Sephirot domains have topics: {:?}", represented, d);
    }
}
