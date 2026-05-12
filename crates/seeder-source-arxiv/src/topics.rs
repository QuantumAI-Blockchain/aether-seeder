//! Curated arXiv topic list with Sephirot mapping.
//!
//! Hand-picked papers that a frontier AI should have read. Heavily
//! weighted toward foundational ML, attention/transformers,
//! decentralized training, alignment, and quantum computing — the
//! intellectual diet of the Aether Mind.

use seeder_common::SephirotDomain;

pub const SEED_TOPICS: &[(&str, SephirotDomain)] = &[
    // ── Hod — transformers, language, ML systems ──
    ("1706.03762", SephirotDomain::Hod), // Attention Is All You Need
    ("1810.04805", SephirotDomain::Hod), // BERT
    ("2005.14165", SephirotDomain::Hod), // GPT-3
    ("2203.02155", SephirotDomain::Hod), // InstructGPT
    ("2305.18290", SephirotDomain::Hod), // DPO
    ("2307.09288", SephirotDomain::Hod), // Llama 2
    ("2407.10671", SephirotDomain::Hod), // Qwen2
    ("2303.08774", SephirotDomain::Hod), // GPT-4 technical report
    ("2106.09685", SephirotDomain::Hod), // LoRA
    ("2305.14314", SephirotDomain::Hod), // QLoRA
    ("2104.09864", SephirotDomain::Hod), // RoPE
    ("2305.13245", SephirotDomain::Hod), // Grouped-query attention (GQA)
    ("2401.04088", SephirotDomain::Hod), // Mixtral of Experts
    ("1911.02150", SephirotDomain::Hod), // Multi-query attention
    ("2204.02311", SephirotDomain::Hod), // PaLM
    ("2206.07682", SephirotDomain::Hod), // Emergent Abilities (Wei et al)

    // ── Yesod — memory, retrieval, augmented architectures ──
    ("2005.11401", SephirotDomain::Yesod), // RAG
    ("2112.04426", SephirotDomain::Yesod), // RETRO
    ("2310.11511", SephirotDomain::Yesod), // Self-RAG
    ("2402.03216", SephirotDomain::Yesod), // BGE reranker
    ("1607.06450", SephirotDomain::Yesod), // Layer Norm
    ("2305.13245", SephirotDomain::Yesod), // KV cache / GQA
    ("2104.05952", SephirotDomain::Yesod), // Hopfield networks

    // ── Binah — distributed training / optimization / theory ──
    ("2311.08105", SephirotDomain::Binah), // DiLoCo
    ("2301.11913", SephirotDomain::Binah), // SWARM Parallelism
    ("1811.06965", SephirotDomain::Binah), // GPipe
    ("1909.08053", SephirotDomain::Binah), // Megatron-LM
    ("1910.02054", SephirotDomain::Binah), // ZeRO
    ("2104.07857", SephirotDomain::Binah), // ZeRO-Infinity
    ("2007.07314", SephirotDomain::Binah), // PowerSGD
    ("1901.09269", SephirotDomain::Binah), // signSGD
    ("1904.09848", SephirotDomain::Binah), // Local SGD
    ("1610.05492", SephirotDomain::Binah), // Federated learning (McMahan)
    ("2105.04663", SephirotDomain::Binah), // BFT in adversarial federated learning

    // ── Gevurah — AI safety, alignment, evals ──
    ("2212.08073", SephirotDomain::Gevurah), // Constitutional AI
    ("2210.10760", SephirotDomain::Gevurah), // Discovering Latent Knowledge
    ("2202.03286", SephirotDomain::Gevurah), // Red Teaming LMs
    ("2402.13228", SephirotDomain::Gevurah), // Sleeper Agents
    ("1906.01820", SephirotDomain::Gevurah), // AI Safety via Debate
    ("2009.03300", SephirotDomain::Gevurah), // MMLU
    ("2110.14168", SephirotDomain::Gevurah), // GSM8K
    ("2109.07958", SephirotDomain::Gevurah), // TruthfulQA
    ("1803.05457", SephirotDomain::Gevurah), // ARC

    // ── Chochmah — quantum computing & physics ──
    ("1411.4028", SephirotDomain::Chochmah), // QAOA
    ("1304.3061", SephirotDomain::Chochmah), // VQE original
    ("1812.10773", SephirotDomain::Chochmah), // QML survey
    ("1905.10876", SephirotDomain::Chochmah), // Expressibility of PQCs
    ("2012.09265", SephirotDomain::Chochmah), // QNN trainability
    ("2103.07585", SephirotDomain::Chochmah), // Variational quantum algorithms review

    // ── Tiferet — synthesis / consciousness theory / integration ──
    ("0712.1374", SephirotDomain::Tiferet),  // IIT (Tononi)
    ("2105.11521", SephirotDomain::Tiferet), // Free energy principle (Friston)

    // ── Netzach — reinforcement, multi-agent, mechanism design ──
    ("2206.11795", SephirotDomain::Netzach), // PPO follow-ups
    ("1707.06347", SephirotDomain::Netzach), // PPO
    ("1502.05477", SephirotDomain::Netzach), // TRPO
    ("1707.06887", SephirotDomain::Netzach), // C51
    ("1710.06542", SephirotDomain::Netzach), // Rainbow DQN

    // ── Malkuth — applied compute / decentralized systems ──
    ("2206.01288", SephirotDomain::Malkuth), // Petals (collaborative inference)
    ("2206.06550", SephirotDomain::Malkuth), // BLOOM
    ("1810.00440", SephirotDomain::Malkuth), // Mixed-precision training
    ("2208.07339", SephirotDomain::Malkuth), // 8-bit optimizers
    ("2306.00978", SephirotDomain::Malkuth), // AWQ quantization
    ("2306.03078", SephirotDomain::Malkuth), // GPTQ
    ("2310.11453", SephirotDomain::Malkuth), // BitNet 1-bit LLMs

    // ── Chesed — history of ideas / divergent contributions ──
    ("2206.10498", SephirotDomain::Chesed), // Beyond the Imitation Game
    ("2007.05558", SephirotDomain::Chesed), // The Scaling Hypothesis (intro)

    // ── Keter — intentionally empty (meta-cognition is emergent) ──
];

pub fn distribution() -> [usize; 10] {
    let mut d = [0usize; 10];
    for (_, domain) in SEED_TOPICS {
        d[*domain as usize] += 1;
    }
    d
}
