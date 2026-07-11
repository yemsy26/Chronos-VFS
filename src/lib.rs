pub mod aura_bridge {
    pub struct AuraAstNode {
        pub node_id: u64,
        pub parent_id: u64,
        pub opcode: u8,
        pub content_hash: String,
        pub intent: String,
        pub meta: [u8; 16],
    }

    pub struct AuraIntentTranslator;
    impl AuraIntentTranslator {
        pub fn tokenize_intent(
            opcode: u8,
            parent_id: u64,
            node_id: u64,
            intent: &str,
            meta: [u8; 16],
        ) -> AuraAstNode {
            AuraAstNode {
                node_id,
                parent_id,
                opcode,
                content_hash: format!("{:x}", md5::compute(intent.as_bytes())), // mock hash
                intent: intent.to_string(),
                meta,
            }
        }
    }
}

pub mod workspace {
    use super::aura_bridge::AuraAstNode;
    
    /// Type alias for the default workspace node type
    pub type DefaultNode = AuraAstNode;

    pub struct AgentWorkspace<T> {
        nodes: Vec<T>,
        max_size: usize,
    }

    impl<T> AgentWorkspace<T> {
        pub fn new(max_size: usize) -> Result<Self, String> {
            Ok(Self {
                nodes: Vec::new(),
                max_size,
            })
        }

        pub fn push_node(&mut self, node: T) -> Result<(), String> {
            if self.nodes.len() >= self.max_size {
                return Err("Buffer full".to_string());
            }
            self.nodes.push(node);
            Ok(())
        }
    }
}
