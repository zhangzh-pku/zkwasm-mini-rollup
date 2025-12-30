use zkwasm_rest_abi::WithdrawInfo;

/// Settlement is flushed at the end of each bundle
/// 1. The application transactions will push withdraw into settlement
/// 2. All withdraw will get flushed at the end of each bundle (at the preemption point)
pub struct SettlementInfo(Vec<WithdrawInfo>);
pub static mut SETTLEMENT: SettlementInfo = SettlementInfo(vec![]);

impl SettlementInfo {
    pub fn append_settlement(info: WithdrawInfo) {
        unsafe { SETTLEMENT.0.push(info) };
    }
    pub fn flush_settlement() -> Vec<u8> {
        zkwasm_rust_sdk::dbg!("flush settlement\n");
        let sinfo = unsafe { &mut SETTLEMENT };
        let mut bytes: Vec<u8> = Vec::with_capacity(sinfo.0.len() * 32);
        for s in &sinfo.0 {
            s.flush(&mut bytes);
        }
        sinfo.0 = vec![];
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::SettlementInfo;
    use zkwasm_rest_abi::WithdrawInfo;

    #[test]
    fn flush_settlement_clears_buffer_and_preserves_order() {
        let first = WithdrawInfo {
            feature: 1,
            address: [0x11; 20],
            amount: 9,
        };
        let second = WithdrawInfo {
            feature: 2,
            address: [0x22; 20],
            amount: 10,
        };

        SettlementInfo::append_settlement(first);
        SettlementInfo::append_settlement(second);

        let bytes = SettlementInfo::flush_settlement();
        assert_eq!(bytes.len(), 64);

        let first_feature = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let first_amount = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
        assert_eq!(first_feature, 1);
        assert_eq!(first_amount, 9);

        let second_feature = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
        let second_amount = u64::from_be_bytes(bytes[56..64].try_into().unwrap());
        assert_eq!(second_feature, 2);
        assert_eq!(second_amount, 10);

        let empty = SettlementInfo::flush_settlement();
        assert!(empty.is_empty());
    }
}

