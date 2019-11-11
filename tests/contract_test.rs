use std::io;

use kvs::contract::KvContractMessage;

#[test]
fn make_and_parse() {
    let c = KvContractMessage::remove("hello".to_owned());
    let bc = c.clone().into_binary();
    let reader = io::Cursor::new(bc.as_slice());
    let cr = KvContractMessage::parse(reader).expect("Failed to parse.");
    assert_eq!(c, cr);
}
