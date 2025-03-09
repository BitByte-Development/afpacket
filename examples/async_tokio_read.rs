use afpacket::tokio::RawPacketStream;
use nom::HexDisplay;
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() {
    let mut ps = RawPacketStream::new().unwrap();
    loop {
        let mut buf = [0u8; 1500];
        ps.read(&mut buf).await.unwrap();
        println!("{}", buf.to_hex(24));
    }
}
