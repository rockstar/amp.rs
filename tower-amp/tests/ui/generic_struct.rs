//! The attribute carries a struct's generics onto the `Command` impl.
use tower_amp::{Command, command};

#[command(name = "Cmd", response = ())]
struct Cmd<T> {
    v: T,
}

fn main() {
    let _ = Cmd { v: 1u8 };
    assert_eq!(<Cmd<String> as Command>::NAME, "Cmd");
}
