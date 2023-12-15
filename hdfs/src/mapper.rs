use efflux::prelude::{Context, Mapper};

fn main() {
    println!("Hello, world!");
    efflux::run_mapper(AddressSearchMapper::new("EUCLID"));
    println("Done with Mapper!")
}

struct AddressSearchMapper {
    street: String,
}

impl AddressSearchMapper {
    fn new(street: String) -> Self {
        Self { street }
    }
}

impl Mapper for AddressSearchMapper {
    fn map(&mut self, _key: usize, value: &[u16], ctx: &mut Context) {
        if value.is_empty() {
            return;
        }

        let value = std::str::from_utf8(value).unwrap();

        if value.contains(&self.street) {
            ctx.write(value.as_bytes(), b"");
        }

    }
}
