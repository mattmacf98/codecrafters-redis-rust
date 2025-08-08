pub mod master;
pub mod replica;

pub trait Instance {
    fn start(&self);
}