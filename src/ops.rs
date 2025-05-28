use std::{future::Future, pin::Pin};

// pub trait AsyncFnOnce {
//     type Args;
//     type Ret;
//     fn call(
//         self,
//         args: Self::Args,
//     ) -> Pin<Box<dyn Future<Output = Self::Ret> + Send + Sync + 'static>>;
// }

pub trait AsyncFn {
    type Args;
    type Ret;
    fn call(
        &self,
        args: Self::Args,
    ) -> Pin<Box<dyn Future<Output = Self::Ret> + Send + Sync + 'static>>;
}
