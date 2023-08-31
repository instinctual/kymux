// Utils to be able to use Rc/RefCell on wasm and Arc/Mutex on other platforms.

use std::ops::{Deref, DerefMut};

#[cfg(not(target_family = "wasm"))]
pub(crate) type KyArc<T> = std::sync::Arc<T>;
#[cfg(target_family = "wasm")]
pub(crate) type KyArc<T> = std::rc::Rc<T>;

#[cfg(not(target_family = "wasm"))]
pub(crate) struct KyMutex<T>(std::sync::Mutex<T>);
#[cfg(target_family = "wasm")]
pub(crate) struct KyMutex<T>(std::cell::RefCell<T>);

#[cfg(not(target_family = "wasm"))]
pub(crate) struct KyMutexGuard<'a, T: ?Sized + 'a>(std::sync::MutexGuard<'a, T>);
#[cfg(target_family = "wasm")]
pub(crate) struct KyMutexGuard<'a, T: ?Sized + 'a>(std::cell::RefMut<'a, T>);

impl<T> KyMutex<T> {
    #[inline(always)]
    pub(crate) fn new(t: T) -> Self {
        #[cfg(not(target_family = "wasm"))]
        let inner = std::sync::Mutex::new(t);
        #[cfg(target_family = "wasm")]
        let inner = std::cell::RefCell::new(t);

        KyMutex(inner)
    }

    #[inline(always)]
    pub(crate) fn lock(&self) -> KyMutexGuard<'_, T> {
        #[cfg(not(target_family = "wasm"))]
        let inner = self.0.lock().unwrap();
        #[cfg(target_family = "wasm")]
        let inner = self.0.borrow_mut();

        KyMutexGuard(inner)
    }
}

impl<T: ?Sized> Deref for KyMutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.0.deref()
    }
}

impl<T: ?Sized> DerefMut for KyMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.0.deref_mut()
    }
}
