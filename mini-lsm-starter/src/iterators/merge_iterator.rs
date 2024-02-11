#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        };
        let mut heap = BinaryHeap::new();
        if iters.iter().all(|x| !x.is_valid()) {
            let mut iters = iters;
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        };
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }
        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }
    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        // 获取当前元素：let current = self.current.as_mut().unwrap();这行代码尝试获取当前元素的可变引用。这里假设current总是有值，这是通过unwrap调用强制的，如果current为None，则会引发panic。
        let current = self.current.as_mut().unwrap();
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            // 堆不变性检查：使用debug_assert!宏来确保堆（或优先队列）的不变性没有被破坏，即每个内部迭代器的当前键都不小于current迭代器的键。
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );
            // 处理相同键的情况：如果内部迭代器的当前键与current的键相同，根据内部迭代器是否还有效或遇到错误，进行相应的处理（比如弹出不再有效的迭代器）。
            if inner_iter.1.key() == current.1.key() {
                // Case 1: an error occurred when calling `next`.
                if let e @ Err(_) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }

                // Case 2: iter is no longer valid.
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }
        // 移动到下一个元素：current.1.next()?;尝试将current迭代器移动到下一个元素。
        current.1.next()?;
        //current迭代器可能会失效的原因通常与它遍历的数据结构的状态变化有关。在处理合并多个有序序列的场景中，每个迭代器代表对一个序列的遍历。current迭代器失效可能是因为以下几个原因：
        //遍历完成：如果current迭代器已经遍历完它所对应的序列中的所有元素，那么它就没有更多的元素可以访问了，此时它会被视为不再有效。
        //遇到错误：在迭代过程中，可能会因为各种原因（如IO错误、数据解析错误等）遇到错误。如果在尝试推进current迭代器时发生错误，这可能会导致迭代器不再能继续遍历，从而被视为失效。
        // 动态数据源：如果迭代器背后的数据源是动态变化的（虽然这在合并静态序列的常见用例中较少见），那么在某些情况下，数据源的变化可能会导致迭代器失效。
        // 当current迭代器失效时，需要从堆（或优先队列）中选择下一个迭代器来继续遍历过程，这是为了保证能够顺利遍历合并后的序列中的所有元素。选择下一个迭代器的过程通常是基于堆的顶部元素，因为堆是按照一定的顺序（例如最小堆是按照元素的升序排列）来组织的，这保证了每次从堆中取出的都是当前所有迭代器中指向的最小（或根据排序标准确定的下一个）元素。
        // 在next函数的上下文中，如果current迭代器失效，则尝试从堆中弹出下一个迭代器并将其设置为current迭代器，是为了继续保持遍历过程的连续性和有序性，直到所有序列的所有元素都被遍历完毕。
        //
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        //         访问堆顶的迭代器：

        // if let Some(mut inner_iter) = self.iters.peek_mut()尝试获取堆（或优先队列）顶部的迭代器的可变引用。这里使用的是peek_mut方法，它允许我们查看但不移除堆顶的元素，并且可以修改这个元素。
        // 比较current与堆顶迭代器：

        // if *current < *inner_iter检查当前处理的元素（current）是否小于堆顶迭代器指向的元素。这里的比较通常基于迭代器当前指向的元素的值。如果current小于inner_iter，说明当前元素已经是所有序列中下一个最小的元素，不需要做任何改变。反之，如果inner_iter指向的元素更小，则需要交换current和inner_iter。
        // 交换操作：

        // std::mem::swap(&mut *inner_iter, current);如果堆顶迭代器指向的元素小于当前元素，这行代码将current和堆顶迭代器指向的元素进行交换。std::mem::swap是Rust标准库提供的一个函数，用于交换两个值的内容而不引入临时变量。这里的交换确保了current总是指向所有迭代器中下一个最小的元素。
        // 通过这种方式，这个合并迭代器能够维护一个有序的状态，确保每次调用next方法时，都能按顺序访问到合并序列中的下一个元素。这个逻辑对于实现如外部排序、时间序列数据合并等复杂操作是非常重要的。
        // Otherwise, compare with heap top and swap if necessary.
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            if *current < *inner_iter {
                std::mem::swap(&mut *inner_iter, current);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|x| x.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|x| x.1.num_active_iterators())
                .unwrap_or(0)
    }
}
