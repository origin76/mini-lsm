// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator + 'static, B: StorageIterator + 'static> {
    a: A,
    b: B,
    // Add fields as need
    current_is_a: bool,
    valid: bool,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            current_is_a: true,
            valid: false,
        };

        iter.advance_to_valid()?;

        Ok(iter)
    }

    fn advance_to_valid(&mut self) -> Result<()> {
        // Reset validity
        self.valid = false;

        // Check if both iterators are valid
        let a_valid = self.a.is_valid();
        let b_valid = self.b.is_valid();

        // If both are invalid, we're done
        if !a_valid && !b_valid {
            println!("Both iterators are invalid");
            return Ok(());
        }

        // If only A is valid, use A
        if a_valid && !b_valid {
            self.current_is_a = true;
            self.valid = true;
            return Ok(());
        }

        // If only B is valid, use B
        if !a_valid && b_valid {
            self.current_is_a = false;
            self.valid = true;
            return Ok(());
        }

        // Both are valid, compare keys and choose the smaller one
        // A takes precedence when keys are equal
        let a_key = self.a.key();
        let b_key = self.b.key();
        let comparison = a_key.cmp(&b_key);

        match comparison {
            std::cmp::Ordering::Less => {
                self.current_is_a = true;
                self.valid = true;
            }
            std::cmp::Ordering::Equal => {
                // A takes precedence when keys are equal
                self.current_is_a = true;
                self.valid = true;
            }
            std::cmp::Ordering::Greater => {
                self.current_is_a = false;
                self.valid = true;
            }
        }

        Ok(())
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        // Return the key from the appropriate iterator based on current_is_a
        if self.current_is_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        // Return the value from the appropriate iterator based on current_is_a
        if self.current_is_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.valid
    }

    fn next(&mut self) -> Result<()> {
        // Advance the current iterator
        if self.a.is_valid() && self.b.is_valid() {
            if self.a.key() == self.b.key() {
                self.a.next()?;
                self.b.next()?;
                self.advance_to_valid()?;
                return Ok(());
            }
        }

        // If keys are not equal, advance the current iterator
        if self.current_is_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }

        // Determine which iterator should be the current one
        self.advance_to_valid()?;

        Ok(())
    }
}
