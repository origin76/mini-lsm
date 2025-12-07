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

use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    /// 记录一个新的活跃读取时间戳
    pub fn add_reader(&mut self, ts: u64) {
        // 如果该 ts 已存在，计数 +1；否则初始化为 1
        *self.readers.entry(ts).or_insert(0) += 1;
    }

    /// 移除一个读取时间戳（当事务结束或 Drop 时调用）
    pub fn remove_reader(&mut self, ts: u64) {
        // 获取该 ts 的计数器
        if let Some(count) = self.readers.get_mut(&ts) {
            *count -= 1;
            // 如果计数归零，说明没有任何事务在看这个时间点了
            // 必须将其从 Map 中彻底删除，否则水位线（最小 Key）无法向前推进
            if *count == 0 {
                self.readers.remove(&ts);
            }
        }
    }

    /// 返回当前追踪的不同时间戳数量（用于统计/调试）
    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }

    /// 获取当前的全局水位线（最小的活跃读取时间戳）
    /// 如果没有活跃事务，返回 None，意味着可以回收任意旧的数据（除了最新版）
    pub fn watermark(&self) -> Option<u64> {
        // BTreeMap 是有序的，keys().next() 返回的是最小的 Key
        self.readers.keys().next().cloned()
    }
}
