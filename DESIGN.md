# duckdb-hudi — 用 hudi-rs 0.5.0 重写内核：闭环设计

> 状态：**设计稿（待 sigil + codex review，未动代码）**
> 作者上下文：Hermes，2026-10-10。所有 API 断言均来自本地 sparse-clone 的 hudi-rs 源码 / quack-rs 0.18 源码 / crates.io 元数据（非猜测）。

---

## 1. 现状与病灶（数据说话）

当前 `duckdb-hudi`（master，3 commit）= **手写 timeline 解析 + `read_parquet([...])` 字符串拼接**，单文件 `src/lib.rs`（304 行）。

| 维度 | 现状 | 问题 |
|---|---|---|
| 读数据 | 3 个标量函数：`hudi_info` / `hudi_files` / `hudi_scan_sql` | **没有 `read_hudi()` 表函数**——数据要靠用户拿 `hudi_scan_sql` 返回的字符串二次执行 |
| `description.yml` | 宣称 `SELECT * FROM read_hudi('s3://...', as_of=>...)` | **文档与实现不符**（宣传的函数根本不存在） |
| COW | 解析最新 commit 的 `partitionToWriteStats`，取 file group 最新 base parquet | 仅能碰巧工作；commit JSON 结构一变就退化为全盘 `*.parquet` glob 兜底（`collect_parquet_files_recursive`），**会把未提交/中间态 parquet 当数据读** |
| MOR | 只读 base parquet，打"数据可能过期"警告 | **不合并 delta log** —— MOR 的核心价值（增量日志）完全丢失，等于假 MOR |
| Time-travel | 无 | `as_of` 不存在 |
| Incremental | 无 | 无 `start_timestamp`/`end_timestamp` |
| 云存储 | `std::fs::read_dir`（纯本地） | **S3/Azure/GS 全不支持**，而 description 宣称支持 s3:// |
| 过滤/投影下推 | 无 | 无 partition pruning / projection pushdown |

**根因**：hand-rolled 内核只覆盖了 COW 的 happy path，MOR/时间旅行/增量/云全是假的。这正是 hudi-rs 0.5.0 成熟后要解决的——它已是"内核"（类比 delta-kernel-rs之于 Delta 扩展）。

---

## 2. 已核实的事实（全部来自源码，非 claim）

### hudi-rs 0.5.0 内核 API（`crates/core` → `hudi_core`，`hudi` crate 是它的再导出）
- `Table::new_with_options(base_uri, options)` → `Result<Table>`（async）
- `Table::get_schema() -> Result<Schema>` / `get_partition_schema()`（async）
- `Table::get_file_slices(&ReadOptions) -> Result<Vec<FileSlice>>`（async）
- `Table::read(&ReadOptions) -> Result<Vec<RecordBatch>>`（async，**完整 MOR 合并**）
- `Table::read_stream(&ReadOptions) -> Result<BoxStream<'static, Result<RecordBatch>>>`（async，**仅 Snapshot**；Incremental 报 "Streaming for incremental queries is not yet supported"）
- `Table::create_file_group_reader_with_options(read_options, extra_storage_overrides) -> FileGroupReader`
- `Table::compute_table_stats(Option<&ReadOptions>) -> Option<(u64 /*rows*/, u64 /*bytes*/)>`
- `Table::table_type() / is_mor() / table_name() / timezone() / base_url() / hudi_options() / storage_options()`
- `Table::read_metadata_table_files_partition_arrow(&keys) -> RecordBatch`（C ABI 已在 jvm-ffi 用）

### `FileSlice`（pub 字段）
`base_file: Option<BaseFile>`, `log_files: BTreeSet<LogFile>`, `partition_path: String`, `base_file_column_stats: Option<StatisticsContainer>` + `has_log_file()/file_id()/creation_instant_time()/total_size_bytes()/log_size_bytes()`。
→ **并行化的天然切分单位**（每 thread 领若干 slice）。

### `ReadOptions`（builder）
`new()` / `with_query_type(QueryType::{Snapshot,Incremental})` / `with_as_of_timestamp` / `with_start_timestamp` / `with_end_timestamp` / `with_batch_size` / `with_filters([(field,op,value)])` / `with_projection([cols])` / `with_hudi_option(k,v)`（`hudi_options: HashMap<String,String>`，per-read，不存表）。
→ 云凭证就是从这里走（`aws_region` 等）。

### hudi-rs 的 C ABI = **独立 crate `crates/jvm-ffi`**（给 JVM 用，#700），非内核本体
导出：`hudi_ffi_table_open / read_metadata_files_partition / free_table / reader_open / read_slice / free_reader / free_stream`，输出 `FFI_ArrowArrayStream`（Arrow C Data Interface），`guard()` 把每个导出函数的 panic 转成 null+`hudi_ffi_last_error`。
→ **我们不走 jvm-ffi**（那是给 JVM 的 C ABI）。我们是 Rust 扩展，直接依赖 `hudi` crate 拿 `RecordBatch`，再自己过 Arrow C Data Interface 进 DuckDB。jvm-ffi 只是"Arrow C Data Interface 出口"这一点的旁证 + panic-guard 范式的参考。

### quack-rs 0.18 的 Arrow C Data Interface 桥（**现成的，已测**）
feature `duckdb-1-5-4`（implies `duckdb-1-5-3`→`duckdb-1-5`）门控 `quack_rs::arrow::{ArrowOptions, ArrowSchema, ArrowArray, schema_from_arrow, data_chunk_from_arrow, data_chunk_to_arrow, to_arrow_schema}`。
- `ArrowArray::take_from(*mut FFI_ArrowArray)` —— 从 arrow-rs 的 `*mut FFI_ArrowArray` 指针 cast 过来，**中性化源端避免 double-free**。
- `libduckdb-sys` 的 `ArrowSchema/ArrowArray` 与 arrow-rs `FFI_ArrowSchema/FFI_ArrowArray` **逐字段一致**（libduckdb-sys 自测断言），桥接是**指针 cast 而非共享 arrow 依赖** → **quack 不依赖 arrow crate，与 hudi 的 arrow 58 零冲突**。
- `data_chunk_from_arrow` 带完整的前置安全 walk（`import_layout::check`）：拒绝 DuckDB 会误读的 layout（struct/list/union/dict/run-end 的 offset 错位、geoarrow.wkb 单向量越界等）。
- **依赖 `libduckdb-sys >= 1.10504.0` + 运行时 DuckDB 1.5.0+**（本机 homebrew 1.5.4 ✓）。

### 构建环境（本机，已探）
- `/opt/homebrew/bin/duckdb` = **v1.5.4**（满足 1.5.0+，可 `LOAD` 真实引擎测试）
- py-duckdb 1.4.5（**仅** Python UDF 路径用，本设计不用）
- `hudi` **0.5.0 已发布到 crates.io** ✓（可 `cargo add hudi@0.5.0`，非 path 依赖，可复现）
- hudi 0.5.0 依赖：**arrow 58**、DataFusion 54、object_store 0.13、Lance 11；feature `spill-rocksdb`（**默认开**，MOR merge 的磁盘层，bindgen 源码编译 RocksDB → 需 libclang + C++ 工具链，Mac 自带 ✓）

---

## 3. 新架构（内核换血，外壳不变）

```
SELECT * FROM read_hudi('/path', as_of=>..., start=>..., end=>..., batch_size=>..., option=>(k=v, ...))
  │
  ▼  [bind]  解析命名参数 → ReadOptions；异步 get_schema() → DuckDB LogicalType 列集（bind 定 schema）
  ▼  [init]  建共享 tokio 运行时；get_file_slices(ReadOptions) → Vec<FileSlice>；set_max_threads；按 slice 分桶给各线程
  ▼  [local_init] 每线程：领本线程 slice 子集 + 建 FileGroupReader
  ▼  [scan]  逐 slice read_file_slice_stream() → BoxStream<RecordBatch>
             每批: FFI 导出 → quack ArrowArray::take_from → schema_from_arrow → data_chunk_from_arrow → 填 output chunk
             扫完 → chunk size 0 → 结束
  ▼  [hudi_info / hudi_files] 改走 hudi-rs：compute_table_stats + get_file_slices → 精确行/字节/文件组/分区/commit 数（替换 hand-rolled glob）
  ▼  [hudi_commits]  新增：get_timeline() 的 commit 列表 + 每 commit 元数据（时间旅行导航）
```

**与现状的关键区别**：数据路径从"拼 SQL 字符串让用户二次执行"变成"表函数内直接产出 DuckDB 原生 DataChunk"，且 MOR 是**真合并**（base+log→record merge，file group reader v2）。

### 命名函数面
| 函数 | 类型 | 说明 |
|---|---|---|
| `read_hudi(uri, as_of?, start?, end?, batch_size?, option?)` | 表函数 | 核心。snapshot + incremental + 时间旅行 |
| `hudi_info(uri)` | 标量 | 精确表元数据（类型/行/字节/文件组/分区/commit 数），走 hudi-rs |
| `hudi_files(uri)` | 标量 | 精确 base+log 文件清单（替代 glob 兜底） |
| `hudi_commits(uri)` | 标量(新) | timeline commit 列表，供时间旅行 |
| `hudi_scan_sql(uri)` | 标量(保留/废弃) | 过渡期保留，指向 `read_hudi(...)` 的等价 SQL |

> 命名参数用 `as_of/start/end/batch_size/option`，**避开 SQL 保留字**（`limit/order/...` 会 parse 失败，见技能教训）。

---

## 4. 关键技术决策（每项已核实可行性）

1. **内核**：`hudi@0.5.0`（crates.io）做 `hudi_core`；**不走 jvm-ffi C ABI**（那是给 JVM 的）。我们 Rust 直接调内核拿 `RecordBatch`。
2. **数据桥**：quack-rs 0.18 `duckdb-1-5-4` 的 Arrow C Data Interface（`data_chunk_from_arrow`）。hudi 的 arrow 58 经 `FFI_ArrowArray` 指针 cast 进 quack 的 `ArrowArray`，**零 arrow 共享依赖、零版本冲突**。
3. **表函数**：quack `TableFunctionBuilder`（raw 回调，要 projection pushdown + 并行）或 `TypedTableFunctionBuilder`。**所有回调必须走 quack 的 `table_bind/init/scan_callback!` 宏**（catch_unwind，防 panic 穿过 FFI abort 掉宿主 DuckDB）。
4. **异步驱动**：共享 **multi-thread** tokio 运行时（`new_multi_thread`）。**不可用 current-thread**：DuckDB 并行 scan 时每个 worker 线程都会进 scan 回调调 `block_on`，current-thread runtime 被 A 线程 block_on 期间 B 线程再 block_on 同 runtime 会死锁/报错。scan 回调里 `runtime.block_on(stream.next().await)` **逐批**驱动（每次 poll 一个 batch，协作式，不长时间阻塞）。multi-thread runtime 允许多线程同时 block_on。
5. **并行**：`get_file_slices` 得到 slice 列表 → `set_max_threads` → 每线程子集。file slice 是天然切分单位。
6. **MOR 真合并**：默认 `spill-rocksdb`（开），大 merge 溢写 RocksDB；`hoodie.read.use.read_optimized.mode=true` 可退化为 base-only（快但 stale）。
7. **下推**：bind 期把 WHERE 列过滤（分区列/数据列）映射成 `ReadOptions::with_filters`（分区裁剪+file 级 stats 裁剪+行级）；投影 → `with_projection`（流式下推到 parquet reader）。
8. **时间旅行/增量**：`as_of` → `with_as_of_timestamp`（snapshot）；`start/end` → `with_start_timestamp/with_end_timestamp`（incremental，走 `read()` 非 stream）。
9. **云存储**：base_uri 带 `s3://`/`az://`/`gs://`，凭证经 `option=>('aws_region'='us-west-2', ...)` 走 `with_hudi_option`（object_store 自动识别 env）。
10. **panic 安全**：quack 宏兜底 + bind 期对 `batch_size`/时间戳格式做范围/格式校验（病态值 → 干净的 bind 错误，不是多 GB 分配或 panic）。

---

## 5. 分阶段实施（P0→P6，每步有验收门）

- **P0 地基打通（最关键，先过这道门才继续）**：Cargo.toml 加 `hudi=0.5.0` + `quack-rs=0.18 {features=["duckdb-1-5-4"]}` + `tokio`；最小 `read_hudi(uri)` 表函数：单 slice，`block_on(read_stream)` 一个 slice → `RecordBatch` → `data_chunk_from_arrow` → **真实 homebrew duckdb 1.5.4 LOAD 后 `SELECT count(*) FROM read_hudi(<本地COW小表>)` 出正确数**。
  - *闸门 A（Arrow-58-FFI round-trip）*：hudi 的 arrow 58 导出的 `FFI_ArrowArray/FFI_ArrowSchema` 经 quack `data_chunk_from_arrow` 进 DuckDB 1.5.4 全类型 round-trip 正确。**已预核实（2026-10-10，源码比对）**：arrow-data 58 的 `FFI_ArrowArray` 与 libduckdb-sys(1.10504) 的 `ArrowArray` 逐字段一致（含 `private_data: *mut c_void` 尾字段，两边都有）——指针 cast 桥接成立，P0 仍用真实数据复验一次。
  - *闸门 B（异 schema slice）*：P0 就用**多 commit 且中间做过 schema evolution 的 COW 表**（commit1: a INT; commit2 加列 b VARCHAR 再写）实测：bind 期 `get_schema()` 拿到的表级 schema 与每个 slice 产出的 `RecordBatch` schema 是否一致（file group reader v2 是否已归一）。不一致 → 需 per-batch 对齐（缺失列补 NULL / cast），P1 处理。
- **P1 数据路径补全**：多 slice + 多线程 + MOR 真合并（base+log）+ 时间旅行（as_of）+ 精确 schema（bind 期 `get_schema`）。✅ 完成（0.2.0）
- **P2 元数据**：`hudi_info`/`hudi_files` 全改走 hudi-rs（`get_timeline`/`get_file_slices`/`hudi_options`），删掉 hand-rolled 解析 + glob 兜底。✅ 完成（0.2.0：P0 重写时误删，P2 批量已按内核 API 重建并回归验证）
- **P3 增量 + 下推**：`start/end` incremental ✅ 完成（0.2.0，走 `read()`，`QueryType::Incremental`）；`with_filters`/`with_projection` 下推（未完成——内核支持 projection，但 DuckDB 侧 pushdown 接线未做）。
- **P4 云存储**：`hudi_option` 凭证透传 ✅ 已接线（`Table::new_with_options` + `with_hudi_option`，本地无凭证路径已验证）；s3/gcs 真桶端到端（本机无凭据，未验证）。
- **P5 测试**：✅ 完成（0.2.0）。in-crate `#[cfg(test)]`：`hudi_option` 解析、query-arg→QueryType 映射（纯逻辑），+ COW/MOR 行数、as_of 时间旅行、incremental 窗口（fixture 对拍，`scripts/materialize.sh` 提取样本表）。SQL 级闸门 = 真实 duckdb 1.5.4 无签名 LOAD 跑完整查询面（bind 在全局 OwnedConnection 上导入，进程内 InMemoryDb 无法复现该状态）。
- **P6 对抗式 review**：✅ 完成（0.2.1）。命令级自核验（15 项 SQL 闸门 + 稳定性/NULL/二次 LOAD/16 线程）+ v1.5.4 源码核验（reference_vector 别名语义、SetSize 契约）+ 外部 agent 审计。
  - *外部 agent 状态（如实记录）*：sigil 两次失败——spinortech flash 驱动在宽审计下 markup 泄漏（已知故障模式）；本地 sglang qwen3.8-27b 一跑产出了有效核查线索但未交完整报告（被 max-steps/上下文截断）。codex 本机跑不通：该 ChatGPT 账号不支持所配模型（gpt-6.1-sol / gpt-5.4 / gpt-5-codex 均 400）。
  - *审计发现并修复（0.2.1）*：
    1. **二次 LOAD 悬垂连接（真 bug，静态推断+源码坐实）**：原 `*GLOBAL_OWNED = Some(owned)` 覆盖会使旧 `OwnedConnection` 被 drop → `duckdb_disconnect` 旧全局连接；而旧连接上已导入的 chunk 可能被 in-flight 查询引用（`duckdb_vector_reference_vector` 是 `dto->Reference(*from)` 零拷贝别名，v1.5.4 源码核验）。修复：`GLOBAL_OWNED` 改 `Vec` 只增不减，进程内所有 LOAD 打开的连接永不 disconnect（进程生命有界，实际无泄漏）。
    2. *sgil 线索核实为内核行为而非扩展缺陷*：`read_snapshot_inner` 在 end_timestamp=None 时返回空——但 `prepare_reader_options` 恒为 Snapshot 补 end=as_of|latest（`resolve_snapshot_timestamp`）、为 Incremental 补 end=latest completion / start=EARLIEST（`resolve_incremental_range`），扩展侧传裸 `ReadOptions` 永远走补全路径，无需处理。
    3. *空表 0 行边界*：`batches` 空时用 `get_schema()` 合成 0 行 batch（`read_hudi_table` 已实现，测试覆盖）。

---

## 6. 已知风险 / 开放问题（review 时重点看）

1. **build 重量**：hudi 0.5.0 拉 arrow58 + DataFusion54 + Lance11 + (默认)RocksDB-from-source（bindgen，需 libclang）。首次 `cargo build --release` 很重（估 15–40min）。缓解：P0 可 `hudi` 用 `default-features=false` 先关 `spill-rocksdb`（纯 COW 不需溢写）把 build 打小，MOR 大 merge 场景再开。
2. **arrow 58 的 `FFI_ArrowArray` 与 libduckdb-sys(1.10504) 的 `ArrowArray` 逐字段一致性**：libduckdb-sys 自测断言它和 *arrow-rs* 一致，但 hudi 用 arrow **58**——需 P0 实测确认 58 的 FFI layout 未被 arrow 58 改动（Arrow C Data Interface 是稳定 ABI，极大概率一致，但要验）。
3. **`read_stream` 不支持 incremental**：增量只能走 `read()`（一次性 `Vec<RecordBatch>`）；增量+流式要等 hudi-rs 后续。
4. **schema 稳定性**：Hudi schema evolution 下不同 slice 可能 schema 不同；bind 期取 `get_schema()`（表级最新 schema），跨 slice cast 需验证（file group reader 内部是否已统一到表 schema）。
5. **`option` 命名参数是 `STRUCT`/`MAP` 还是逐 k=v**：需定 DuckDB 侧形态（倾向 `option => {'aws_region':'us-west-2'}` STRUCT，或 `hudi_option => MAP`）。
6. **RocksDB 缺失时 MOR 大 merge 直接 fail**（`default-features=false` 的行为）——文档需写明。

---

## 7. 交付形态

- 保留 cdylib + `hudi_init_c_api` 入口（quack `init_extension_v2`，`DB_HANDLE` 存 `OwnedConnection` 而非裸 db 指针——现有代码存裸 `duckdb_database`，改存 `OwnedConnection` 更安全）。
- `description.yml` 的 hello_world 与**实现真正对齐**（read_hudi 存在了）。
- 版本 bump 0.1.0 → 0.2.0（内核换血属 breaking：移除 glob 兜底、MOR 行为从"stale base"变"真合并"）。
