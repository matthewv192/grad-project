# SKILL: q/kdb+ Developer

You are a fully qualified q/kdb+ developer. When writing q code, always follow the conventions, idioms, and best practices documented by KX. When uncertain about syntax or behaviour, defer to the references listed below — never guess.

---

## Primary References

Always consult these sources (in priority order) when writing or reviewing q code:

1. **KX Reference:** https://code.kx.com/q/ref/ — Definitive language reference for all built-in functions, operators, and keywords
2. **KX Learn q:** https://code.kx.com/q/learn/ — Tutorials, idioms, and worked examples
3. **KX Database:** https://code.kx.com/q/database/ — Splayed tables, partitioned databases, HDB/RDB patterns, `.Q` namespace
4. **KX Knowledge Base:** https://code.kx.com/q/kb/ — Best practices, performance tips, common patterns
5. **KX White Papers:** https://code.kx.com/q/wp/ — In-depth technical guidance (temporal data, IPC, memory management)
6. **TorQ Documentation:** https://aquaqanalytics.github.io/TorQ/ — TorQ framework conventions, process management, logging
7. **q for Mortals (Jeffry Borror):** https://code.kx.com/q4m3/ — Comprehensive textbook, excellent for idioms and data modelling

---

## Language Fundamentals

### Evaluation Order
q evaluates **right to left** with no operator precedence. Every expression is parsed this way.
```q
/ This is 2 * (3 + 4) = 14, NOT (2*3) + 4
2 * 3 + 4   / → 14
```
Ref: https://code.kx.com/q/basics/syntax/

### Atoms, Lists & Type System
- q is **typed**. Every atom has a type char and short. Know them:
  - `7h` = long, `9h` = float, `-11h` = symbol atom, `0h` = general list
- Empty typed lists: `` `long$() ``, `` `symbol$() ``, etc.
- Type promotion follows specific rules. Never assume — check with `type` and `meta`.
- Ref: https://code.kx.com/q/basics/datatypes/

### Null & Infinity
- Every type has its own null: `0N` (long), `0n` (float), `` ` `` (symbol), `0Np` (timestamp), `0Nd` (date)
- Every numeric type has infinity: `0W` (long), `0w` (float), `0Wp` (timestamp)
- **Nulls sort to the bottom** (are considered smallest). This matters for `asc`, `xasc`, `aj`.
- Ref: https://code.kx.com/q/ref/null/

### Temporal Types
- **date** (`2024.01.15`), **time** (`09:30:00.000`), **timestamp** (`2024.01.15D09:30:00.000000000`)
- Arithmetic works naturally: `.z.d - 1` gives yesterday
- Cast between them: `` `date$timestamp ``, `` `timestamp$date ``
- Ref: https://code.kx.com/q/basics/datatypes/#temporal

---

## Functions & Lambdas

```q
/ Explicit arguments (preferred for clarity)
f:{[x;y] x + y}

/ Implicit arguments x, y, z (use only for trivial one-liners)
f:{x + y}

/ Multiple statements separated by ;
process:{[data;cfg]
    filtered: select from data where sym in cfg`syms;
    agg: select avg price, sum size by sym from filtered;
    agg
 }
```

- Functions return the **last expression** evaluated
- Max 8 explicit parameters. If you need more, pass a dictionary.
- Use **local assignment** (`:`) inside functions. Global assignment (`::`) only when explicitly needed.
- Ref: https://code.kx.com/q/basics/function-notation/

### Projection & Composition
```q
add10: 10 +          / projection of +
f: {x * x} {x + 1}  / composition: f[3] → (3+1)^2 = 16... NO — right to left: square then add? No.
/ Be explicit. Use explicit composition:
f: {(x+1) * (x+1)}  / clearer
```
Ref: https://code.kx.com/q/basics/application/#projection

### Error Handling
```q
/ Protected evaluation — critical for production code
@[function; argument; errorHandler]
.[function; argList; errorHandler]

/ Examples
@[{1%x}; 0; {`error}]                    / catch divide-by-zero
.[insert; (`trades; newData); {.lg.e[`loader;"Insert failed: ",x]}]
```
Ref: https://code.kx.com/q/ref/apply/#trap

---

## Tables & qSQL

### Table Creation
```q
/ Keyed table
kt: ([sym:`symbol$()] name:`symbol$(); exchange:`symbol$())

/ Unkeyed table
t: ([] sym:`symbol$(); time:`timestamp$(); price:`float$(); size:`long$())
```

### qSQL — SELECT, EXEC, UPDATE, DELETE
```q
/ select — always returns a table
select avg price, sum size by sym from trades where date = 2024.01.15, sym in `AAPL`MSFT

/ exec — returns columns (lists) or atoms
exec distinct sym from trades

/ update — returns modified copy (does NOT mutate unless you assign back)
update adj_price: price * factor from trades

/ delete — removes rows or columns
delete from trades where size = 0
```

**Critical rules:**
- Column names in qSQL are **not quoted**. They are bare names.
- `by` clause creates a keyed table.
- Aggregations without `by` return a single-row table.
- **where clauses are applied left to right** — put the most restrictive filter first (especially `date` in partitioned tables).
- Ref: https://code.kx.com/q/basics/qsql/

### Functional qSQL
Use functional form when column names or conditions are dynamic:
```q
/ Functional select: ?[table; whereClause; groupBy; columns]
?[trades; enlist (=; `sym; enlist `AAPL); 0b; `price`size ! `price`size]

/ Functional update
![trades; (); 0b; (enlist `adj) ! enlist (*; `price; `factor)]
```
Ref: https://code.kx.com/q/basics/funsql/

---

## Joins — Know When To Use Each

| Join | Syntax | Use case |
|------|--------|----------|
| **ij** (inner) | `t1 ij t2` | Match on key columns, keep matches only |
| **lj** (left) | `t1 lj t2` | Enrich t1 with columns from t2, keep all t1 rows |
| **uj** (union) | `t1 uj t2` | Combine tables, fill nulls for missing columns |
| **aj** (as-of) | `aj[\`sym\`time; t1; t2]` | Point-in-time lookup — **the most important join in kdb+** |
| **wj** (window) | `wj[windows; \`sym\`time; t1; (t2; (agg; \`col))]` | Aggregate t2 within time windows around t1 |

### aj (as-of join) — Critical for this project
```q
/ For each row in t1, find the most recent matching row in t2
/ where t2.sym = t1.sym AND t2.time <= t1.time
result: aj[`sym`time; marketData; refData]
```
- t2 **must be sorted** by the join columns (`` `sym`time `` or `` `sym`date ``)
- This is how you do point-in-time lookups for adjustment factors, security master, etc.
- Ref: https://code.kx.com/q/ref/aj/

---

## On-Disk Database (HDB) Patterns

### Partitioned Tables
```
hdb/
├── 2024.01.15/
│   ├── trades/
│   │   ├── sym
│   │   ├── time
│   │   ├── price
│   │   └── size
│   └── ohlcv_1m/
│       ├── sym
│       ├── time
│       ├── open
│       └── ...
├── 2024.01.16/
│   └── ...
└── sym              ← enumeration file
```

- **Partition column** (typically `date`) is the directory name — it is NOT stored as a column on disk
- Data within each partition must be **sorted by `sym then time** (`` `sym xasc `time xasc table ``)  for aj and query performance
- The `sym` file at the HDB root is the **symbol enumeration**. All symbol columns must be enumerated against it.
- Ref: https://code.kx.com/q/database/partition/

### Writing Partitions
```q
/ Standard pattern for writing a single date partition
saveTrade:{[hdbRoot; dt; data]
    path: hsym `$string[hdbRoot],"/",string[dt],"/trades/";
    sorted: `sym`time xasc data;
    enumerated: .Q.en[hsym `$string hdbRoot; sorted];
    path set enumerated;
 }
```

### Loading & Maintenance
```q
/ Load an HDB
\l /path/to/hdb

/ After adding partitions, refresh in-memory view
.Q.chk[hsym `$hdbRoot]   / fill missing tables with empty versions
\l .                       / reload

/ Or use system "l ." for reload
```
Ref: https://code.kx.com/q/ref/dotq/#chk-fill-hdb

### .Q Namespace — Essential Functions
| Function | Purpose |
|----------|---------|
| `.Q.en[dir;table]` | Enumerate symbol columns against sym file |
| `.Q.chk[dir]` | Fill missing partitioned tables |
| `.Q.dpft[dir;part;`p#field;tableName]` | Save partitioned table with parted attribute |
| `.Q.ind[table;indices]` | Index into partitioned table |
| `.Q.V table` | Get column dictionary of table |

Ref: https://code.kx.com/q/ref/dotq/

---

## Attributes — Critical for Performance

| Attribute | Syntax | Effect |
|-----------|--------|--------|
| **`s#** (sorted) | `` `s#list `` | Binary search. Apply to time columns. |
| **`p#** (parted) | `` `p#list `` | Group index. Apply to `sym` in partitioned tables. |
| **`g#** (grouped) | `` `g#list `` | Hash index. Good for low-cardinality lookups. |
| **`u#** (unique) | `` `u#list `` | Hash for unique values. Good for key columns. |

For HDB partitions: **always apply `p# to sym** via `.Q.dpft` or manually.
Ref: https://code.kx.com/q/ref/set-attribute/

---

## IPC & Inter-Process Communication

```q
/ Open a connection
h: hopen `::5001          / localhost port 5001
h: hopen `:host:port:user:pass

/ Synchronous call
result: h "select from trades where date = .z.d"

/ Asynchronous call (fire and forget)
(neg h) "insert[`trades; newData]"

/ Close
hclose h
```
Ref: https://code.kx.com/q/basics/ipc/

---

## TorQ-Specific Conventions

When writing code for a TorQ package:

### Process Management
- Processes are defined in `config/process.csv`
- Each process type has its own config in `config/`
- Bootstrap with `torq.q`: `q ${TORQHOME}/torq.q -load ${PACKAGEHOME}/code/loader.q`

### Logging
```q
/ TorQ logging — use these, not -1 or 0N!
.lg.o[`component; "Info message"]       / info
.lg.e[`component; "Error message"]      / error
.lg.w[`component; "Warning message"]    / warning
```

### Timer
```q
/ TorQ timer — schedule recurring tasks
.timer.addTimer[`functionName; period; description]
```

### Configuration
- Use `.proc.params` for command-line arguments
- Store settings in config files loaded at startup
- Ref: https://aquaqanalytics.github.io/TorQ/

---

## File I/O

### Reading
```q
/ Read CSV with types
trades: ("DSPFJJ"; enlist ",") 0: `:staging/trades.csv
/ Type chars: D=date, S=symbol, P=timestamp, F=float, J=long, C=char, *=infer

/ Read lines
lines: read0 `:file.txt

/ Read binary
data: read1 `:file.bin
```

### Writing
```q
/ Save as CSV
`:output.csv 0: csv 0: table

/ Save splayed table
`:hdb/2024.01.15/trades/ set .Q.en[`:hdb; table]

/ Save single object
`:path/obj set variable
```
Ref: https://code.kx.com/q/ref/file-text/

---

## Common Idioms & Patterns

### String Handling
```q
/ Symbols are NOT strings. Strings are char lists.
s: `AAPL              / symbol
str: "AAPL"           / string (char list)
`$str                 / string → symbol
string s              / symbol → string

/ Concatenation
"hello"," ","world"   / → "hello world"
```

### Dictionary & Table Manipulation
```q
/ Create dict
d: `a`b`c ! 1 2 3

/ Table from dicts
t: flip `sym`price ! (`AAPL`MSFT; 150.0 280.0)

/ Upsert
`t upsert ([] sym: enlist `GOOG; price: enlist 140.0)
```

### Iteration
```q
/ each — apply to each element
f each list

/ peach — parallel each (use for CPU-bound work)
f peach list

/ each-both — pairwise
list1 f' list2

/ over & scan
(+/) 1 2 3 4     / fold: 10
(+\) 1 2 3 4     / scan: 1 3 6 10
```
Ref: https://code.kx.com/q/ref/maps/

---

## Performance Rules

1. **Vector operations over loops.** Never iterate row-by-row when a vector operation exists.
2. **Filter early.** In partitioned queries, `date` constraint first, then `sym`, then other filters.
3. **Minimise memory copies.** Use update-in-place (`` `table set ... ``) where appropriate.
4. **Enumerate symbols.** Always `.Q.en` before saving to disk.
5. **Apply attributes.** `p#sym` on partitioned tables. `s#time` where needed.
6. **Avoid generic lists.** Typed lists are faster and use less memory. Use `()` only when truly mixed types.
7. **Profile with `\t`.** Use `\t expression` to time critical code paths.

Ref: https://code.kx.com/q/kb/performance-tips/

---

## Error-Prone Areas — Be Careful

- **`select from t where col = val`** — if `val` is a variable holding a list, you need `in` not `=`
- **Amend vs assign:** `t[`col]: newVals` modifies in place; `update col: newVals from t` returns a copy
- **Enlist for single-row inserts:** `insert[`t; enlist row]` not `insert[`t; row]`
- **Partition column not stored on disk** — don't include `date` as a column when saving splayed partitioned data; it's derived from the directory name
- **sym file corruption** — never write to an HDB from two processes simultaneously. Enumerate carefully.
- **aj requires sorted data** — if your as-of join returns unexpected nulls, check sort order first

---

## Debugging

```q
/ Show value/definition
value functionName

/ Table metadata
meta tableName

/ Count
count tableName

/ Type inspection
type variable      / short type code
.Q.ty variable     / char type code

/ System commands
\v                 / list variables
\f                 / list functions
\a                 / list tables
\w                 / memory usage
\t expr            / time expression
```

---

## When Writing q Code For This Project

1. Always comment non-obvious logic with `/` comments explaining **why**
2. Use descriptive function and variable names — this is a grad project, clarity over brevity
3. Handle errors with protected evaluation (`@[;;]` / `.[;;]`) in all I/O and loading code
4. Log with TorQ's `.lg.o` / `.lg.e` — never use bare `-1` or `0N!` in production code
5. Test edge cases: empty tables, missing partitions, null values, single-row inputs
6. When in doubt about a built-in function, check https://code.kx.com/q/ref/ before writing custom logic
