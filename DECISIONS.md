# spark-spill-simulator — Problem

Have you ever wondered how Spark runs aggregate functions over huge
amounts of data? The obvious answer is that it splits the data into
partitions spread across workers — but that only explains how the
*input* is handled.

The harder question is: how does something like a `groupby` work on
data that's spread across a cluster and, in total, is far bigger than
any single machine's RAM? Unlike an operation like `map`, which
processes each record independently with no shared state, a
`groupby`/aggregation has to accumulate a running result *per key*
across every record it sees — and that running accumulator lives in
memory.

If that accumulator is allowed to grow without limit, it can eventually
exceed available RAM, and the OS's out-of-memory killer will terminate
the whole operation. So the risk was never really about the size of the
input data — the input can be streamed in gradually. The real risk is
the accumulator itself, growing in RAM as it tracks more and more
distinct keys.

The solution: once the accumulator approaches a memory budget, spill its
current contents to disk and clear it, freeing RAM to keep processing.
At the end, merge everything — the spilled pieces plus whatever's left
in memory — into one correct final result. This project builds a small,
observable version of that exact mechanism.

## Design

#### Data generator
A `yield`-based generator, not a pre-built list. An early test confirmed
why: materializing 500,000 records as a list upfront cost ~92MB,
sitting in memory for the whole run regardless of what the accumulator
itself was doing — which would have muddied any memory measurement.
The generator version costs ~208 bytes no matter how many records it
eventually produces, since only one record exists in memory at a time.

Each record is `{"category": f"category_{i}", "amount": <random int>}`.
Categories are deliberately all-unique (`category_0` to `category_N-1`)
rather than using something like `faker`, because the thing that
actually grows the accumulator dict is the number of *distinct* keys,
not the number of records — a guaranteed-unique, dependency-free
generator serves that purpose better than a library that needs explicit
uniqueness constraints.

#### Accumulator
The groupby accumulator uses each category as a dict key:
`totals[key] = totals.get(key, 0) + value`

#### Trigger mechanism
Check the estimated memory usage only on new-key addition (updating an
existing key doesn't grow the dict, so there's no need to check then).
If the estimate exceeds the configured memory budget, spill.

**First attempt — `sys.getsizeof()`:** chosen initially because it's a
real measurement rather than a fixed guess, and the project doesn't
need perfect precision, just a trigger that's directionally correct
enough to keep memory bounded. This turned out to be a flawed
assumption: `sys.getsizeof()` on a dict only measures the dict's own
internal hash-table scaffolding, not the actual string/int objects
stored as keys and values. A direct test confirmed it: a dict stayed at
184 bytes across 1 to 3 entries, only growing in large, infrequent
jumps as the underlying hash table resized. Testing against a 100,000
byte budget produced only one spill at N=5,000,000, even though the
real process memory (measured via `ru_maxrss`) was ~547MB. The
estimate was badly under-reporting real memory usage.

**Second attempt — `resource.ru_maxrss` as the trigger:** switched to
measuring real process memory directly. This introduced a worse bug:
`ru_maxrss` tracks the *peak* memory ever reached, and never decreases
even after the accumulator is cleared. Once it crossed the budget once,
every subsequent new-key event also exceeded the (permanently-elevated)
peak, so the function tried to spill on nearly every remaining key —
tens of millions of near-empty JSON files were created before the
process was force-killed.

**Final choice — `psutil`:** `psutil.Process().memory_info().rss`
reports *current* resident memory, not a historical peak, which fixes
the `ru_maxrss` problem — memory usage correctly drops back down after
a spill clears the dict, so the trigger only fires again once real
usage climbs back up. It also measures the same thing as the project's
`ru_maxrss`-based baseline (whole-process memory), so the two are
directly comparable, unlike `sys.getsizeof()`'s narrower, incomplete
number.

The real cost of this choice: `psutil.Process().memory_info().rss` is
a genuine OS-level system call, not a cheap in-Python read. Checking it
on every new-key event (which, in this generator, is nearly every
record) adds real, measurable overhead — see Results below.

#### Spill
Write the current dict to disk as a uniquely-named JSON file
(`json_file_{count}.json`), *then* clear the in-memory dict — in that
order, so the record that triggered the spill (already added to the
dict before the size check runs) isn't lost.

This raised a real question: if a category that was already spilled to
disk appears again later in the stream, it's no longer in memory — how
do we accumulate it with the partial sum already spilled? Two options:

- **A.** Treat it as a new key and start a fresh running sum in memory.
  At the end, merge every spilled file's partial sums with whatever's
  in memory to get the correct final totals.
- **B.** Go back to the spilled file on disk immediately and merge the
  new value into it there, keeping everything in one file per category.

Chosen: **A**. The simple-sounding reason is that it's easier to
implement — but the real reason is Chapter 6's memory hierarchy: disk
access costs roughly 100,000x more than a RAM access. Option B would
mean going back to disk on every reappearing key, which is far slower
and more complicated than deferring all of that cost to a single merge
pass at the end.

#### Merge
Treat every spilled file's contents, plus whatever's left in memory at
the end, as new input, and run the same accumulation logic
(`left_dict[key] = left_dict.get(key, 0) + value`) once more to produce
the final, correct totals.

## Correctness

Naive (unbounded) and spilling versions were run on identical input
(seeding `random` before each call, since `gen()` otherwise produces
different random amounts each time it's called) and their final results
compared directly with `==`. Result: **exactly equal**, at both
N=1,000,000 and N=5,000,000. The spill-and-merge mechanism produces the
same correct output as the naive baseline.

## Results

Budget set to 100MB for all spilling runs.

| N | Version | Peak memory | Runtime | Spills |
|---|---------|-------------|---------|--------|
| 1,000,000 | Naive | 283.5 MB | 0.59 s | — |
| 1,000,000 | Spilling | 239.7 MB | 16.0 s | 1 |
| 5,000,000 | Naive | 1088.2 MB | 3.41 s | — |
| 5,000,000 | Spilling | 650.0 MB | 81.4 s | 7 |

**Memory:** at N=5,000,000, spilling used ~650MB against naive's
~1088MB — roughly a 40% reduction. Worth being honest about: this is
not tightly bounded to the 100MB budget. The gap comes from the
`psutil` check only running on new-key events, combined with how
quickly real memory can grow between checks — the trigger is
directionally correct, not precise.

**Runtime:** spilling is roughly 24-28x slower than naive at both
scales. This is the real, accepted cost of this implementation's
correctness-and-bounded-memory guarantee: a genuine OS system call
(`psutil`) on nearly every new key, plus real disk writes for each
spill. Same tradeoff shape as Chapter 6's cache design lessons — safer
or more bounded behavior isn't free, it costs something measurable
elsewhere.

