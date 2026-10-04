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

#### Accumulator
The groupby accumulator uses each category as a dict key:
`totals[key] = totals.get(key, 0) + value`

#### Trigger mechanism
Check the estimated byte size only on new-key addition (updating an
existing key doesn't grow the dict, so there's no need to check then).
If the estimate exceeds the configured memory budget, spill.

For the size estimate itself: a fixed assumed size per entry (e.g. "100
bytes per key") would be wrong in a way that's hard to justify, since
Python's dynamic typing means actual sizes vary — a short string and a
long string, or a small int and a large one, don't cost the same number
of bytes. `sys.getsizeof()` gives a real measurement instead. It's only
called on new-key events, which are infrequent, so the cost concern that
ruled out calling it on every record doesn't apply here. The goal isn't
perfect precision — it's a trigger that's directionally correct enough
to keep memory bounded and produce an explainable benchmark, and
`sys.getsizeof()` gets that for free at no real added cost.

#### Spill
Write the current dict to disk as a file, then clear it from memory,
and proceed with processing.

This raises a real question: if a category that was already spilled to
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
the end, as new input, and run the same accumulation logic once more to
produce the final, correct totals.