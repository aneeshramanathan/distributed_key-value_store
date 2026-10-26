# What this project is, in plain terms

Imagine you have 3–5 copies of a simple key-value database (like a
dictionary: `"hello" → "world"`) running on different machines. You want
them to always agree on what's stored, even if machines crash or the
network gets flaky — and you never want a client to see wrong or stale
data.

**The core problem:** if every machine could just accept writes
independently, they'd disagree with each other constantly. You need them
to act like *one* reliable database even though they're really several
unreliable ones.

## The solution (Raft)

- At any time, the group elects one machine as the **leader**. Only the
  leader accepts writes.
- Every write gets written to an ordered **log** (`"set hello=world"` is
  entry #47), and the leader copies that log entry to the other machines.
- A write only "counts" once more than half the machines have it safely
  saved. That majority rule is why the system survives failures — as long
  as more than half the machines are alive and reachable, the data is
  safe, even if the rest are dead or cut off.
- If the leader dies, the others notice (nobody's heard from it in a
  while) and elect a new one automatically. There are some clever tricks
  to stop this from happening falsely — e.g. a machine that got
  temporarily disconnected shouldn't come back and kick out a perfectly
  fine leader just because it "feels" out of date.

## Why even reads go through this process

You'd think reading a value is simple — just look it up locally. But a
machine's local copy might be stale. So every read, like every write,
goes through the same "ask the leader, wait for it to be confirmed by a
majority" process. That guarantees nobody ever sees outdated data — a
property called **linearizability**.

## The hard part: proving it's correct

It's easy to write code that *looks* right and works when tested
normally. The real risk is subtle bugs that only show up when the network
is behaving badly — messages arriving late, out of order, or machines
crashing at exactly the wrong moment. So this project includes a fake,
"evil" network that deliberately drops messages, delays them, scrambles
their order, and randomly kills and restarts machines — then hammers the
system with random reads/writes while that chaos is happening, and uses
an automated checker (Porcupine) to verify that, despite all that chaos,
the system never let a client see an impossible or inconsistent result.

## In one sentence

It's a tiny, fault-tolerant database that stays correct and available as
long as most of its machines are up, proven by trying as hard as possible
to break it and checking that it never lied to a client.
