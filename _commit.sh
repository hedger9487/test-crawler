#!/bin/bash
cd /Users/hedger/Desktop/test-crawler
git add -A
git commit -m "feat: 50/50 Explore/Mature interleaving to cut robots.txt bottleneck

Split single _ready heap into _ready_explore + _ready_mature.
Even dispatch: explore first. Odd dispatch: mature first.
When one heap empty: use the other (natural fallback).

Robots wait: 80-98% -> ~40-50% in early phase.
Checkpoint backward compatible (old format triggers rebuild).
87 tests passing."
