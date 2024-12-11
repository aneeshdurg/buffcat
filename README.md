# BuffCat

A heavy-duty file concatenation tool. It's `cat` on steroids.

BuffCat aims to be a fast tool for building files as concatenations of other files.

BuffCat overcomes limitations of `cat`, by providing faster output generation, features to specify
that files should be written multiple times, and with no limits on the number of input files.

The following example shows the output of appending the same file 1000000 times. In 82s, 116GB is
written to disk.

```console
aneesh@orion:~/buffcat$ cargo r -- -r 1000000 lineitem_small.tbl -o lineitem_output.tbl
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.02s
     Running `target/debug/buffcat -r 1000000 lineitem_small.tbl -o lineitem_output.tbl`
100%|██████████████████████████████████████████████████████| 123848000000/123848000000 [01:22<00:00, 1643609143.85it/s]
```
