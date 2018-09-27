go-split
===

Split text files into parts.

```
Usage: go-split [options] input-file...
  -compress string
        {gzip|other=without compression}
  -parallelism int
        Maximum number of files which read parallely (default 4)
  -prefix string
        Path prefix of outputs (default "out-")
  -split int
        Number of files that splitted (default 8)
  -verbose
        Verbose output
```

# Development

## Requirements

* Go 1.10
* dep
* GNU make

## Build

```bash
$ dep ensure
$ make
```

# License

BSD 2-Clause License

SEE LICENSE

# My Environment

* CentOS 7.5
* Go 1.10.3
* dep 0.5.0
* GNU make 3.82
