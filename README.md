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
  -version
        Show version
```

# Development

## Requirements

* Go 1.15
* GNU Make

## Prerequisites

* Install stringer project locally.
  ```
  $ make tools
  ```

## Build

```
$ make
# -> go-split
```

# License

BSD 2-Clause License

SEE LICENSE

# My Environment

* CentOS 8.3.2011
* Go 1.15.11
* GNU Make 4.2.1
