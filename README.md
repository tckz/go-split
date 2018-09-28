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

* Go 1.11
* dep
* stringer
* GNU make

## Prerequisites

* Install dep  
  https://golang.github.io/dep/docs/installation.html
* Install stringer
  ```bash
  $ go get golang.org/x/tools/cmd/stringer
  ```

## Build

```bash
$ dep ensure
$ go generate
$ make
```

# License

BSD 2-Clause License

SEE LICENSE

# My Environment

* CentOS 7.5
* Go 1.11
* dep 0.5.0
* GNU make 3.82
