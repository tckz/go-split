package main

import (
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/tckz/go-split"
)

var version string

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage: %s [options] input-file...\n", path.Base(os.Args[0]))
	flag.PrintDefaults()
}

func main() {
	optVersion := flag.Bool("version", false, "Show version")

	param := split.Param{}
	param.Verbose = flag.Bool("verbose", false, "Verbose output")
	param.Split = flag.Int("split", 8, "Number of files that splitted")
	param.Prefix = flag.String("prefix", "out-", "Path prefix of outputs")
	param.Compress = flag.String("compress", "", "{gzip|other=without compression}")
	param.Parallelism = flag.Int("parallelism", 4, "Maximum number of files which read parallely")

	flag.Usage = usage
	flag.Parse()

	if *optVersion {
		fmt.Fprintf(os.Stdout, "%s\n", version)
		return
	}

	files := flag.Args()
	if len(files) == 0 {
		usage()
		fmt.Fprintf(os.Stderr, "*** One or more input files must be specified\n")
		os.Exit(1)
	}

	if *param.Parallelism <= 0 {
		usage()
		fmt.Fprintf(os.Stderr, "*** --parallelism must be >= 1")
		os.Exit(1)
	}

	if *param.Split <= 0 {
		usage()
		fmt.Fprintf(os.Stderr, "*** --split must be >= 1")
		os.Exit(1)
	}

	split.NewSplitter().Do(files, param)
}
