package main

import (
	"flag"
	"fmt"
	"github.com/hyperhq/hyper/client"
	"os"
)

func main() {
	var (
		proto = "unix"
		addr  = "/var/run/hyper.sock"
	)
	cli := client.NewHyperClient(proto, addr, nil)

	// set the flag to output
	flHelp := flag.Bool("help", false, "Help Message")
	flVersion := flag.Bool("version", false, "Version Message")
	flag.Usage = func() { cli.Cmd("help") }
	flag.Parse()
	if flag.NArg() == 0 {
		cli.Cmd("help")
		return
	}
	if *flHelp == true {
		cli.Cmd("help")
	}
	if *flVersion == true {
		cli.Cmd("version")
	}

	if err := cli.Cmd(flag.Args()...); err != nil {
		if sterr, ok := err.(client.StatusError); ok {
			if sterr.Status != "" {
				fmt.Printf("%s ERROR: %s\n", os.Args[0], err.Error())
				os.Exit(-1)
			}
			os.Exit(sterr.StatusCode)
		}

		fmt.Printf("%s ERROR: %s\n", os.Args[0], err.Error())
		os.Exit(-1)
	}
}
