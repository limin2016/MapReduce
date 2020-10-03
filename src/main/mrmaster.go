package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//

import (
	"fmt"
	"mr"
	"os"
	"time"
)

func main() {
	// if the user doesn't give right command line instruction, the code exit
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}
	// os.Args[0] = /var/folders/8k/jkrlr0q1295bskmwq1yxsn3w0000gn/T/go-build701444532/b001/exe/mrmaster
	// os.Args[1:] = [.txt, .txt, ...]
	m := mr.MakeMaster(os.Args[1:], 10)
	// when the mr task is over, quit the main program
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
