package main

import (
	"flag"
	"fmt"
	"treeless/src/com"
)

func main() {
	create := flag.Bool("create", false, "Create a new DB")
	asoc := flag.String("asoc", "", "Associate to an existing DB")
	port := flag.String("port", "9876", "Use this port as the localhost server port")
	monitor := flag.String("monitor", "", "Monitor an existing DB")
	flag.Parse()
	if *monitor != "" {
		ac, err := tlcom.ConnectAsClient(*monitor)
		if err != nil {
			fmt.Println("Access couldn't be established")
			fmt.Println(err)
		} else {
			fmt.Println(ac)
		}
	} else if *asoc != "" {
		tlcom.Start(false, *asoc, *port)
		select {}
	} else if *create {
		tlcom.Start(true, "", *port)
		select {}
	} else {
		fmt.Println("No operations requested.")
		flag.Usage()
	}
}
