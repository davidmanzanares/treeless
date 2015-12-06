package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"time"
	"treeless/src/com"
	"treeless/src/server"
)

func main() {
	//Operations
	assoc := flag.String("assoc", "", "Associate to an existing DB server group")
	monitor := flag.String("monitor", "", "Monitor an existing DB")
	//Options
	port := flag.String("port", "9876", "Use this port as the localhost server port")
	redundancy := flag.Int("redundancy", 2, "Redundancy of the new DB server group")
	dbpath := flag.String("dbpath", "tmpDB"+time.Now().String(), "Filesystem path to store DB info")
	cpuprofile := flag.Bool("cpuprofile", false, "write cpu profile to file")

	flag.Parse()

	if *cpuprofile {
		f, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		go func() {
			time.Sleep(time.Second * 10)
			pprof.StopCPUProfile()
			f.Close()
			fmt.Println("Profiling output generated")
			fmt.Println("View the pprof graph with:")
			fmt.Println("go tool pprof --png treeless cpu.prof > a.png")
		}()
	}

	if *monitor != "" {
		_, err := tlcom.ConnectAsClient(*monitor)
		if err != nil {
			fmt.Println("Access couldn't be established")
			fmt.Println(err)
		} else {
			select {}
		}
	} else {
		tlserver.Start(*assoc, *port, *redundancy, *dbpath)
		select {}
	}
}
