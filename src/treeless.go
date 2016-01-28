package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"treeless/src/sg"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	//Operations
	create := flag.Bool("create", false, "Create a new DB server group")
	assoc := flag.String("assoc", "", "Associate to an existing DB server group")
	monitor := flag.String("monitor", "", "Monitor an existing DB")
	//Options
	port := flag.Int("port", 9876, "Use this port as the localhost server port")
	redundancy := flag.Int("redundancy", 2, "Redundancy of the new DB server group")
	dbpath := flag.String("dbpath", "tmp_DB", "Filesystem path to store DB info")
	cpuprofile := flag.Bool("cpuprofile", false, "write cpu profile to file")

	flag.Parse()

	var f *os.File
	if *cpuprofile {
		f, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	if *monitor != "" {
		s, err := tlsg.ConnectAsClient(*monitor)
		if err != nil {
			fmt.Println("Access couldn't be established")
			fmt.Println(err)
		}
		fmt.Println(s)
	} else if *create {
		//TODO 8 parametrizar
		s := tlsg.Start("", *port, 8, *redundancy, *dbpath)
		go func() {
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt)
			<-c
			log.Println("Interrupt signal recieved")
			if *cpuprofile {
				pprof.StopCPUProfile()
				f.Close()
				fmt.Println("Profiling output generated")
				fmt.Println("View the pprof graph with:")
				fmt.Println("go tool pprof --png treeless cpu.prof > a.png")
			}
			s.Stop()
			log.Println("Server stopped")
			os.Exit(0)
		}()
		select {}
	} else if *assoc != "" {
		s := tlsg.Start(*assoc, *port, 8, *redundancy, *dbpath)
		go func() {
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt)
			<-c
			log.Println("Interrupt signal recieved")
			if *cpuprofile {
				pprof.StopCPUProfile()
				f.Close()
				fmt.Println("Profiling output generated")
				fmt.Println("View the pprof graph with:")
				fmt.Println("go tool pprof --png treeless cpu.prof > a.png")
			}
			s.Stop()
			log.Println("Server stopped")
			os.Exit(0)
		}()
		select {}
	} else {
		log.Fatal("No operations passed. See usage with --help.")
	}
}
