package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"
	"treeless/src/tlcom"
	"treeless/src/tlheartbeat"
	"treeless/src/tlserver"
	"treeless/src/tlsg"
)

func main() {
	rand.Seed(time.Now().UnixNano())
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
	localIP := flag.String("localip", tlcom.GetLocalIP(), "set local IP")
	logToFile := flag.Bool("logtofile", false, "set logging to file")

	flag.Parse()

	if *logToFile {
		f, err := os.OpenFile("/mnt/treeless.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Println("Error when opening log file")
			return
		}
		defer f.Close()
		log.SetOutput(f)
	}

	var f *os.File
	if *cpuprofile {
		go func() {
			f, err := os.Create("cpu.prof")
			if err != nil {
				log.Fatal(err)
			}
			time.Sleep(time.Second * 10)
			pprof.StartCPUProfile(f)
		}()
	}

	if *monitor != "" {
		sg, err := tlsg.Assoc(*monitor)
		if err != nil {
			fmt.Println(err)
			return
		}
		//Start heartbeat listener
		hb := tlheartbeat.Start(sg, nil)
		go func() {
			for {
				fmt.Println("\033[H\033[2J" + sg.String())
				time.Sleep(time.Millisecond * 100)
			}
		}()
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		hb.Stop()
		/*s, err := tlsgOLD.ConnectAsClient(*monitor)
		if err != nil {
			fmt.Println("Access couldn't be established")
			fmt.Println(err)
		}
		fmt.Println(s)*/
	} else if *create {
		//TODO 8 parametrizar
		s := tlserver.Start("", *localIP, *port, 8, *redundancy, *dbpath)
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
		s := tlserver.Start(*assoc, *localIP, *port, 8, *redundancy, *dbpath)
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
