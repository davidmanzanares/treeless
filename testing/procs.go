package test

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type procServer struct {
	id     int
	phy    string
	dbpath string
	cmd    *exec.Cmd
}

var ramonly = false

var localIP = "127.0.0.1"

func procStartCluster(numServers int) []testServer {
	dbTestFolder := ""
	if exists("/mnt/dbs/") && !ramonly {
		dbTestFolder = "/mnt/dbs/"
	}

	l := make([]testServer, numServers)
	ps := new(procServer)
	ps.phy = string(localIP + ":" + fmt.Sprint(10000))
	if ramonly {
		ps.dbpath = ""
	} else {
		ps.dbpath = dbTestFolder + "testDB" + fmt.Sprint(0)
	}
	l[0] = ps
	for i := 1; i < numServers; i++ {
		ps = new(procServer)
		ps.id = i
		ps.phy = string(localIP + ":" + fmt.Sprint(10000+i))
		if ramonly {
			ps.dbpath = ""
		} else {
			ps.dbpath = dbTestFolder + "testDB" + fmt.Sprint(i)
		}
		l[i] = ps
	}
	return l
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func (ps *procServer) addr() string {
	return ps.phy
}

func (ps *procServer) create(numChunks, redundancy int, verbose bool) string {
	ps.kill()
	exec.Command("killall", "-q", "treeless").Run()
	ps.cmd = exec.Command("./treeless", "-create", "-port",
		fmt.Sprint(10000+ps.id), "-dbpath", ps.dbpath, "-localip", localIP,
		"-redundancy", fmt.Sprint(redundancy), "-procs", "1", "-chunks", fmt.Sprint(numChunks))
	if verbose {
		ps.cmd.Stdout = os.Stdout
		ps.cmd.Stderr = os.Stderr
	}
	err := ps.cmd.Start()
	cmdCopy := ps.cmd
	go func() {
		cmdCopy.Wait()
	}()
	if err != nil {
		panic(err)
	}
	waitForServer(ps.phy)
	return ps.phy
}

func (ps *procServer) assoc(addr string, verbose bool) string {
	ps.cmd = exec.Command("./treeless", "-assoc", addr, "-port",
		fmt.Sprint(10000+ps.id), "-dbpath", ps.dbpath, "-localip", localIP)
	if verbose {
		ps.cmd.Stdout = os.Stdout
		ps.cmd.Stderr = os.Stderr
	}
	err := ps.cmd.Start()
	cmdCopy := ps.cmd
	go func() {
		cmdCopy.Wait()
	}()
	if err != nil {
		panic(err)
	}
	waitForServer(ps.phy)
	return ps.phy
}

func (ps *procServer) kill() {
	if ps.cmd != nil {
		ps.cmd.Process.Signal(os.Kill)
		//log.Println("Killed", ps.dbpath)
		time.Sleep(time.Millisecond * 10)
		if ps.dbpath != "" {
			//os.RemoveAll(ps.dbpath)
		}
	}
}

func (ps *procServer) restart() {
	panic("Not implemented!")
}
func (ps *procServer) disconnect() {
	if ps.cmd != nil {
		ps.cmd.Process.Signal(syscall.SIGSTOP)
		time.Sleep(time.Millisecond * 10)
	}
}
func (ps *procServer) reconnect() {
	if ps.cmd != nil {
		ps.cmd.Process.Signal(syscall.SIGCONT)
		time.Sleep(time.Millisecond * 10)
	}
}

func (ps *procServer) testCapability(c capability) bool {
	return c == capKill || c == capDisconnect
}
