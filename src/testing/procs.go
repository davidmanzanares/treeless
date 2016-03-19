package tltest

import (
	"fmt"
	"os"
	"os/exec"
	"time"
)

func procStartCluster(numServers int) []testServer {
	l := make([]testServer, numServers)
	l[0] = new(procServer)
	for i := 1; i < numServers; i++ {
		l[i] = new(procServer)
	}
	return l
}

var procID = 0

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

type procServer struct {
	phy    string
	dbpath string
	cmd    *exec.Cmd
}

func (ps *procServer) addr() string {
	return ps.phy
}

func (ps *procServer) create(numChunks, redundancy int) string {
	exec.Command("killall", "-q", "treeless").Run()
	dbTestFolder := ""
	if exists("/mnt/dbs/") {
		dbTestFolder = "/mnt/dbs/"
	}
	ps.dbpath = dbTestFolder + "testDB" + fmt.Sprint(procID)
	ps.dbpath = "testDB" + fmt.Sprint(procID)
	ps.cmd = exec.Command("./treeless", "-create", "-port",
		fmt.Sprint(10000+procID), "-dbpath", ps.dbpath, "-localip", "127.0.0.1",
		"-redundancy", fmt.Sprint(redundancy), "-chunks", fmt.Sprint(numChunks))
	if true {
		ps.cmd.Stdout = os.Stdout
		ps.cmd.Stderr = os.Stderr
	}
	procID++
	err := ps.cmd.Start()
	cmdCopy := ps.cmd
	go func() {
		cmdCopy.Wait()
	}()
	if err != nil {
		panic(err)
	}
	ps.phy = string("127.0.0.1" + ":" + fmt.Sprint(10000+procID-1))
	waitForServer(ps.phy)
	return ps.phy
}

func (ps *procServer) assoc(addr string) string {
	dbTestFolder := ""
	if exists("/mnt/dbs/") {
		dbTestFolder = "/mnt/dbs/"
	}
	ps.dbpath = dbTestFolder + "testDB" + fmt.Sprint(procID)
	ps.dbpath = "testDB" + fmt.Sprint(procID)
	ps.cmd = exec.Command("./treeless", "-assoc", addr, "-port",
		fmt.Sprint(10000+procID), "-dbpath", ps.dbpath, "-localip", "127.0.0.1") //, "-cpuprofile"
	if true {
		ps.cmd.Stdout = os.Stdout
		ps.cmd.Stderr = os.Stderr
	}
	procID++
	err := ps.cmd.Start()
	cmdCopy := ps.cmd
	go func() {
		cmdCopy.Wait()
	}()
	if err != nil {
		panic(err)
	}
	ps.phy = string("127.0.0.1" + ":" + fmt.Sprint(10000+procID-1))
	waitForServer(ps.phy)
	return ps.phy
}

func (ps *procServer) kill() {
	ps.cmd.Process.Signal(os.Interrupt)
	time.Sleep(time.Millisecond * 10)
	os.RemoveAll(ps.dbpath)
}

func (ps *procServer) restart() {
	panic("Not implemented!")
}
func (ps *procServer) disconnect() {
	panic("Not implemented!")
}
func (ps *procServer) reconnect() {
	panic("Not implemented!")
}

func (ps *procServer) testCapability(c capability) bool {
	return c == capKill
}
