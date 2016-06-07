package test

/*
type sshServer struct {
	address string
}

func sshStartCluster(numServers int) []testServer {
	l := make([]testServer, 4)
	l[0] = &sshServer{address: ""}
	l[1] = &sshServer{address: ""}
	l[2] = &sshServer{address: ""}
	l[3] = &sshServer{address: ""}
	return l[:numServers]
}

func (ss *sshServer) addr() string {
	return ss.address + ":9876"
}

func (ss *sshServer) create(numChunks, redundancy int, verbose bool) string {
	ss.kill()
	cmd := exec.Command("ssh", ss.address, "./treeless", "-create", "-port",
		"9876", "-localip", ss.address,
		"-redundancy", fmt.Sprint(redundancy), "-chunks", fmt.Sprint(numChunks))
	err := cmd.Start()
	go func() {
		cmd.Wait()
	}()
	if err != nil {
		panic(err)
	}
	waitForServer(ss.addr())
	return ss.addr()
}

func (ss *sshServer) assoc(addr string, verbose bool) string {
	ss.kill()
	cmd := exec.Command("ssh", ss.address, "./treeless", "-assoc", addr, "-port",
		"9876", "-localip", ss.address)
	err := cmd.Start()
	go func() {
		cmd.Wait()
	}()
	if err != nil {
		panic(err)
	}
	waitForServer(ss.addr())
	return ss.addr()
}

func (ss *sshServer) kill() {
	exec.Command("ssh", ss.address, "killall", "-q", "treeless").Run()
}

func (ss *sshServer) restart() {
	panic("Not implemented!")
}
func (ss *sshServer) disconnect() {
	panic("Not implemented!")
}
func (ss *sshServer) reconnect() {
	panic("Not implemented!")
}

func (ss *sshServer) testCapability(c capability) bool {
	return c == capKill
}
*/
