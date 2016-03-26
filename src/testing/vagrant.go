package tltest

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
)

type vagrantServer struct {
	id int
}

func vagrantStartCluster(numServers int) []testServer {
	l := make([]testServer, numServers)
	v := new(vagrantServer)
	v.id = 0
	l[0] = v
	ids := 1
	for i := 1; i < numServers; i++ {
		v := new(vagrantServer)
		v.id = ids
		ids++
		l[i] = v
	}

	//Vagrant up VMs
	exec.Command("cp", "../treeless", "./").Run()
	runtime.GOMAXPROCS(4)

	//Initialize Vagrant auxiliary files

	//Vagrantfile
	middle := ""
	for i := 1; i < numServers; i++ {
		middle = middle + `config.vm.define "vm` + fmt.Sprint(i) + `" do |vmN|
			vmN.vm.provision :shell, path: "vm` + fmt.Sprint(i) + `.sh"
			vmN.vm.network "private_network", ip: "192.168.2.` + fmt.Sprint(100+i) + `"
		end
		`
	}
	vagrantFile := `Vagrant.configure(2) do |config|
	  config.vm.box = "ubuntu/trusty64"
	  config.vm.provider "virtualbox" do |v|
		v.cpus = 1
	  end
	  config.vm.define "vm0" do |vmN|
		  vmN.vm.provision :shell, path: "vm0.sh"
		  vmN.vm.network "private_network", ip: "192.168.2.100"
	  end
	  ` + middle + `
	end`
	err := ioutil.WriteFile("Vagrantfile", []byte(vagrantFile), 0777)
	if err != nil {
		panic(err)
	}

	//Provision bash scripts
	//First VM provision script
	//http://stackoverflow.com/questions/8251933/how-can-i-log-the-stdout-of-a-process-started-by-start-stop-daemon
	sh0 := `echo Starting up VM0...
	sysctl -w fs.file-max=100000
	echo *    soft    nofile  8192 >> /etc/security/limits.conf
	echo *    hard    nofile  8192 >> /etc/security/limits.conf
	mkdir /home/vagrant/db
	chmod -R 0777 /home/vagrant/db
	echo VM0 is online`
	err = ioutil.WriteFile("vm0.sh", []byte(sh0), 0777)
	//defer os.Remove("vm0.sh")
	if err != nil {
		panic(err)
	}
	//Rest of VM provision script
	for i := 1; i < numServers; i++ {
		sh := `echo Starting up VM` + fmt.Sprint(i) + `...
		sysctl -w fs.file-max=100000
		echo *    soft    nofile  8192 >> /etc/security/limits.conf
		echo *    hard    nofile  8192 >> /etc/security/limits.conf 
		mkdir /home/vagrant/db
		chmod -R 0777 /home/vagrant/db
		echo VM` + fmt.Sprint(i) + ` is online`
		err = ioutil.WriteFile("vm"+fmt.Sprint(i)+".sh", []byte(sh), 0777)
		//defer os.Remove("vm" + fmt.Sprint(i) + ".sh")
		if err != nil {
			panic(err)
		}
	}

	cmd := exec.Command("vagrant", "up", "--parallel")
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Run()
	for _, s := range l {
		s.kill()
	}
	return l
}

func (vs *vagrantServer) addr() string {
	return "192.168.2." + fmt.Sprint(100+vs.id) + ":9876"
}

func (vs *vagrantServer) vagrantSSH(cmd string) {
	c := exec.Command("vagrant", "ssh", "vm"+fmt.Sprint(vs.id), "-c", cmd)
	c.Stderr = os.Stderr
	c.Stdout = os.Stdout
	c.Run()
}

func (vs *vagrantServer) create(numChunks, redundancy int) string {
	//Stop
	vs.vagrantSSH("killall -q treeless; rm -f /home/vagrant/treeless.*")
	//Start and create
	vs.vagrantSSH(`start-stop-daemon -S -b --make-pidfile --pidfile /home/vagrant/treeless.pid --startas /bin/bash -- -c "exec /vagrant/treeless -create -localip 192.168.2.100 -port 9876 -dbpath /home/vagrant/db -redundancy ` + fmt.Sprint(redundancy) + ` -chunks ` + fmt.Sprint(numChunks) + ` > /home/vagrant/treeless.log 2>&1"`)
	waitForServer(vs.addr())
	return vs.addr()
}

func (vs *vagrantServer) assoc(addr string) string {
	//Stop
	vs.vagrantSSH("killall -q treeless; rm -f /home/vagrant/treeless.*")
	//Start and assoc
	vs.vagrantSSH(`start-stop-daemon -S -b --make-pidfile --pidfile /home/vagrant/treeless.pid --startas /bin/bash -- -c "exec /vagrant/treeless -assoc ` + addr + " -localip 192.168.2." + fmt.Sprint(100+vs.id) + ` -port 9876 -dbpath /home/vagrant/db -cpuprofile /home/vagrant/cpu > /home/vagrant/treeless.log 2>&1"`)
	waitForServer(vs.addr())
	return vs.addr()
}

func (vs *vagrantServer) kill() {
	vs.vagrantSSH("killall -q treeless; rm -f /home/vagrant/treeless.*")
}

func (vs *vagrantServer) restart() {
	panic("Not implemented!")
}
func (vs *vagrantServer) disconnect() {
	panic("Not implemented!")
}
func (vs *vagrantServer) reconnect() {
	panic("Not implemented!")
}

func (vs *vagrantServer) testCapability(c capability) bool {
	return c == capKill
}
