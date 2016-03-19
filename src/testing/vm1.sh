echo Starting up VM1...
		sysctl -w fs.file-max=100000
		mkdir /home/vagrant/db
		chmod -R 0777 /home/vagrant/db
		echo VM1 is online