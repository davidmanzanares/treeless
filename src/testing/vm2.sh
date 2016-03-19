echo Starting up VM2...
		sysctl -w fs.file-max=100000
		mkdir /home/vagrant/db
		chmod -R 0777 /home/vagrant/db
		echo VM2 is online