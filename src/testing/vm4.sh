echo Starting up VM4...
		sysctl -w fs.file-max=100000
		echo *    soft    nofile  8192 >> /etc/security/limits.conf
		echo *    hard    nofile  8192 >> /etc/security/limits.conf 
		mkdir /home/vagrant/db
		chmod -R 0777 /home/vagrant/db
		echo VM4 is online