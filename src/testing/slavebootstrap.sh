#!/usr/bin/env bash

echo Starting up slave...
cd /mnt
whoami
pwd
start-stop-daemon -b -S --exec /vagrant/treeless -- -assoc 192.168.2.100:9876 -localip 192.168.2.101 -port 9876 -dbpath /home/vagrant
sleep 1
echo Slave is online
