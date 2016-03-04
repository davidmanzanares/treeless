#!/usr/bin/env bash

echo Starting up master...
cd /mnt
whoami
pwd
start-stop-daemon -b -S --exec /vagrant/treeless -- -create -localip 192.168.2.100 -port 9876 -dbpath /home/vagrant
sleep 1
echo Master is online
