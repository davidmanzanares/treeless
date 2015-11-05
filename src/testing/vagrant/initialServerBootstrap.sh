#!/usr/bin/env bash

killall treeless
echo "Initial treeless server..."
/vagrant/treeless -createDB testDB&
sleep 1
