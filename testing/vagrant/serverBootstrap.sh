#!/usr/bin/env bash

killall treeless
echo "Treeless server..."
/vagrant/treeless -asocDB testDB&
sleep 1
