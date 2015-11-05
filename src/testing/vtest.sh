#!/usr/bin/env bash

cd ..
go build
cd testing
go test -c
cp ../treeless vagrant/
cp testing.test vagrant/
cd vagrant/
echo "Program built"
vagrant up --parallel s0 s1
vagrant provision s0 s1
vagrant up c0
vagrant provision c0
vagrant suspend
