#!/usr/bin/env bash

cd ..
go build
cd testing
go test -c
cp ../treeless vagrant/
cp testing.test vagrant/
cd vagrant/
vagrant destroy -f
vagrant up
vagrant destroy -f
