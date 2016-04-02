#!/bin/sh
~/Programacion/go/bin/godepgraph -s . | dot -Tsvg -o depgraph.svg
