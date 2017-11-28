#!/usr/bin/env bash
nservers=$1
dcl_config=${nservers}_in_vicci
client_config=${nservers}_clients_in_vicci

./vicci_stress_killer.bash $client_config
./vicci_cassandra_killer.bash $dcl_config