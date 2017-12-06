#!/usr/bin/env bash
nservers=$1
root=$(dirname $0)

dcl_config=$root/vicci_dcl_config/${nservers}_in_vicci
client_config=$root/vicci_dcl_config/${nservers}_clients_in_vicci


$root/vicci_stress_killer.bash $client_config
$root/vicci_cassandra_killer.bash $dcl_config
