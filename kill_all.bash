nservers=$1
dcl_config=${nservers}_in_vicci
client_config=${nservers}_clients_in_vicci

./vicci_stress_killer $client_config
./vicci_cassandra_killer $dcl_config