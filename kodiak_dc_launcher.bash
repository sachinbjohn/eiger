#!/bin/bash
#
# Launches multiple cassandra instances on KODIAK
#

set -u

if [ $# -ne 1 ]; then
    echo "Usage: "$0" [vicci_dcl_config_file]"
    exit
fi

dcl_config=$1
wait=wait
cops_dir=$HOME/eiger
var_dir=$cops_dir/cassandra_var


num_dcs=$(grep num_dcs $dcl_config | awk -F "=" '{ print $2 }')
ips=($(grep cassandra_ips $dcl_config | awk -F "=" '{ print $2 }'))
ips=($(echo "echo ${ips[@]}" | bash))
#Seed from all the nodes
seeds=$(echo ${ips[@]} | sed 's/ /, /g')
echo "seeds=$seeds"

nodes_per_dc=$((${#ips[@]} / num_dcs))
total_nodes=$((num_dcs * nodes_per_dc))

#sanity check
if [ $(($nodes_per_dc * $num_dcs)) -ne ${#ips[@]} ]; then 
    echo ${nodes_per_dc}" * "${num_dcs}" != "${#ips[@]};
    exit
fi

#clean up all nodes in parallel
for ip in ${ips[@]}; do
    (ssh -t -t -o StrictHostKeyChecking=no $ip "\
${cops_dir}/kill_all_cassandra.bash;\
rm ${cops_dir}/*hprof 2> /dev/null;\
rm ${var_dir}/cassandra*log 2> /dev/null;\
rm ${var_dir}/cassandra*log* 2> /dev/null;\
rm -rf ${var_dir}/data/* 2> /dev/null;\
rm -rf ${var_dir}/commitlog/* 2> /dev/null;\
rm -rf ${var_dir}/saved_caches/* 2> /dev/null;\
rm -rf ${var_dir}/stdout/* 2> /dev/null;\
mkdir ${var_dir} 2> /dev/null;\
mkdir ${var_dir}/data 2> /dev/null;\
mkdir ${var_dir}/commitlog 2> /dev/null;\
mkdir ${var_dir}/saved_caches 2> /dev/null;\
mkdir ${var_dir}/stdout 2> /dev/null;\
" 2>&1 | awk '{ print "'$ip': "$0 }' ) &
done

wait
unset ip

set +m


#create the topo file, we must eventually write this to conf/cassandra-topology.properties
#because that location is hardcoded into cassandra
topo_file=${cops_dir}/conf/vicci/cassandra-topology.properties
echo -n "" > $topo_file
for dc in $(seq 0 $((num_dcs - 1))); do
    for n in $(seq 0 $((nodes_per_dc - 1))); do
        global_node_num=$((dc * nodes_per_dc + n))
	local_ip=$(echo ${ips[global_node_num]})
        #update the topology describing file
        #we only care about splitting things up by datacenter for now
        echo $local_ip=DC$dc:RAC1 >> $topo_file
    done
done
#echo "default=DC0:RAC1" >> $topo_file
unset dc
unset n
unset global_node_num
unset local_ip

for dc in $(seq 0 $((num_dcs - 1))); do
    for n in $(seq 0 $((nodes_per_dc - 1))); do
        global_node_num=$((dc * nodes_per_dc + n))
	local_ip=$(echo ${ips[global_node_num]})
        # tokens can't be identical even though we want them to be ... so for now let's get them as close as possible
        token=$(echo "${n}*(2^127)/${nodes_per_dc} + $dc" | bc)
        # Using tokens for evenly splitting type 4 uuids now
        #token=$(./uuid_token.py ${dc} ${n} ${nodes_per_dc})
        echo $token" @ "$local_ip

        conf_file=${num_dcs}x${nodes_per_dc}_${dc}_${n}.yaml
        log4j_file=log4j-server_${global_node_num}.properties

        #create the custom config file for this node
        sed 's/INITIAL_TOKEN/'$token'/g' conf/cassandra_KODIAK_BASE.yaml \
	    | sed 's/LISTEN_ADDRESS/'$local_ip'/g' \
            | sed 's/RPC_ADDRESS/'$local_ip'/g' \
            | sed 's/SEEDS/'"$seeds"'/g' \
            | sed 's,VAR_DIR,'$var_dir',g' \
            > ${cops_dir}/conf/vicci/$conf_file
        
	#| sed 's/NODE_NUM/'$global_node_num'/g' \

        sed 's+LOG_FILE+'$var_dir'/cassandra_system.'$global_node_num'.log+g' ${cops_dir}/conf/log4j-server_BASE.properties > ${cops_dir}/conf/vicci/$log4j_file

        #set -x
	#copy over conf files
	(
	scp -o StrictHostKeyChecking=no ${topo_file} ${local_ip}:${cops_dir}/conf/ 2>&1 | awk '{ print "'$local_ip': "$0 }'
	scp -o StrictHostKeyChecking=no ${cops_dir}/conf/vicci/${conf_file} ${local_ip}:${cops_dir}/conf/ 2>&1 | awk '{ print "'$local_ip': "$0 }'
	scp -o StrictHostKeyChecking=no ${cops_dir}/conf/vicci/${log4j_file} ${local_ip}:${cops_dir}/conf/ 2>&1 | awk '{ print "'$local_ip': "$0 }'

        #put this in ssh commands to modify JVM options
        #export JVM_OPTS="-Xms32M -Xmn64M"

	while [ 1 ]; do
            ssh_output=$(ssh -o StrictHostKeyChecking=no $local_ip "\
cd ${cops_dir};\
 CASSANDRA_INCLUDE=bin/cassandra.in.sh bin/cassandra -Dcassandra.config=file:///${cops_dir}/conf/${conf_file} -Dlog4j.configuration=${log4j_file} > ${var_dir}/stdout/${dc}_${n}.out;\
" 2>&1)
	    failure=$(echo $ssh_output | grep "error while loading shared libraries")
	    if [ "$failure" == "" ]; then
		break
	    fi
	done
	) &


        #set +x
    done
done


timeout=60
if [ "$wait" != "" ]; then
    #wait until all nodes have joined the ring
    normal_nodes=0
    echo "Nodes up and normal: "
    wait_time=0
    while [ "${normal_nodes}" -ne "${total_nodes}" ]; do
        sleep 5
        normal_nodes=$(ssh -o StrictHostKeyChecking=no ${ips[0]} \
	    'CASSANDRA_INCLUDE=${cops_dir}/bin/cassandra.in.sh ${cops_dir}/bin/nodetool -h localhost ring 2>&1 | grep "Normal" | wc -l')
        echo "  "$normal_nodes
	wait_time=$((wait_time+5))
	if [[ $wait_time -ge 60 ]]; then
	    echo "timeout waiting for nodes to come up ... you'll need to try again"
	    exit 1
	fi
    done
    sleep 5
fi