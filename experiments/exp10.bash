#!/usr/bin/env bash


#Using VICCI confs
set -u
set -x

if [ $# -ne 1 ]; then
    echo "$0: [# servers]"
    exit
fi



nservers=$1
dcl_config=${nservers}_in_vicci
client_config=${nservers}_clients_in_vicci

cops_dir="$HOME/eiger"
exp_dir="${cops_dir}/experiments"
stress_dir="${cops_dir}/tools/stress"

output_dir_base="${exp_dir}/exp10"
exp_uid=$(date +%s)
output_dir="${output_dir_base}/${exp_uid}"
mkdir -p ${output_dir}
rm $output_dir_base/latest
ln -s $output_dir $output_dir_base/latest


dcl_config_full="${cops_dir}/vicci_dcl_config/${dcl_config}"

all_servers=($(cat $dcl_config_full | grep cassandra_ips | awk -F"=" '{ print $2 }' | xargs))
all_servers=$(echo "echo ${all_servers[@]}" | bash)
num_dcs=$(cat $dcl_config_full | grep num_dcs | awk -F"=" '{ print $2 }')

strategy_properties="DC0:1"
for i in $(seq 1 $((num_dcs-1))); do
    strategy_properties=$(echo ${strategy_properties}",DC${i}:1")
done

num_servers=$(echo $all_servers | wc -w)
num_servers_per_dc=$((num_servers / num_dcs))

for dc in $(seq 0 $((num_dcs-1))); do
    this_dc_servers=$(echo $all_servers | sed 's/ /\n/g' | head -n $((num_servers_per_dc * (dc+1))) | tail -n $num_servers_per_dc | xargs)
    servers_by_dc[$dc]=${this_dc_servers}
done
echo ${servers_by_dc[@]}



client_config_full="${cops_dir}/vicci_dcl_config/${client_config}"

all_clients=$(cat $client_config_full | grep cassandra_ips | awk -F"=" '{ print $2 }' | xargs)
all_clients=$(echo "echo ${all_clients[@]}" | bash)

num_clients=$(echo $all_clients | wc -w)
num_clients_per_dc=$((num_clients / num_dcs))

for dc in $(seq 0 $((num_dcs-1))); do
    this_dc_clients=$(echo $all_clients | sed 's/ /\n/g' | head -n $((num_clients_per_dc * (dc+1))) | tail -n $num_clients_per_dc | xargs)
    clients_by_dc[$dc]=${this_dc_clients}
done
echo ${clients_by_dc[@]}

kill_all_cmd="${cops_dir}/vicci_cassandra_killer.bash ${cops_dir}/vicci_dcl_config/${dcl_config}"
stress_killer="${cops_dir}/kill_stress_vicci.bash"


#get cluster up an running
internal_cluster_start_cmd() {
    cur_dir=$PWD
    src_dir=$1
    cd ${src_dir};
    $kill_all_cmd;
    sleep 1;
    while [ 1 ]; do
        ./vicci_dc_launcher.bash ${dcl_config_full}
        return_value=$?
        if [ $return_value -eq 0 ]; then
            break
        fi
    done
    cd $cur_dir;
}


internal_populate_cluster() {
    src_dir=$1
    insert_cmd=$2
    total_keys=$3
    max_columns=$4
    column_size=$5
    column_per_key_write=$6

    #set the keyspace
    for i in $(seq 3); do
        first_dc_servers_csv=$(echo ${servers_by_dc[0]} | sed 's/ /,/g')

        # set up a killall for stress in case it hangs
        (sleep 60; killall stress) &
        killall_jck_pid=$!
        ${src_dir}/tools/stress/bin/stress --nodes=$first_dc_servers_csv --just-create-keyspace --replication-strategy=NetworkTopologyStrategy --strategy-properties=$strategy_properties
        kill $killall_jck_pid
        sleep 5
    done
    sleep 10

    populate_attempts=0
    while [ 1 ]; do

        KILLALL_SSH_TIME=300
        MAX_ATTEMPTS=10
        (sleep $KILLALL_SSH_TIME; killall ssh) &
        killall_ssh_pid=$!

        #divide keys across first cluster clients only
        keys_per_client=$((total_keys / num_clients_per_dc))

        pop_pids=""
        for dc in 0; do
            local_servers_csv=$(echo ${servers_by_dc[$dc]} | sed 's/ /,/g')

            for cli_index in $(seq 0 $((num_clients_per_dc - 1))); do
                client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index+1)) | tail -n 1)

                #all_servers_csv=$(echo $all_servers | sed 's/ /,/g')
                first_dc_servers_csv=$(echo ${servers_by_dc[0]} | sed 's/ /,/g')

                #write to ALL so the cluster is populated everywhere
                ssh $client -o StrictHostKeyChecking=no "\
                    mkdir -p ${output_dir}; \
                    $stress_killer; sleep 10; \
                    cd ${src_dir}/tools/stress; \
                    bin/stress \
                    --nodes=$first_dc_servers_csv \
                    --columns=$max_columns \
                    --column-size=$column_size \
                    --operation=${insert_cmd} \
                    --consistency-level=LOCAL_QUORUM \
                    --replication-strategy=NetworkTopologyStrategy \
                    --strategy-properties=$strategy_properties \
                    --num-different-keys=$keys_per_client \
                    --num-keys=$keys_per_client \
                    --stress-index=$cli_index \
                    --stress-count=$num_clients_per_dc \
                     > >(tee ${output_dir}/populate.out) \
                    2> >(tee ${output_dir}/populate.err) \
                    " 2>&1 | awk '{ print "'$client': "$0 }' &
                                    pop_pid=$!
                                    pop_pids="$pop_pids $pop_pid"
            done
        done

        #wait for clients to finish
        for pop_pid in $pop_pids; do
            echo "Waiting on $pop_pid"
            wait $pop_pid
        done

        sleep 1

        kill $killall_ssh_pid
        # if we kill killall successfully, it will return 0 and that means we populated the cluster and can continue
        #  otherwise we try again
        killed_killall=$?

        if [ $killed_killall == "0" ]; then
            break;
        fi
        ((populate_attempts++))
        if [[ $populate_attempts -ge $MAX_ATTEMPTS ]]; then
            echo -e "\n\n \e[01;31m Could not populate the cluster after $MAX_ATTEMPTS attempts \e[0m \n\n"
            exit;
        fi
        echo -e "\e[01;31m Failed populating $populate_attempts times, trying again (out of $MAX_ATTEMPTS) \e[0m"
    done
}

run_exp10() {
    keys_per_serv=$1
    num_serv=$2
    column_size=$3
    keys_per_read=$4
    write_frac=$5
    zipf_const=$6
    num_threads=$7
    exp_time=$8
    trial=$9

    cli_output_dir="$output_dir/trial${trial}"
    data_file_name=$1_$2_$3_$4_$5_$6_$7_$8+$9+data
    for dc in 0; do
        local_servers_csv=$(echo ${servers_by_dc[$dc]} | sed 's/ /,/g')
        for cli_index in $(seq 0 $((num_clients_per_dc - 1))); do
            client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index + 1)) | tail -n 1)
            ssh $client -o StrictHostKeyChecking=no "\
            mkdir -p $cli_output_dir; \
            cd ${src_dir}/tools/stress; \
            ((bin/stress \
            --progress-interval=1 \
            --nodes=$local_servers_csv \
            --operation=EXP10 \
            --consistency-level=LOCAL_QUORUM \
            --replication-strategy=NetworkTopologyStrategy \
            --strategy-properties=$strategy_properties \
            --keys-per-server=$keys_per_serv \
            --num-servers=$num_servers \
            --stress-index=$cli_index \
            --stress-count=$num_clients_per_dc \
            --num-keys=20000000 \
            --column-size=$column_size \
            --keys-per-read=$keys_per_read \
            --write-fraction=$write_frac \
            --threads=$num_threads \
            --zipfian-constant=$zipf_const \
             > >(tee ${cli_output_dir}/${data_file_name}) \
            2> ${cli_output_dir}/${data_file_name}.stderr \
            ) &); \
            sleep $((exp_time + 10)); ${src_dir}/kill_stress_vicci.bash" \
            2>&1 | awk '{ print "'$client': "$0 }' &
        done
    done
    #wait for clients to finish
    wait
}

gather_results() {
    for dc in 0; do
        for cli_index in $(seq 0 $((num_clients_per_dc - 1))); do
            client_dir=${output_dir}/client${cli_index}
            client=$(echo ${clients_by_dc[$dc]} | sed 's/ /\n/g' | head -n $((cli_index+1)) | tail -n 1)
            rsync -az $client:$output_dir/* ${client_dir}
        done
    done
}



keys_per_server=1000000
total_keys=$((keys_per_server*num_servers))
run_time=60
for trial in 1 #2 3 4 5
do
    for value_size in 8 #128 512
    do

        for keys_per_read in 2 #4 16
        do
            for write_frac in 0.01 #0.05 0.1
            do
                for zipf_c in 0.99 #0.0 0.8 0.99
                do
                    for numT in 32 24 16 12 8 4 1 #4 8 12 16 24 32
                    do
                        internal_cluster_start_cmd $cops_dir
                        internal_populate_cluster $cops_dir INSERTCL $total_keys 1 $value_size 1
                        run_exp10 $keys_per_server $num_servers $value_size $keys_per_read $write_frac $zipf_c $numT $run_time $trial
                        $kill_all_cmd
                        gather_results
                    done
                done
            done
        done
    done
done
