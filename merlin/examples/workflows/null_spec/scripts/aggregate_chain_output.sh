
# $1    read_path

samples=(1 10 100 1000 10000 100000)

ALL_DIR=all_data
mkdir ${ALL_DIR}
runs=( $(find $1 -type d -name "run_*" -maxdepth 1) )
for run in "${runs[@]}"
    do
    r=${run: -1}
    read_path="$1/run_${r}"
    LOGS=( $(find $read_path -type f -name "*.log" -maxdepth 1) )
    DATA_DIR=${ALL_DIR}/my_data${r}
    mkdir ${DATA_DIR}
    cat $read_path/*.log > ${DATA_DIR}/extra_lines.log
    grep '^\[' ${DATA_DIR}/extra_lines.log > ${DATA_DIR}/all_nodes.log
    sort ${DATA_DIR}/all_nodes.log -o ${DATA_DIR}/all_nodes.log
    csplit -f ${DATA_DIR}/split_log_ -z ${DATA_DIR}/all_nodes.log /"Step 'verify' in "/ '{*}'

    SPLIT_LOGS=( $(find $DATA_DIR -type f -name "split_log_*_*" -maxdepth 1) )
    #       samples
    # 0     1
    # 1     10
    # 2     100
    # 3     1000
    # 4     10000
    # 5     100000
    # 6     n/a

    DATA=${DATA_DIR}/my_data${r}.yaml
    touch ${DATA}

    s=1
    i=0
    c=$((2**$(($r-1))))
    echo "${c}"
    python3 read_output_chain.py ${read_path}/ ${DATA_DIR}/ $c >> ${DATA}
    perl -pi -e 's/ : \n/ : /g' ${DATA}
    done

cat all_data/my_data*/my_data*.yaml > all_data/all_data.yaml
sort all_data/all_data.yaml -o all_data/all_data.yaml

