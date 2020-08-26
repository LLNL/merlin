
# $1    run_id
# $2    read_path

samples=(1 10 100 1000 10000 100000)

read_path="$2/run_${1}"
NULL_CHAINS=( $(find $read_path -type d -name "null_chain_*" -maxdepth 1) )

echo ${NULL_CHAINS}

DATA=my_data${1}.yaml
touch ${DATA}

c=1
for chain in "${NULL_CHAINS[@]}"
    do
    s=1
    for s in $samples
        do
        echo "c${c}_s${s} : " >> ${DATA}
        wf_path="${chain}/"
        echo ${wf_path}
        python3 read_output.py ${wf_path}/ $c $s >> ${DATA}
        s=$(( $s*10 ))
        done
    c=$(( $c*2 ))
    done
perl -pi -e 's/ : \n/ : /g' ${DATA}
