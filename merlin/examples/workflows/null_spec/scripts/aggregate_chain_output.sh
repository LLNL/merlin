
# $1    read_path


# > TODO: expand for all runs?
r=2
samples=(1 10 100 1000 10000 100000)

read_path="$1/run_${r}"
#NULL_CHAINS=( $(find $read_path -type d -name "null_chain_*" -maxdepth 1) )
#echo ${NULL_CHAINS}
LOGS=( $(find $read_path -type f -name "*.log" -maxdepth 1) )


DATA_DIR=my_data${r}
mkdir ${DATA_DIR}
# for each log file, split it into sections based on sample
i=0
for log in "${LOGS[@]}"
    do
    echo "${log}"
    csplit -f ${DATA_DIR}/split_log_${i}_ -z ${log} /"Step 'verify' in "/ '{*}'
    i=$(( $i+1 ))
    done

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
c=2 #make this dynamic
#for chain in "${NULL_CHAINS[@]}"
#for s in "${samples[@]}"
#    do
    #s=${samples[${i}]}
#    echo "c${c}_s${s} : " >> ${DATA}
    #wf_path="${chain}/"
python3 read_output_chain.py ${read_path}/ ${DATA_DIR}/ $c >> ${DATA}
    #s=$(( $s*10 ))
    #i=$(( $i+1 ))
    #done
#c=$(( $c*2 ))


perl -pi -e 's/ : \n/ : /g' ${DATA}
sort ${DATA} -o ${DATA}
