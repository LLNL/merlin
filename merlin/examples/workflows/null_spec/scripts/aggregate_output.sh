
runs=( 31 )
concurrencies=(1 2 4 8 16 32 64)
#samples=(100000)
samples=(1 10 100 1000 10000)

for run in "${runs[@]}"
    do
    read_path="/p/lustre1/bay1/null_results/run_${run}"
    DATA=my_data${run}.yaml
    touch ${DATA}

    for c in "${concurrencies[@]}"
        do
        for s in "${samples[@]}"
            do
            echo "c${c}_s${s} : " >> ${DATA}
            wf_path="${read_path}/c_$c/s_$s"

            echo "${wf_path}/*.log"
            echo "${wf_path}/*.err"
            echo "$c"
            echo "$s"
    
            python3 read_output.py --logfile ${wf_path}/*.log --errfile ${wf_path}/*.err --c $c --s $s >> ${DATA}
            done
        done
    perl -pi -e 's/ : \n/ : /g' ${DATA}
    done

