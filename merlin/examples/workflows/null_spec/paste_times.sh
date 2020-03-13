
concurrencies=(2 4 8 16 32)
samples=(1 10 100 1000)

for c in "${concurrencies[@]}"
    do
    for s in "${samples[@]}"
        do
        echo "c${c}_s${s} = " >> data.py
        python3 task_script.py c_$c/s_$s/*.log >> data.py
        done
    done
