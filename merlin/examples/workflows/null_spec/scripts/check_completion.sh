concurrencies=(1 2 4 8 16 32)
samples=(1 10 100 1000 10000)

for c in "${concurrencies[@]}"
    do
    for s in "${samples[@]}"
        do
            if [[ ! -f "c_${c}/s_${s}" ]]; then
                echo "c_${c}/s_${s} does NOT exist"
            fi
        done
    done
