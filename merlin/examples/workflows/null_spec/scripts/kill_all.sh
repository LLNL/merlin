# 1     run id
# 2     concurrency

scancel -u ${USER}
merlin stop-workers
merlin purge -f ../null_chain.yaml --vars QUEUE=queue_c${2}_r${1}_chain
