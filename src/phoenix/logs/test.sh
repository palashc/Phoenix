init-config -nf 1 -ns $1 -nm $2 -slots $3

init-executor &
init-monitor &
init-scheduler &
echo "going to sleep... zzzz"
sleep 5

prototype
pkill init-executor
pkill init-monitor
pkill init-scheduler
