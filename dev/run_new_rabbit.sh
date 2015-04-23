#!/bin/bash
# example call: run_new_rabbit.cmd foo 5674 15674
export RABBITMQ_NODENAME=$1
export RABBITMQ_NODE_PORT=$2
export RABBITMQ_BASE=`readlink -m rabbits`
export RABBITMQ_CONFIG_FILE=$RABBITMQ_BASE/$1
export RABBITMQ_MNESIA_BASE=$RABBITMQ_BASE/mnesia
export RABBITMQ_LOG_BASE=$RABBITMQ_BASE/logs
#RABBITMQ_SERVER_START_ARGS="-setcookie dummycookie"
export RABBITMQ_SERVER_START_ARGS="-detached -rabbitmq_management listener [{port,$3}] "
rabbitmq-server 
