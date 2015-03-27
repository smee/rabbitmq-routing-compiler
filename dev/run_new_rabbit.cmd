@echo off
rem example call: run_new_rabbit.cmd foo 5674 15674
set RABBITMQ_NODENAME=%1
set RABBITMQ_NODE_PORT=%2
set RABBITMQ_BASE=rabbits
set RABBITMQ_CONFIG_FILE=%RABBITMQ_base%/%1
rem set RABBITMQ_SERVER_START_ARGS="-setcookie dummycookie"
"c:\Program Files (x86)\RabbitMQ Server\rabbitmq_server-3.5.0\sbin\rabbitmq-server" -detached -rabbitmq_management listener "[{port,%3}]"  