#!/bin/bash

# Compile the project
mvn clean compile

# Run the CLI tool
if [[ $1 == "--view-all" ]]; then
    mvn exec:java -Dexec.mainClass="com.centralstation.BitcaskClient.Client" -Dexec.args="--view-all" -e

elif [[ $1 == --view* ]]; then
    key_arg=$1
    mvn exec:java -Dexec.mainClass="com.centralstation.BitcaskClient.Client" -Dexec.args="$key_arg"

elif [[ $1 == --perf* ]]; then
    perf_arg=$1
    mvn exec:java -Dexec.mainClass="com.centralstation.BitcaskClient.Client" -Dexec.args="$perf_arg"

else
    echo "Usage:"
    echo "  ./bitcask_client.sh --view-all"
    echo "  ./bitcask_client.sh --view=KEY"
    echo "  ./bitcask_client.sh --perf=100"
fi
