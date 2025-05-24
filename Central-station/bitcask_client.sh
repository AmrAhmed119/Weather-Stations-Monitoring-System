# #!/bin/bash

# # Compile the project
# mvn clean compile

# # Run the CLI tool
# if [[ $1 == "--view-all" ]]; then
#     mvn exec:java -Dexec.mainClass="com.centralstation.BitcaskClient.Client" -Dexec.args="--view-all" -e

# elif [[ $1 == --view* ]]; then
#     key_arg=$1
#     mvn exec:java -Dexec.mainClass="com.centralstation.BitcaskClient.Client" -Dexec.args="$key_arg"

# elif [[ $1 == --perf* ]]; then
#     perf_arg=$1
#     mvn exec:java -Dexec.mainClass="com.centralstation.BitcaskClient.Client" -Dexec.args="$perf_arg"

# else
#     echo "Usage:"
#     echo "  ./bitcask_client.sh --view-all"
#     echo "  ./bitcask_client.sh --view=KEY"
#     echo "  ./bitcask_client.sh --perf=100"
# fi


#!/bin/bash

BASE_URL="http://localhost:8080"

case "$1" in
  --get)
    if [[ -z "$2" ]]; then
      echo "Usage: ./bitcask_client.sh --get KEY"
      exit 1
    fi
    curl -s "$BASE_URL/get?key=$2"
    echo
    ;;

  --view-all)
    curl -s "$BASE_URL/all"
    echo
    ;;

  *)
    echo "Usage:"
    echo "  ./bitcask_client.sh --put KEY VALUE"
    echo "  ./bitcask_client.sh --get KEY"
    echo "  ./bitcask_client.sh --all"
    ;;
esac
