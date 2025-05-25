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

  --perf)
    if [[ "$2" =~ --clients=([0-9]+) ]]; then
      CLIENTS="${BASH_REMATCH[1]}"
      curl -s "$BASE_URL/perf?key=$CLIENTS"
      echo
    else
      echo "Usage: ./bitcask_client.sh --perf --clients=N"
      exit 1
    fi
    ;;


  *)
    echo "Usage:"
    echo "  ./bitcask_client.sh --put KEY VALUE"
    echo "  ./bitcask_client.sh --get KEY"
    echo "  ./bitcask_client.sh --all"
    ;;
esac
