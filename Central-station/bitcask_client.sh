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
