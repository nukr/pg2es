#!/bin/bash

function main() {
	for i in "$@"; do
		case $i in
		--esurl=*)
			ESURL="${i#*=}"
			shift
			;;
		--pgurl=*)
			PGURL="${i#*=}"
			shift
			;;
		--esindex=*)
			ESINDEX="${i#*=}"
			shift
			;;
		*)
			echo "unknown option"
			;;
		esac
	done

	while read p; do
    arr=($p)
    echo ${arr[1]}
    echo $ESURL
    echo $ESINDEX
    echo $PGURL
  done < table
}

main $@
