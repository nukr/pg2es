#!/usr/bin/env fish

function main
  set -l esurl
  set -l pgdsl
  set -l esindex
  getopts $argv | while read -l key value
    switch $key
      case esurl
        set esurl $value
      case pgdsn
        set pgdsn $value
      case esindex
        set esindex $value
    end
  end

  for x in (cat table)
    set -l aa (string split " " $x)
    go run main.go \
    --pgtable $aa[1] \
    --estable $aa[2] \
    --esurl $esurl  \
    --pgdsn $pgdsn \
    --esindex $esindex \
    --workernum 1 \
    --jobsize 200
  end
end

main $argv
