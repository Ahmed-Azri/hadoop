#!/bin/sh

if [ $# -ne 2 ]; then
    echo "Usage: compute_mr_result.sh Job_Id LocalSide"
    exit 0
fi

cd /usr/local/hadoop
sh StatMR.sh $1 $2
