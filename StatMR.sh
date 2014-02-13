#!/bin/sh

if [ $# -ne 2 ]; then
    echo "Usage: StatMR Job_Id LocalSide"
    exit 0
fi

contains() {
	string="$1"
	substring="$2"
	if test "${string#*$substring}" != "$string"
	then
		return 0    # $substring is in $string
	else
		return 1    # $substring is not in $string
	fi
}

list=""
for file in ./logs/userlogs/job_$1/*
do
	contains "$file" "job-acls.xml" && continue
	contains "$file" "_m_" && continue
	contains "$file" "cleanup" && continue
	filename="$file/syslog"
	list="$list $filename"
done

java com.zack.stat.MRStat $2 $2_output $list
