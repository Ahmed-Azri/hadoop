[ "$#" -lt 2 ] && echo "Usage: generate_MRJob <Job Name> <Source>" && exit 0

mkdir MR_Job/bin/$1
javac -classpath hadoop-core-1.1.2.jar -d MR_Job/bin/$1 $2
jar -cvf $1.jar -C MR_Job/bin/$1 .
