if [ $# -ne 1 ]; then
	echo "Usage: run_wordcount.sh output-file"
	exit 0
fi

bin/hadoop jar WordCount.jar org.myorg.WordCount /hduser/data_10_to_3/* $1
