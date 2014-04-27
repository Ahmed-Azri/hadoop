if [ $# -ne 1 ]; then
	echo "Usage: run_wordcount_all_to_all.sh output-file"
	exit 0
fi

bin/hadoop jar WordCountAllToAll.jar org.myorg.WordCount /hduser/data_16_to_16/* $1
