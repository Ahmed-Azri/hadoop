if [ $# -ne 1 ]; then
	echo "Usage: run_wordcount_all_to_all.sh output-file"
	exit 0
fi

bin/hadoop jar WordCount_16_to_12.jar org.myorg.WordCount /hduser/data_16_to_12/* $1
