if [ $# -ne 1 ]; then
	echo "Usage: run_wordcount_all_to_all.sh output-file"
	exit 0
fi

bin/hadoop jar WordCount_16_to_4.jar org.myorg.WordCount /hduser/shuffle_pattern/data_16_to_4/* $1
