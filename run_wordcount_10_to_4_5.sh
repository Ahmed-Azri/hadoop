if [ $# -ne 1 ]; then
	echo "Usage: run_wordcount.sh output-file"
	exit 0
fi

bin/hadoop jar WordCount_10_to_4.jar org.myorg.WordCount /hduser/multi_job/data_10_to_4_1_5/* $1
