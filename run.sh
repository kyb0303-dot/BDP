#!/bin/bash

echo "----------Start Preprocess----------"
spark-submit src/pipeline/preprocess_subway.py
echo "----------Start Analysis----------"
spark-submit src/analyze/analyze_subway.py
echo "----------Preprocess and Analysis Finish----------"
# automated pipeline script
