#!/bin/bash

set -e

cd "${0%/*}"

input_uri=$1
output_uri=$2
jar_path="../target/scala-2.11/ShopKickInterview-assembly-20170621.0.jar"

spark-submit \
  --master local[4] \
  --conf spark.executor.memory=2g \
  $jar_path --inputUri $input_uri --outputUri $output_uri
  
