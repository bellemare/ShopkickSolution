##How to build:
-  git clone the project
-  cd to project root
-  execute "sbt assembly". This should build the fat jar with the dependencies.

##How to run:
-  ./bin/run_local.sh <inputURI> <outputURI>
Note: The application will run in local mode, unless the spark-submit parameters are modified to specify a cluster.
ex: ./bin/run_local.sh /Users/adambellemare/Personal/ShopkickSolution/src/test/resources/multiJsonRowSample.json /tmp/myLocalOutputDirectory  

