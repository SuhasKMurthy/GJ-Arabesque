#! /usr/bin/env sh

# guarantees that the environment knows Spark
if [ -z "$SPARK_HOME" ]; then
   echo "Please, make sure SPARK_HOME is properly set"
   exit
fi

if [ -z "$ARABESQUE_HOME" ]; then
   echo "Make sure ARABESQUE_HOME is properly set. Don't forget to Run arabesque-env.sh script"
   exit
fi

# TODO : Need a more robust check to declare whether HDFS is up and running as required
if hdfs dfsadmin -report 2>&1 >/dev/null | grep -q 'exception:'; then
   echo "Make sure HDFS is running"
   exit
fi

echo "Spark installation: SPARK_HOME=$SPARK_HOME"

configs="$@"
if [ $1 = "DEBUG" ]; then
   configs="${@:2}"
fi

# configuration files in spark must be read from $SPARK_HOME/io.arabesque.conf
# we aggregate the configs passed by the user in one single temporary file
tempfile=$(mktemp $SPARK_HOME/conf/qfrag-yaml.XXXXXX)
for config_file in $configs; do
   cat $config_file >> $tempfile
done

echo ""
echo "The aggregated config passed by the user:"
echo "========================================="
cat $tempfile
echo ""

# extract the driver memory
driverMemory=4g
maxResultSize=4g

while IFS='' read -r line || [ -n "$line" ]; do
	if echo "$line" | grep 'driver_memory'; then
		driverMemory=${line#*: }
	fi
	if echo "$line" | grep 'max_result_size'; then
		maxResultSize=${line#*: }
	fi
done < "$tempfile"

# qfrag executable
ARABESQUE_JAR=`find $ARABESQUE_HOME -maxdepth 2 -name "arabesque*-jar-with-dependencies.jar" | head -1`

if [ -z "$ARABESQUE_JAR" ] ; then
  echo "No Arabesque jar found. Did you compile it?"
  exit 66
fi

# submit the application to spark cluster. 
# If the DEBUG option is passed then the spark-job will pause, waiting for a remote debugger to attach
if [ $1 = "DEBUG" ] ; then
    $SPARK_HOME/bin/spark-submit --driver-memory $driverMemory --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 spark.driver.maxResultSize=$maxResultSize --verbose --class io.arabesque.QfragRunner $ARABESQUE_JAR -y $(basename $tempfile)
else
    $SPARK_HOME/bin/spark-submit --driver-memory $driverMemory --conf spark.driver.maxResultSize=$maxResultSize --verbose --class  io.arabesque.QfragRunner $ARABESQUE_JAR -y $(basename $tempfile)
fi

# remove the tempfile
rm $tempfile
