#! /usr/bin/env sh

configs="$@"

# configuration files in spark must be read from $SPARK_HOME/io.arabesque.conf
# we aggregate the configs passed by the user in one single temporary file
tempfile=$(mktemp /usr/lib/spark/conf/qfrag-yaml.XXXXXX)
for config_file in $configs; do
   cat $config_file >> $tempfile
done

echo ""
echo "The aggregated config passed by the user:"
echo "========================================="
cat $tempfile
echo ""

# extract the driver memory
driverMemory=1g
maxResultSize=1g

while IFS='' read -r line || [ -n "$line" ]; do
	if echo "$line" | grep 'driver_memory'; then
		driverMemory=${line#*: }
	fi
	if echo "$line" | grep 'max_result_size'; then
		maxResultSize=${line#*: }
	fi
done < "$tempfile"

# submit the application to spark cluster. 
spark-submit --verbose --class io.arabesque.GJRunner arabesque-1.0.1-SPARK-jar-with-dependencies.jar -y $(basename $tempfile)

# remove the tempfile
rm $tempfile
