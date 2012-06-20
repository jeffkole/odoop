# Extra Java CLASSPATH elements.  Optional.
if [ -d /Users/jeff/Documents/projects/opower/odoop/hbase-filters/target ]; then
    FILTER_JAR=`ls /Users/jeff/Documents/projects/opower/odoop/hbase-filters/target/hbase-filters-*-SNAPSHOT.jar`
    if [ -n "$FILTER_JAR" ]; then
        # echo "Including hbase-filters jar in HBase classpath"
        export HBASE_CLASSPATH="$FILTER_JAR"
    fi
fi
