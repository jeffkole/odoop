# Extra Java CLASSPATH elements.  Optional.
if [ -d /Users/jeff/Documents/projects/opower/odoop/hbase-filters/target ]; then
    FILTER_JAR=`ls /Users/jeff/Documents/projects/opower/odoop/hbase-filters/target/deployed-filter-*-bin.jar 2> /dev/null`
    if [ -n "$FILTER_JAR" ]; then
        # Echo to stderr, because the output here comes out when you run "hbase classpath"
        # and this is not usable as a classpath
        echo "Including hbase-filters jar in HBase classpath" 1>&2
        export HBASE_CLASSPATH="$FILTER_JAR"
    fi
fi
