# Extra Java CLASSPATH elements.  Optional.

# BASH_SOURCE is the path of this file, since it is the source being executed
TARGET_DIR="$(dirname $(dirname $BASH_SOURCE))/target"
if [ -d $TARGET_DIR ]; then
    FILTER_JAR=`ls $TARGET_DIR/deployed-filter-*-bin.jar 2> /dev/null`
    if [ -n "$FILTER_JAR" ]; then
        # Echo to stderr, because the output here comes out when you run "hbase classpath"
        # and this is not usable as a classpath
        echo "Including hbase-filters jar in HBase classpath" 1>&2
        export HBASE_CLASSPATH="$FILTER_JAR"
    fi
fi
