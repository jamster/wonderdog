CLASSPATH=$CLASSPATH:$ES_HOME/lib/*

# Arguments to pass to the JVM
# java.net.preferIPv4Stack=true: Better OOTB experience, especially with jgroups
JAVA_OPTS=" \
        -Xms128M \
        -Xmx5120m \
        -Djline.enabled=true \
        -Djava.net.preferIPv4Stack=true \
        -XX:+AggressiveOpts \
        -XX:+UseParNewGC \
        -XX:+UseConcMarkSweepGC \
        -XX:+CMSParallelRemarkEnabled \
        -XX:+HeapDumpOnOutOfMemoryError \
        -XX:CMSInitiatingOccupancyFraction=88 \
        -Des.path.conf=/etc/elasticsearch"

# run compressed pointers to save on heap
JAVA_OPTS="$JAVA_OPTS -XX:+UseCompressedOops"
