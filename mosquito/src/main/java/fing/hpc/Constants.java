package fing.hpc;

public class Constants {
    public static final int MIN_DAYS_BETWEEN_CHANGE_POINTS = 3;
    public static final long MAX_SECONDS_CHANGEPOINT_EXECUTION = 3 * 60;
    public static final String HDFSMASTER = "hdfs://hadoop-master:9000"; // en local es hdfs://hadoop-master:9000 sino
                                                                         // hdfs://example-cluster-m
    public static final int QTY_NODES_IN_CLUSTER = 8;
}
