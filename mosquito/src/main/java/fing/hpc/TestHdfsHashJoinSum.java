package fing.hpc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

public class TestHdfsHashJoinSum extends CacheHdfs.CDriver {

    public TestHdfsHashJoinSum() {
        super();
        jobCount = 1;
    }

    @Override
    public void configureJob(Job job, int i) throws Exception {
        super.configureJob(job, i);

        job.setMapperClass(CatDepAnioHashJoinMapper.class);
        // job.setCombinerClass(job_combine_class);
        job.setReducerClass(GroupByReducer.SumAllBySubKey.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TestHdfsHashJoinSum(), args);
        System.exit(exitCode);
    }
}