package top.rabbitcrows.mr.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author LEHOSO
 * @date 2021/11/5
 * @apinote
 */
public class TopNDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(TopNDriver.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1);
        //map阶段输出的key
        job.setMapOutputKeyClass(NullWritable.class);
        //map阶段输出的value
        job.setMapOutputValueClass(IntWritable.class);
        //reduce阶段输出的key
        job.setOutputKeyClass(NullWritable.class);
        //reduce阶段输出的value
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("input/TopN/num.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output/TopN"));

        boolean res = job.waitForCompletion(true);
        System.out.println(res ? 0 : 1);

    }

}
