package top.rabbitcrows.top.hdfsdemo.mr.InvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author LEHOSO
 * @date 2021/11/2
 * @apinote
 */
public class InvertedIndexDriver {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf);

        job.setJarByClass(InvertedIndexDriver.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setCombinerClass(InvertedIndexCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        // 指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(job,
                new Path("input/InvertedIndex/"));
        // 指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job,
                new Path("output/InvertedIndex"));

        // 提交程序并且监控打印程序执行情况
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
