package top.rabbitcrows.top.hdfsdemo.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver类就是MR程序运行的主类，本类中组装了一些程序运行时所需要的信息
 * 比如：使用的Mapper类是什么，Reducer类，数据在什么地方，输出在哪里
 *
 * @author LEHOSO
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception {
        // 通过Job来封装本次MR的相关信息
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        Job wcjob = Job.getInstance(conf);

        // 指定MR Job jar包运行主类
        wcjob.setJarByClass(WordCountDriver.class);
        // 指定本次MR所有的Mapper Reducer类
        wcjob.setMapperClass(WordCountMapper.class);
        wcjob.setReducerClass(WordCountReducer.class);

        // 设置我们的业务逻辑 Mapper类的输出 key和 value的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(IntWritable.class);

        // 设置我们的业务逻辑 Reducer类的输出 key和 value的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(IntWritable.class);

        //设置Combiner组件
        wcjob.setCombinerClass(WordCountCombiner.class);

        // 指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob, new Path("input/mr"));
        // 指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(wcjob, new Path("output/mr"));

        // 提交程序并且监控打印程序执行情况
        boolean res = wcjob.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
