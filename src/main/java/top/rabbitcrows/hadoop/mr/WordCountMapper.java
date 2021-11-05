package top.rabbitcrows.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 这里就是MapReduce程序 Map阶段业务逻辑实现的类 Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * <p>
 * KEYIN:表示mapper数据输入时key的数据类型，在默认读取数据组件下，叫作ImportFormat,它的行为是每行读取待处理的数据
 * 读取一行，就返回一行给MR程序，这种情况下 KEYIN就表示每一行的起始偏移，因此数据类型是Long
 * <p>
 * VALUEIN:表示mapper数据输入的时候Value的数据类型，在默认读取数据组件下，
 * valueIN就表示读取的一行内容 因此数据类型是String
 * <p>
 * KEYOUT:表示mapper阶段数据输出的时候key的数据类型，在本案例中输出的key是单词，因此数据类型是String
 * ValueOUT:表示mapper阶段数据输出的时候value的数据类型，在本案例中输出的value是单次的此书，因此数据类型是Integer
 * <p>
 * 这里所说的数据类型String,Long都是JDK的自带的类型，
 * 数据在分布式系统中跨网络传输就需要将数据序列化，默认JDK序列化时效率低下，因此
 * 使用Hadoop封装的序列化类型。 long--LongWritable String --Text Integer intWritable ....
 *
 * @author LEHOSO
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 这里就是mapper阶段具体业务逻辑实现的方法 该方法的调用取决于读取数据的组件有没有给MR传入数据
     * 如果有数据传入，每一个<k,v>对，map就会被调用一次
     */
    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // 拿到传入进来的一行内容，把数据类型转换为String
        String line = value.toString();
        // 将这行内容按照分隔符切割
        String[] words = line.split(" ");
        // 遍历数组，每出现一个单词就标记一个数组1 例如：<单词,1>
        for (String word : words) {
            // 使用MR上下文context，把Map阶段处理的数据发送给Reduce阶段作为输入数据
            context.write(new Text(word), new IntWritable(1));
            //第一行 hadoop hadoop spark  发送出去的是<hadoop,1><hadoop,1><spark,1>
        }
    }
}
