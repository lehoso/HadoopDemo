package top.rabbitcrows.top.hdfsdemo.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//都要继承Reducer 这就是我们所说的变成模型，只需要套模板就行了

/**
 * 这里是MR程序 reducer阶段处理的类
 * <p>
 * KEYIN：就是Reducer阶段输入的数据key类型，对应Mapper阶段输出KEY类型 ，在本案例中就是单词
 * <p>
 * VALUEIN：就是Reducer阶段输入的数据value类型，对应Mapper阶段输出VALUE类型 ，在本案例中就是个数
 * <p>
 * KEYOUT:就是Reducer阶段输出的数据key类型，在本案例中，就是单词 Text
 * <p>
 * VALUEOUT:reducer阶段输出的数据value类型，在本案例中，就是单词的总次数
 *
 * @author LEHOSO
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 这里是REDUCE阶段具体业务类的实现方法
     * 第一行 hadoop hadoop spark  发送出去的是<hadoop,1><hadoop,1><spark,1>
     * reduce接受所有来自Map阶段处理的数据之后，按照Key的字典序进行排序
     * 按照key是否相同作一组去调用reduce方法
     * 本方法的key就是这一组相同的kv对 共同的Key
     * 把这一组的所有v作为一个迭代器传入我们的reduce方法
     * <p>
     * 迭代器：<hadoop,[1,1]>
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> value,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        //定义一个计数器
        int count = 0;
        //遍历一组迭代器，把每一个数量1累加起来就构成了单词的总次数

        //
        for (IntWritable iw : value) {
            count += iw.get();
        }
        context.write(key, new IntWritable(count));
    }
}
