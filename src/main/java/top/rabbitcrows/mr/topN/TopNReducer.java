package top.rabbitcrows.mr.topN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * @author LEHOSO
 * @date 2021/11/5
 * @apinote
 */
public class TopNReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {

    private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>(new Comparator<Integer>() {

        //返回一个基本类型的整型,谁大谁排后面.
        //返回负数表示：o1 小于o2
        //返回0表示:表示：o1和o2相等
        //返回正数表示：o1大于o2。
        public int compare(Integer a, Integer b) {
            return b - a;
        }
    });

    public void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for (IntWritable value : values) {
            repToRecordMap.put(value.get(), " ");
            if (repToRecordMap.size() > 5) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }
        for (Integer i : repToRecordMap.keySet()) {
            context.write(NullWritable.get(), new IntWritable(i));
        }
    }
}
