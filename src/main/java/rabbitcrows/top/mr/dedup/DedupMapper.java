package rabbitcrows.top.mr.dedup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author LEHOSO
 * @date 2021/11/5
 * @apinote
 */
public class DedupMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static Text field = new Text();

    //<0,2021-11-1 a><11,2021-11-2 b>
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        field = value;
        //NullWritable.get()方法设置空值
        context.write(field, NullWritable.get());
        // <2018-3-3 c,null> <2018-3-4 d,null>

    }
}
