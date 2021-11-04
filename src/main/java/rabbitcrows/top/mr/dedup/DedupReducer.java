package rabbitcrows.top.mr.dedup;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author LEHOSO
 * @date 2021/11/5
 * @apinote
 */
public class DedupReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    //<2021-11-1,a,null><2021-11-2,b,null><2021-11-3,c,null>

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
