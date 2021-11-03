package top.rabbitcrows.top.hdfsdemo.mr.InvertedIndex;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author LEHOSO
 * @date 2021/11/2
 * @apinote
 */
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    //存储单词和文档名称
    private static Text KeyInfo = new Text();

    //存储词频，初始化为1
    private static final Text valueInfo = new Text("1");

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fileds = StringUtils.split(line, " ");
        //得到这行数据所在的文件切片
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        //根据文件切片得到文件名
        String fileName = fileSplit.getPath().getName();
        for (String filed : fileds) {
            //key值由单词和文档名称组成，如“MapReduce：file1.txt”
            KeyInfo.set(filed + ":" + fileName);
            context.write(KeyInfo, valueInfo);
        }
    }
}
