package xyz.blackme.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangliang
 * @create 2018-12-28 下午 4:54
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key , Iterable<IntWritable> values , Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable intWritable : values) {
            count += intWritable.get();
        }
        context.write(key, new IntWritable(count));
    }
}
