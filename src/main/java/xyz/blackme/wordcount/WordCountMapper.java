package xyz.blackme.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangliang
 * @create 2018-12-28 下午 4:45
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException {
        String line = value.toString().replaceAll("\\.", "").replaceAll(",", "");
        String [] words = line.split(" ");
        for (int i = 0; i < words.length; i++) {
            context.write(new Text(words[i]), new IntWritable(1));
            //System.out.println(String.format("key = %s,value = %s",words[i], 1));
        }
    }
}
