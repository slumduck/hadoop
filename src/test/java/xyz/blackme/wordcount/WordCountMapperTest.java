package xyz.blackme.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author zhangliang
 * @create 2018-12-29 上午 11:15
 */
public class WordCountMapperTest {

    @Test
    public void mapTest() throws Exception{
        Text value = new Text("HivHiveQL, .s");
        List<Pair<Text, IntWritable>> result = new ArrayList<>(2);
        result.add(new Pair<Text, IntWritable>(new Text("HivHiveQL"), new IntWritable(1)));
        result.add(new Pair<Text, IntWritable>(new Text("s"), new IntWritable(1)));
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withInput(new LongWritable(0), value)
                //.withOutput(new Text("HivHiveQL"), new IntWritable(1))
                .withAllOutput(result)
                .runTest();
    }

    @Test
    public void reduceTest() throws Exception{
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new WordCountReduce())
                .withInput(new Text("hello world"), Arrays.asList(new IntWritable(1), new IntWritable(2), new IntWritable(3)))
                .withOutput(new Text("hello world"), new IntWritable(6))
                .runTest();

    }


}
