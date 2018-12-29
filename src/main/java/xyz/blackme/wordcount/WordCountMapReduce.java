package xyz.blackme.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author zhangliang
 * @create 2018-12-28 下午 4:59
 */
public class WordCountMapReduce extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration, "wordCount");
        //job 入口
        job.setJarByClass(getClass());
        //map,reduce
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReduce.class);
        job.setReducerClass(WordCountReduce.class);
        //输出数据类型
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        //数据来源，数据存放位置
        //设置运行报错
        //job.setInputFormatClass(FileInputFormat.class);
        //job.setOutputFormatClass(FileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int code = ToolRunner.run(new WordCountMapReduce(), args);
        System.exit(code);
    }
}
