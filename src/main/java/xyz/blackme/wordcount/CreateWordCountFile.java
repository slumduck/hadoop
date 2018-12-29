package xyz.blackme.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.net.URI;
import java.nio.file.Files;

/**
 * @author zhangliang
 * @create 2018-12-28 下午 5:27
 */
public class CreateWordCountFile {

    public static void main(String[] args) throws Exception{
        createNew();
    }

    public static void copyFromLocal() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.190.129:9000"), conf,"hadoop");
        Path p = new Path("/user/hadoop/wordcount");
        if (!fs.exists(p)) {
            fs.mkdirs(p);
        }
        fs.copyFromLocalFile(new Path("E:\\GitHub\\hadoop\\src\\main\\java\\xyz\\blackme\\wordcount\\wordcount.txt"),new Path("/user/hadoop/wordcount/wordcount.txt"));
        fs.close();
    }

    public static void createNew() throws Exception{

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.190.129:9000"), conf,"hadoop");
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/user/hadoop/wordcount/wordcount.txt"), true);
        //Files.copy();
        IOUtils.copyBytes(new FileInputStream("E:\\GitHub\\hadoop\\src\\main\\java\\xyz\\blackme\\wordcount\\wordcount.txt"), fsDataOutputStream, 4096);
    }

}
