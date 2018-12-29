package xyz.blackme.chapter2_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * @author zhangliang
 * @create 2018-12-17 上午 10:58
 */
public class FileSystemDoubleCat {

    public static void main(String[] args) {
        String uri = args[0];
        FileSystem fs = null;
        FSDataInputStream fsDataInputStream = null;
        try {
            fs = FileSystem.get(URI.create(uri), new Configuration());
            fsDataInputStream = fs.open(new Path(uri));
            IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
            fsDataInputStream.seek(0);
            IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
