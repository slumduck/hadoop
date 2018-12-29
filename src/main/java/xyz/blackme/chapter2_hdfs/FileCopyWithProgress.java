package xyz.blackme.chapter2_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * @author zhangliang
 * @create 2018-12-17 上午 11:24
 */
public class FileCopyWithProgress {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        FileSystem fs = null;
        String localPath = args[0];
        String hdfsPath = args[1];
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            fs = FileSystem.get(URI.create(hdfsPath), configuration);
            inputStream = new BufferedInputStream(new FileInputStream(localPath));
            outputStream = fs.create(new Path(hdfsPath) , () -> System.out.print("."));
            IOUtils.copyBytes(inputStream, outputStream, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(outputStream);
            IOUtils.closeStream(inputStream);
        }
    }
}
