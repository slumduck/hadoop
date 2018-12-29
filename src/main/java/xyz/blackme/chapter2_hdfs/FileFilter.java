package xyz.blackme.chapter2_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

/**
 * @author zhangliang
 * @create 2018-12-17 下午 2:46
 */
public class FileFilter {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        FileSystem fs = null;
        Pattern pattern = Pattern.compile(".*_access.log");
        try {
            fs = FileSystem.get(URI.create(args[0]), conf);
            Path [] paths = new Path[args.length];
            for (int i = 0; i < paths.length; i++ ) {
                paths[i] = new Path(args[i]);
            }
            org.apache.hadoop.fs.FileStatus [] fileStatus = fs.listStatus(paths , p -> !pattern.matcher(p.toString()).matches());
            Path [] paths1 = FileUtil.stat2Paths(fileStatus);
            for (Path p : paths1) {
                System.out.println(p);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
