package xyz.blackme.chapter2_hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @author zhangliang
 * @create 2018-12-17 上午 10:41
 */
public class FileSystemCat {

    public static void main(String[] args) {
        //String uri = args[0];
        String uri = "hdfs://172.19.13.23:9000/home/taiji/data/hbase/hbase.id";
        FileSystem fs = null;
        InputStream in = null;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://39.108.237.119:9000");
        conf.set("dfs.namenode.rpc-address", "172.19.13.23:9001");
        conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        try {
            fs = FileSystem.get(URI.create(uri), conf, "root");
            //FSDataInputStream:支持随机读，指定位置读
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
