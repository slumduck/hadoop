package xyz.blackme.chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author zhangliang
 * @create 2018-12-17 下午 5:28
 */
public class StreamCompressor {
    public static void main(String[] args) throws Exception{
        String codecClassName = args[0];
        Class<?> codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
        CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        CompressionOutputStream out = compressionCodec.createOutputStream(System.out);
        IOUtils.copyBytes(System.in, out, 4096, false);
        out.finish();
    }
}
