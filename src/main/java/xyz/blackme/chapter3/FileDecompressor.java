package xyz.blackme.chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.net.URI;

/**
 * @author zhangliang
 * @create 2018-12-18 上午 10:21
 */
public class FileDecompressor {

    public static void main(String[] args) throws Exception{
        String uri = args[0];
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri), configuration);
        Path inputPath = new Path(uri);
        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(configuration);
        CompressionCodec compressionCodec = compressionCodecFactory.getCodec(inputPath);
    }
}
