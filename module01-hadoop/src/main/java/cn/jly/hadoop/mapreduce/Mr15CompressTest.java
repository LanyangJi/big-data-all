package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.compress.bzip2.Bzip2Compressor;
import org.apache.hadoop.util.ReflectionUtils;

import java.beans.MethodDescriptor;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 压缩测试
 *
 * @author lanyangji
 * @date 2021/4/26 下午 7:24
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr15CompressTest
 */
public class Mr15CompressTest extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        compress("e:/google.txt", BZip2Codec.class.getName());

        decompress("e:/google.txt.bz2");
    }

    /**
     * 压缩
     *
     * @param fileName
     * @param method
     */
    public static void compress(String fileName, String method) throws Exception {
        // 反射构建压缩对象
        final Class<?> clazz = Class.forName(method);
        final CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz, new Configuration());

        try (
                final FileInputStream fis = new FileInputStream(fileName);
                final FileOutputStream fos = new FileOutputStream(fileName + codec.getDefaultExtension());
                final CompressionOutputStream cos = codec.createOutputStream(fos)
        ) {
            // 拷贝流
            IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);
        }
    }

    /**
     * 解压缩
     *
     * @param fileName
     */
    public static void decompress(String fileName) throws Exception {
        // 校验能够解压缩
        final CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
        final CompressionCodec codec = codecFactory.getCodec(new Path(fileName));
        if (null == codec) {
            System.out.println("can not decompress file : " + fileName);
            return;
        }

        try (
                final CompressionInputStream cis = codec.createInputStream(new FileInputStream(fileName));
                final FileOutputStream fos = new FileOutputStream(fileName+".decoded")
        ) {
            // 流拷贝
            IOUtils.copyBytes(cis, fos, 1024 * 1024 * 5, false);
        }
    }
}
