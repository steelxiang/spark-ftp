package Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.GZIPInputStream;

public class hadoopGZ {


   public static Configuration conf = new Configuration();

   public static FileSystem fs=null;

   public static String codecClassName="org.apache.hadoop.io.compress.GzipCodec";

    public static void main(String[] args) throws Exception {

        conf.set("fs.defaultFS", "hdfs://172.31.20.176:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
      System.setProperty("HADOOP_USER_NAME", "misas_dev");
      //  System.setProperty("HADOOP_USER_NAME", "root");
        fs = FileSystem.get(conf);
        String FSpath="/data";

          //unGzipFile(fs,"/user/misas_dev/data/tmp/cdpi-20181121.txt.gzip" , "/user/misas_dev/data/tmp/cdpi-20181121.txt");
        unGzip("e:\\cdpi-20181121.gz","e:\\cdpi");


    }

    //压缩文件
    public static void compress(String input,String output) throws Exception{

        if(input.endsWith(".gzip")||input.endsWith(".gz")) return;
        Class<?> codecClass = Class.forName(codecClassName);

        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        //指定压缩文件路径

        FSDataOutputStream outputStream = fs.create(new Path(output));
        //指定要被压缩的文件路径

        FSDataInputStream in = fs.open(new Path(input));
        //创建压缩输出流
        CompressionOutputStream out = codec.createOutputStream(outputStream);
        System.out.println("开始压缩 ：" +input);
        IOUtils.copyBytes(in, out, conf);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        fs.delete(new Path(input),true );
        System.out.println(" 已删除"+input);
    }

    public static void unGzipFile(FileSystem fs,String input,String ouput) {

        try {
            //建立gzip压缩文件输入流
            FSDataInputStream open = fs.open(new Path(input));
            FSDataOutputStream fout = fs.create(new Path(ouput), true);


            //建立gzip解压工作流
            GZIPInputStream gzin = new GZIPInputStream(open);
            //建立解压文件输出流


            int num;
            byte[] buf=new byte[1024];

            while ((num = gzin.read(buf,0,buf.length)) != -1)
            {
                fout.write(buf,0,num);
            }

            gzin.close();
            fout.close();
            open.close();
        } catch (Exception ex){
            System.err.println(ex.toString());
        }
        return;
    }


    public static void unGzip(String input,String ouput) {

        try {
            //建立gzip压缩文件输入流
            FileInputStream open = new FileInputStream(input);
            FileOutputStream fout = new FileOutputStream(ouput);

            //建立gzip解压工作流
            GZIPInputStream gzin = new GZIPInputStream(open);
            //建立解压文件输出流


            int num;
            byte[] buf=new byte[1024];

            while ((num = gzin.read(buf,0,buf.length)) != -1)
            {
                fout.write(buf,0,num);
            }

            gzin.close();
            fout.close();
            open.close();
        } catch (Exception ex){
            System.err.println(ex.toString());
        }
        return;
    }

}