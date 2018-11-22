package Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class hadoopGZ {


   public static Configuration conf = new Configuration();

   public static FileSystem fs=null;

   public static String codecClassName="org.apache.hadoop.io.compress.GzipCodec";

    public static void main(String[] args) throws Exception {

       // conf.set("fs.defaultFS", "hdfs://172.31.20.176:8020");
        conf.set("fs.defaultFS", "hdfs://192.168.5.200:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
       // System.setProperty("HADOOP_USER_NAME", "misas_dev");
        System.setProperty("HADOOP_USER_NAME", "root");
        fs = FileSystem.get(conf);
        String FSpath="/data";
        RemoteIterator<LocatedFileStatus> listFiles = fs.listLocatedStatus(new Path(FSpath));


        while (listFiles.hasNext()){
           LocatedFileStatus next = listFiles.next();
           Path path = next.getPath();
           System.out.println("文件夹： "+path);
            RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(path);
            while (iterator.hasNext()) {
                LocatedFileStatus fileStatus = iterator.next();
                Path oldpath = fileStatus.getPath();
                String newfile = oldpath + ".gzip";
                if (fileStatus.getLen() == 0) {
                    fs.delete(oldpath, true);

                    System.out.println("删除空文件： "+oldpath);
                    continue;
                } else {
                    compress(oldpath.toString(), newfile);
                }
            }


       }

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

}