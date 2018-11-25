package Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @author xiang
 * @date 2018/11/8
 */
public class JS_hdfs {


    public static FileSystem fs;

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.31.20.176:8020");
        System.setProperty("HADOOP_USER_NAME", "misas_dev");
        fs = FileSystem.get(conf);
        String date = getDate();
        String localpath = "/data1/Data/ApkUrlData/apkData_JS_TMP/" + date + ".txt";
        String Fspath = "/user/misas_dev/data/JS/apkurl/" + date.substring(0, 6);
        append(localpath, Fspath);

        fs.close();
    }

    //DES2018111315593351126FX014.txt
    public static void append(String localpath, String FSpath) throws IOException {

        Path path = new Path(FSpath);
        FSDataOutputStream append = null;
        if (fs.exists(path)) {

            append = fs.append(path);
        } else {
            append = fs.create(path);
            append.size();
        }

        BufferedReader reader = new BufferedReader(new FileReader(localpath));

        String line = " ";
        while (line != null) {

            line = reader.readLine();
            if (line == null) break;
            line = line + "\n";
            append.writeBytes(line);
        }
        append.flush();
        append.close();

    }

    public static String getDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-1);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String s = dateFormat.format(calendar.getTime()).toString();


        return s;

    }

}
