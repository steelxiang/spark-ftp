package Utils;

import com.enterprisedt.net.ftp.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;


public class load_it {

    public static String host = "10.4.41.99";
    public static String username = "yanjy_lch";
    public static String password = "UxGsD1a#,kA";
    public static String path1 = "/data/yhb/pdc_in";
    public static String path2 = "/data/yhb/url_in";

    public static Logger log = Logger.getLogger(load_it.class);
    public static String localdir = "/tmp/";


    //  ftp.downloadFile(fs,path1,s"gdpi-$date.txt.gzip",s"$tmp/gdpi-$date.txt.gz")
    public static void downloadFile(FileSystem fs, String pathname, String filename, String fsname)  {

        FileTransferClient ftp = new FileTransferClient();
        EventListener eventListener = new myListener();

        int count = 1;
        boolean b = true;

        while (b) {
            try {
                ftp.setRemoteHost(host);
                ftp.setUserName(username);
                ftp.setPassword(password);
                ftp.setContentType(FTPTransferType.BINARY);
                ftp.getAdvancedFTPSettings().setConnectMode(FTPConnectMode.ACTIVE);
                ftp.setEventListener(eventListener);
                ftp.connect();

                if (!ftp.exists(pathname + filename)) {
                    log.info("文件不存在：" + filename);
                    return;
                }
                ftp.downloadFile(localdir + filename, pathname + filename, WriteMode.RESUME);
                b = false;
            } catch (Exception e) {
                log.error("下载失败，重试..." + count + "...次");
                count++;
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                continue;

            }finally {
                try {
                    ftp.disconnect();
                } catch (FTPException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }



        Path p = new Path(fsname);
        File localfile = new File(localdir + filename);

        if (!localfile.exists()) {
            try {
                localfile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            fs.copyFromLocalFile(true, true, new Path(localdir + filename), p);
        } catch (IOException e) {
            e.printStackTrace();
        }

        FileUtils.deleteQuietly(localfile);


        log.info("上传hdfs完成 " + filename);

    }


}
