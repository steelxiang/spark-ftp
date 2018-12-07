package Utils;

import com.enterprisedt.net.ftp.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;


public class load_it {

    public static String host="10.4.41.99";
    public static String username="yanjy_lch";
    public static String password="UxGsD1a#,kA";
    public static String path1="/data/yhb/pdc_in";
    public static String path2="/data/yhb/url_in";

    public static Logger log=Logger.getLogger(load_it.class);
    public static String localdir="/tmp/";


//  ftp.downloadFile(fs,path1,s"gdpi-$date.txt.gzip",s"$tmp/gdpi-$date.txt.gz")
public static void downloadFile(FileSystem fs, String pathname, String filename, String fsname) throws Exception{

    FileTransferClient ftp=new FileTransferClient();
    EventListener eventListener = new myListener();
    ftp.setRemoteHost(host);
    ftp.setUserName(username);
    ftp.setPassword(password);
    ftp.connect();
    ftp.setContentType(FTPTransferType.BINARY);
    ftp.getAdvancedFTPSettings().setConnectMode(FTPConnectMode.ACTIVE);
    ftp.setEventListener(eventListener);

  if(!ftp.exists(pathname+filename)) {
     log.info("文件不存在："+filename);
      return;
  }

    ftp.downloadFile(localdir+filename,pathname+filename, WriteMode.RESUME);


    Path p=new Path(fsname);
    File localfile=new File(localdir+filename);

    if(!localfile.exists()){
        localfile.createNewFile();
    }

        fs.copyFromLocalFile(true,true,new Path(localdir+filename) ,p );

        FileUtils.deleteQuietly(localfile);



    log.info("上传hdfs完成 "+filename);
    ftp.disconnect();
}


}
