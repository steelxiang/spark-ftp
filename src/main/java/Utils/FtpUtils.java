package Utils;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.MalformedURLException;

/**
 * @author xiang
 * @date 2018/11/4
 */
public class FtpUtils {
    //ftp服务器地址
    public String hostname = "10.4.41.99";
    //ftp服务器端口号默认为21
    public Integer port = 21 ;
    //ftp登录账号
    public String username = "yanjy_lch";
    //ftp登录密码
    public String password = "UxGsD1a#,kA";

    public FTPClient ftpClient = null;
    public String localdir="/tmp/";

    public String fspath="/user/misas_dev/data/tmp/";



    /**
     * 初始化ftp服务器
     */
    public void initFtpClient() {
        ftpClient = new FTPClient();
        ftpClient.setControlEncoding("GB2312");//GB2312  utf-8
        try {
            System.out.println("connecting...ftp服务器:"+this.hostname+":"+this.port);
            ftpClient.connect(hostname, port); //连接ftp服务器
            ftpClient.login(username, password); //登录ftp服务器
            int replyCode = ftpClient.getReplyCode(); //是否成功登录服务器
            if(!FTPReply.isPositiveCompletion(replyCode)){
                System.out.println("connect failed...ftp服务器:"+this.hostname+":"+this.port);
            }
            System.out.println("connect successfu...ftp服务器:"+this.hostname+":"+this.port);
        }catch (MalformedURLException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }


    //改变目录路径
    public boolean changeWorkingDirectory(String directory) {
        boolean flag = true;
        try {
            flag = ftpClient.changeWorkingDirectory(directory);
            if (flag) {
                System.out.println("进入文件夹" + directory + " 成功！");

            } else {
                System.out.println("进入文件夹" + directory + " 失败！开始创建文件夹");
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return flag;
    }


    /** * 下载文件 *
     * @param pathname FTP服务器文件目录 *
     * @param filename 文件名称 *
     * @return */
    public  void downloadFile(FileSystem fs,String pathname, String filename,String fsname) throws IOException {

        //先下载到本地临时文件/tmp/
        initFtpClient();
        System.out.println("开始下载文件 "+filename);
         File localfile=new File(localdir+filename);
         if(!localfile.exists()){
             localfile.createNewFile();
         }

        FileOutputStream fos=new FileOutputStream(localfile);
        ftpClient.retrieveFile(pathname+filename, fos);
        System.out.println("本地下载完成："+filename);
        fos.flush();
        fos.close();

        Path p=new Path(fspath+fsname);

       fs.copyFromLocalFile(new Path(localdir+filename) ,p );

        System.out.println("上传hdfs完成 "+filename);

        ftpClient.logout();
        fs.close();
    }



    public static void main(String[] args) {
        System.out.println("DES2018111315593351126FX014.txt".substring(3,11));


//        FtpUtils ftp =new FtpUtils();
//        //ftp.uploadFile("ftpFile/data", "123.docx", "E://123.docx");
//        //ftp.downloadFile("ftpFile/data", "123.docx", "F://");
//      //  ftp.downloadFile("/data/yhb/url_in", "cdpi-20180428.txt.gzip", "d:\\");
//        System.out.println("ok");
    }
}
