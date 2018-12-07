package Utils;

import com.enterprisedt.net.ftp.EventListener;
import org.apache.log4j.Logger;

public class myListener implements EventListener {

    public Logger log=Logger.getLogger(myListener.class);
    @Override
    public void commandSent(String s, String s1) {
    }

    @Override
    public void replyReceived(String s, String s1) {

        log.info("reply from "+s+" is: "+s1 );
    }

    @Override
    public void bytesTransferred(String s, String s1, long l) {

    }

    @Override
    public void downloadStarted(String s, String s1) {
        log.info("开始下载------"+s1);
    }

    @Override
    public void downloadCompleted(String s, String s1) {
        log.info("下载完成-------"+s1);
    }

    @Override
    public void uploadStarted(String s, String s1) {

    }

    @Override
    public void uploadCompleted(String s, String s1) {

    }
}
