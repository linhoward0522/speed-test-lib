package fr.bmartel.speedtest;
import fr.bmartel.protocol.http.HttpFrame;
import fr.bmartel.protocol.http.states.HttpStates;
import fr.bmartel.speedtest.inter.ISpeedTestListener;
import fr.bmartel.speedtest.inter.ISpeedTestSocket;
import fr.bmartel.speedtest.model.FtpMode;
import fr.bmartel.speedtest.model.SpeedTestError;
import fr.bmartel.speedtest.model.SpeedTestMode;
import fr.bmartel.speedtest.model.UploadStorageType;
import fr.bmartel.speedtest.utils.RandomGen;
import fr.bmartel.speedtest.utils.SpeedTestUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.*;
import java.util.List;
import java.util.concurrent.*;
public class FtpProtocolHandler implements ProtocolHandler {
    @Override
    public void handleRequest(String uri, URL url, SpeedTestTask task) {
        String userInfo = url.getUserInfo();
        String user = SpeedTestConst.FTP_DEFAULT_USER;
        String pwd = SpeedTestConst.FTP_DEFAULT_PASSWORD;

        if (userInfo != null && userInfo.indexOf(':') != -1) {
            user = userInfo.substring(0, userInfo.indexOf(':'));
            pwd = userInfo.substring(userInfo.indexOf(':') + 1);
        }
        task.startFtpDownload(uri, user, pwd);
    }
}