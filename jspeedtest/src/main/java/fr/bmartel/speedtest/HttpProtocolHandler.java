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
public class HttpProtocolHandler implements ProtocolHandler {
    @Override
    public void handleRequest(String uri, URL url, SpeedTestTask task) {
        String downloadRequest;
        if (task.getProxyUrl() != null) {
            task.setHostname(task.getProxyUrl().getHost());
            task.setPort(task.getProxyUrl().getPort() != -1 ? task.getProxyUrl().getPort() : 8080);
            downloadRequest = "GET " + uri + " HTTP/1.1\r\n" + "Host: " + url.getHost() +
                    "\r\nProxy-Connection: Keep-Alive" + "\r\n\r\n";
        } else {
            task.setHostname(url.getHost());
            task.setPort(url.getProtocol().equals("http") ? (url.getPort() != -1 ? url.getPort() : 80) :
                    (url.getPort() != -1 ? url.getPort() : 443));
            downloadRequest = "GET " + uri + " HTTP/1.1\r\n" + "Host: " + url.getHost() + "\r\n\r\n";
        }
        task.writeDownload(downloadRequest.getBytes());
    }
}