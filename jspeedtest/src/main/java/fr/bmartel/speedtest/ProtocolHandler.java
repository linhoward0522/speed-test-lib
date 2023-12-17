package fr.bmartel.speedtest;

public interface ProtocolHandler {
    void handleRequest(String uri, URL url, SpeedTestTask task);
}