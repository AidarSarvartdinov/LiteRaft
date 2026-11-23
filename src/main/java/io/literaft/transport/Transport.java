package io.literaft.transport;

public interface Transport {
    void send(int peerId, String message);
    void broadcast(String message);
}
