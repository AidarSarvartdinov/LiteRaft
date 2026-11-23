package io.literaft.transport.nio;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

public class NioSocket {
    private final SocketChannel socketChannel;
    private final int peerId;

    public NioSocket(SocketChannel socketChannel, int peerId) {
        this.socketChannel = socketChannel;
        this.peerId = peerId;
    }
    
    public void onConnect() throws IOException {

    }

    public void onWrite() throws IOException {

    }

    public List<Message> onRead() throws IOException {
        return  null;
    }

    public int getPeerId() {
        return peerId;
    }
}
