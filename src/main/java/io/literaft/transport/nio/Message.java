package io.literaft.transport.nio;

import java.nio.ByteBuffer;

public record Message (
    int peerId,
    ByteBuffer payload
) {    
}
