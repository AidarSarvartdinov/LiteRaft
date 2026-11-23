package io.literaft.transport.exception;

public class ProtocolViolationException extends RuntimeException {
    public ProtocolViolationException(String message) {
        super(message);
    }
}
