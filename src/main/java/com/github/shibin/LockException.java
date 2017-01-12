package com.github.shibin;

public class LockException extends RuntimeException{

    public LockException(String message) {
        super(message);
    }

    public LockException(Throwable e) {
        super(e);
    }

    public LockException(String message, Throwable cause) {
        super(message, cause);
    }
}
