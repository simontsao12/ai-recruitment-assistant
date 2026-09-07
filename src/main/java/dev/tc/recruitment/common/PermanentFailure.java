package dev.tc.recruitment.common;

/** Safe machine-readable error; never includes candidate content or provider response bodies. */
public class PermanentFailure extends RuntimeException {
    public PermanentFailure(String code) { super(code); }
}
