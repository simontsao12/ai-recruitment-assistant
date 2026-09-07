package dev.tc.recruitment.auth;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TokenCipher {
    private final String configuredKey;
    private final SecureRandom random = new SecureRandom();
    public TokenCipher(@Value("${app.auth.token-key:}") String key) { configuredKey = key; }
    private SecretKeySpec key() {
        byte[] bytes;
        try { bytes = Base64.getDecoder().decode(configuredKey); }
        catch (IllegalArgumentException e) { throw new IllegalStateException("TOKEN_ENCRYPTION_KEY must be base64"); }
        if (bytes.length != 32) throw new IllegalStateException("TOKEN_ENCRYPTION_KEY must encode 32 bytes");
        return new SecretKeySpec(bytes, "AES");
    }
    public String encrypt(String text, String context) {
        try {
            byte[] iv = new byte[12]; random.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key(), new GCMParameterSpec(128, iv));
            cipher.updateAAD(context.getBytes(StandardCharsets.UTF_8));
            byte[] encrypted = cipher.doFinal(text.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(ByteBuffer.allocate(iv.length + encrypted.length)
                    .put(iv).put(encrypted).array());
        } catch (Exception e) { throw new IllegalStateException("TOKEN_ENCRYPTION_FAILED"); }
    }
    public String decrypt(String encoded, String context) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(Base64.getDecoder().decode(encoded));
            byte[] iv = new byte[12]; buffer.get(iv);
            byte[] encrypted = new byte[buffer.remaining()]; buffer.get(encrypted);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key(), new GCMParameterSpec(128, iv));
            cipher.updateAAD(context.getBytes(StandardCharsets.UTF_8));
            return new String(cipher.doFinal(encrypted), StandardCharsets.UTF_8);
        } catch (Exception e) { throw new IllegalStateException("TOKEN_DECRYPTION_FAILED"); }
    }
}
