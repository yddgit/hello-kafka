package com.my.project.util;

import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES加解密工具
 * @author yang
 */
public class AESUtil {

	private static final String ALGORITHM = "AES/CBC/PKCS5Padding";
	private static final String ENCODE = "UTF-8";

	public static String encrypt(String data, String key, String iv) throws Exception {
		SecretKey secretKey = new SecretKeySpec(key.getBytes(ENCODE), "AES");
		Cipher cipher = Cipher.getInstance(ALGORITHM);
		cipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(iv.getBytes(ENCODE)));
		byte[] encrypted = cipher.doFinal(data.getBytes(ENCODE));
		return Base64.getEncoder().encodeToString(encrypted);
	}

	public static String decrypt(String data, String key, String iv) throws Exception {
		SecretKey secretKey = new SecretKeySpec(key.getBytes(ENCODE), "AES");
		Cipher cipher = Cipher.getInstance(ALGORITHM);
		cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv.getBytes(ENCODE)));
		byte[] decrypted = cipher.doFinal(Base64.getDecoder().decode(data));
		return new String(decrypted, ENCODE);
	}

}
