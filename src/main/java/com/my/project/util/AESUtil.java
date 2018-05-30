package com.my.project.util;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

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
		return Base64.encodeBase64String(encrypted);
	}

	public static String decrypt(String data, String key, String iv) throws Exception {
		SecretKey secretKey = new SecretKeySpec(key.getBytes(ENCODE), "AES");
		Cipher cipher = Cipher.getInstance(ALGORITHM);
		cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv.getBytes(ENCODE)));
		byte[] decrypted = cipher.doFinal(Base64.decodeBase64(data));
		return new String(decrypted, ENCODE);
	}

}
