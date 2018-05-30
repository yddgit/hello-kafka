package com.my.project.util;

import java.security.MessageDigest;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * SHA消息摘要
 * @author yang
 */
public class SHAUtil {

	private static final String ENCODE = "UTF-8";

	public static String hmacSha256(String data, String key) throws Exception {
		String algorithm = "HmacSHA256";
		// Create secret key for HmacSHA256
        SecretKey sk = new SecretKeySpec(key.getBytes(ENCODE), algorithm);
        // Get instance of Mac object implementing HmacSHA256, and
        // initialize it with the above secret key
        Mac mac = Mac.getInstance(algorithm);
        mac.init(sk);
        byte[] result = mac.doFinal(data.getBytes(ENCODE));
        return byte2Hex(result);
	}

	public static String md5(String data) throws Exception {
		return getMessage(data, "MD5");
	}

	public static String sha256(String data) throws Exception {
		return getMessage(data, "SHA-256");
	}

	public static String sha384(String data) throws Exception {
		return getMessage(data, "SHA-384");
	}

	public static String sha512(String data) throws Exception {
		return getMessage(data, "SHA-512");
	}

	private static String getMessage(String data, String algorithm) throws Exception {
		if(data == null || algorithm == null) {
			return null;
		}
		MessageDigest messageDigest = MessageDigest.getInstance(algorithm);
		messageDigest.update(data.getBytes(ENCODE));
		return byte2Hex(messageDigest.digest());
	}

	private static String byte2Hex(byte[] bytes) {
		StringBuffer message = new StringBuffer();
		String temp = null;
		for(byte b : bytes) {
			temp = Integer.toHexString(b & 0xFF);
			if(temp.length() == 1) {
				message.append("0");
			}
			message.append(temp);
		}
		return message.toString();
	}

}
