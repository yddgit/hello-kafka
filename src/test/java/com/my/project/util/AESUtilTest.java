package com.my.project.util;

import static org.junit.Assert.*;
import static com.my.project.util.AESUtil.*;

import org.junit.Test;

/**
 * References: Java Cryptography Architecture (JCA) Reference Guide
 * https://docs.oracle.com/javase/8/docs/technotes/guides/security/crypto/CryptoSpec.html
 * 
 * AES加解密java默认支持128位的key, 对于192位、256位的key，需要安装jce_policy-8
 * 下载地址: http://download.oracle.com/otn-pub/java/jce/8/jce_policy-8.zip
 * 
 * 对于docker容器来说，可以参考install-jce_policy-8-for-docker.txt安装jce_policy-8
 * 
 * @author yang
 */
public class AESUtilTest {

	/**
	 * 128位的iv
	 */
	private String iv = "53ae74be57bd6d25";

	/**
	 * Java默认支持
	 */
	@Test
	public void test16Key() throws Exception {
		String key = "93a20e7519b492d0";
		String e = encrypt("Hello World!", key, iv);
		assertEquals(e, "veaIyFVQ7sZIQ+ilFyUrHA==");
		String d = decrypt(e, key, iv);
		assertEquals(d, "Hello World!");
	}

	/**
	 * 需要安装jce_policy-8
	 */
	@Test
	public void test24Key() throws Exception {
		String key = "0983001ef25668f493a20e75";
		String e = encrypt("Hello World!", key, iv);
		assertEquals(e, "gC+6NQukcnBI3uUcXuLKQw==");
		String d = decrypt(e, key, iv);
		assertEquals(d, "Hello World!");
	}

	/**
	 * 需要安装jce_policy-8
	 */
	@Test
	public void test32Key() throws Exception {
		String key = "53ae74be57bd6d25b80b7f295a52e8da";
		String e = encrypt("Hello World!", key, iv);
		assertEquals(e, "xr0A27M64p7OK5bi8gFFrg==");
		String d = decrypt(e, key, iv);
		assertEquals(d, "Hello World!");
	}

}
