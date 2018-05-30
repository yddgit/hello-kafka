package com.my.project.util;

import static org.junit.Assert.*;
import static com.my.project.util.SHAUtil.*;

import org.junit.Test;

public class SHAUtilTest {

	@Test
	public void testHmacSha256() throws Exception {
		String key = "53ae74be57bd6d25";
		assertEquals(
			"7661ebc622994ae7a28e7428a02ac1f3fbca8442a9f13b661dc09a55ca5e4c0f",
			hmacSha256("Hello World!", key));
	}

	@Test
	public void testMd5() throws Exception {
		assertEquals(
			"ed076287532e86365e841e92bfc50d8c",
			md5("Hello World!"));
	}

	@Test
	public void testSha256() throws Exception {
		assertEquals(
			"7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069",
			sha256("Hello World!"));
	}

	@Test
	public void testSha384() throws Exception {
		assertEquals(
			"bfd76c0ebbd006fee583410547c1887b0292be76d582d96c242d2a792723e3fd6fd061f9d5cfd13b8f961358e6adba4a",
			sha384("Hello World!"));
	}

	@Test
	public void testSha512() throws Exception {
		assertEquals(
			"861844d6704e8573fec34d967e20bcfef3d424cf48be04e6dc08f2bd58c729743371015ead891cc3cf1c9d34b49264b510751b1ff9e537937bc46b5d6ff4ecc8",
			sha512("Hello World!"));
	}

}
