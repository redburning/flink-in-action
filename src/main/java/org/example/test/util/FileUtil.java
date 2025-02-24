package org.example.test.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class FileUtil {

	public static final int MAX_FILE_LENGTH = 50 * 1024 * 1024;

	/**
	 * 读文件
	 * @param file
	 * @return
	 */
	public static String read(File file) {
		long fileLength = file.length();
		if (fileLength >= MAX_FILE_LENGTH) {
			throw new IllegalArgumentException(
					"file " + file + " is to long len " + fileLength + " > " + MAX_FILE_LENGTH);
		}
		InputStream in = null;
		try {
			in = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}

		byte[] bytes = inputStreamToBytes(in, (int) fileLength);
		String text = new String(bytes);

		return text;
	}

	private static byte[] inputStreamToBytes(InputStream in, int size) {
		try {
			return inputStreamToBytes0(in, size);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static byte[] inputStreamToBytes0(InputStream in, int size) throws IOException {
		if (size <= 0) {
			size = 1024;
		}
		ByteArrayOutputStream bytestream = new ByteArrayOutputStream(size);
		byte[] buff = new byte[1024];
		int rc;
		while ((rc = in.read(buff, 0, 1024)) != -1) {
			bytestream.write(buff, 0, rc);
		}
		byte data[] = bytestream.toByteArray();
		bytestream.close();

		return data;
	}
	
	public static void write(File file, String text) {
		if (file.exists()) {
			if (file.isDirectory()) {
				throw new RuntimeException("file " + file + " can not be dir");
			}
			file.delete();
		}
		FileOutputStream out = null;
		try {
			file.createNewFile();
			out = new FileOutputStream(file);

			byte[] bytes = text.getBytes();

			out.write(bytes);
			out.flush();

		} catch (IOException e) {
			throw new RuntimeException("error write file " + file, e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
				}
			}
		}
	}
	
}
