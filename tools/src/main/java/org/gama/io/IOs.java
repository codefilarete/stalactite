package org.gama.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Methods around {@link InputStream} and {@link OutputStream}.
 *
 * @author Guillaume Mary
 */
public final class IOs {
	
	/**
	 * Convert an {@link InputStream} as a {@link ByteArrayInputStream} by using a 1024 byte buffer.
	 * It's up to the caller to close the passed {@link InputStream} argument.
	 *
	 * @param inputStream the source
	 * @return a new {@link ByteArrayInputStream} which content is the input stream
	 * @throws IOException if an error occurs during copy
	 * @see #toByteArrayInputStream(InputStream)
	 */
	public static ByteArrayInputStream toByteArrayInputStream(InputStream inputStream) throws IOException {
		return toByteArrayInputStream(inputStream, 1024);
	}
	
	/**
	 * Convert an {@link InputStream} as a {@link ByteArrayInputStream} by using a byte buffer.
	 * It's up to the caller to close the passed {@link InputStream} argument.
	 *
	 * @param inputStream the source
	 * @param bufferSize the size of the buffer to use (performance optimization)
	 * @return a new {@link ByteArrayInputStream} with ne content of inputStream
	 * @throws IOException if an error occurs during copy
	 */
	public static ByteArrayInputStream toByteArrayInputStream(InputStream inputStream, int bufferSize) throws IOException {
		return new ByteArrayInputStream(toByteArray(inputStream, bufferSize));
	}
	
	/**
	 * Convert an {@link InputStream} as a byte array with a 1024 byte buffer
	 * It's up to the caller to close the passed {@link InputStream} argument.
	 *
	 * @param inputStream the source
	 * @return a byte[] which content is the input stream
	 * @throws IOException if an error occurs during copy
	 * @see #toByteArrayInputStream(InputStream, int)
	 */
	public static byte[] toByteArray(InputStream inputStream) throws IOException {
		return toByteArray(inputStream, 1024);
	}
	
	/**
	 * Convert an {@link InputStream} as a byte array by using a byte buffer.
	 *
	 * @param inputStream the source
	 * @param bufferSize the size of the buffer to use (performance optimization)
	 * @return a byte[] which content is the input stream
	 * @throws IOException if an error occurs during copy
	 */
	public static byte[] toByteArray(InputStream inputStream, int bufferSize) throws IOException {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
			copy(inputStream, bos, bufferSize);
			return bos.toByteArray();
		}
	}
	
	/**
	 * Copy an {@link InputStream} to an {@link OutputStream}
	 *
	 * @param inputStream the source
	 * @param outputStream the target
	 * @param bufferSize the size of packet
	 * @throws IOException if an error occurs during copy
	 */
	public static void copy(InputStream inputStream, OutputStream outputStream, int bufferSize) throws IOException {
		int b;
		byte[] readBytes = new byte[bufferSize];
		while ((b = inputStream.read(readBytes)) != -1) {
			outputStream.write(readBytes, 0, b);
		}
	}
	
	private IOs() {
	}
}
