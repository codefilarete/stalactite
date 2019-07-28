package org.gama.stalactite.sql.binder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import org.gama.lang.collection.Arrays;

/**
 * Implementation of {@link Blob} based on in-memory storage which is far from optimized. Far from perfect :
 * - {@link SQLException} should be thrown when arguments are not into the bounds of internal buffer
 * - optimization could be done on {@link #getBinaryStream()}.write because returned {@link OutputStream} doesn't override {@link OutputStream#write(byte[], int, int)}
 * 
 * @author Guillaume Mary
 */
public class InMemoryBlobSupport implements Blob {
	
	private byte[] buffer;
	
	public InMemoryBlobSupport(int length) {
		this(new byte[length]);
	}
	
	public InMemoryBlobSupport(byte[] buffer) {
		this.buffer = buffer;
	}
	
	public byte[] getBuffer() {
		return buffer;
	}
	
	@Override
	public long length() throws SQLException {
		assertNotFreed();
		return buffer.length;
	}
	
	@Override
	public byte[] getBytes(long pos, int length) throws SQLException {
		assertNotFreed();
		byte[] result = new byte[length];
		System.arraycopy(buffer, (int) pos-1, result, 0, length);
		return result;
	}
	
	@Override
	public InputStream getBinaryStream() throws SQLException {
		assertNotFreed();
		return new ByteArrayInputStream(buffer);
	}
	
	@Override
	public InputStream getBinaryStream(long pos, long length) throws SQLException {
		if (pos < 1 || pos > length() || (pos + length) > length()) {
			throw new SQLException("Incompatible position or length with actual byte count : " + length() + " vs " + pos + " + " + length);
		}
		return new ByteArrayInputStream(getBytes(pos, (int) length));
	}
	
	@Override
	public long position(byte[] pattern, long start) throws SQLException {
		assertNotFreed();
		boolean found = true;
		int currIdx = (int) start-1;	// -1 because our internal array starts at 0 and argument starts at 1
		int offset = 0;
		while(found && currIdx < this.buffer.length && offset < pattern.length) {
			found = this.buffer[currIdx] == pattern[offset];
			offset++;
			currIdx++;
		}
		if (offset != pattern.length) {
			return position(pattern, start +1);
		} else {
			return (long) currIdx - offset + 1;
		}
	}
	
	@Override
	public long position(Blob pattern, long start) throws SQLException {
		return position(pattern.getBytes(1, (int) pattern.length()), start);
	}
	
	@Override
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		return setBytes(pos, bytes, 0, bytes.length);
	}
	
	@Override
	public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
		assertNotFreed();
		if (pos + len > buffer.length) {
			resizeBuffer((int) (pos - 1 + len));
		}
		for (int i = 0; i < len; i++) {
			this.buffer[(int) (pos-1 + i)] = bytes[i + offset];
		}
		return len;
	}
	
	private void resizeBuffer(int newLength) {
		byte[] extendedBuffer = new byte[newLength];
		System.arraycopy(this.buffer, 0, extendedBuffer, 0, this.buffer.length);
		this.buffer = extendedBuffer;
	}
	
	private void extendBuffer(int chunckSize) {
		resizeBuffer(this.buffer.length + chunckSize);
	}
	
	@Override
	public OutputStream setBinaryStream(long pos) throws SQLException {
		assertNotFreed();
		return new OutputStream() {
			private int offset = (int) pos-1;
			
			// should override write(byte[], int, int) for performance optimization
			
			@Override
			public void write(int b) throws IOException {
				if (offset == buffer.length) {
					extendBuffer(1);
				}
				buffer[offset++] = (byte) b;
			}
		};
	}
	
	@Override
	public void truncate(long len) throws SQLException {
		assertNotFreed();
		if (len < this.buffer.length) {
			this.buffer = Arrays.head(this.buffer, (int) len);
		}
	}
	
	@Override
	public void free() throws SQLException {
		this.buffer = null;
	}
	
	private void assertNotFreed() throws SQLException {
		if (this.buffer == null) {
			throw new SQLException("Blob data is no more available because it was freed");
		}
	}
}
