package org.gama.sql.binder;

import java.io.IOException;
import java.sql.SQLException;

import org.gama.lang.io.IOs;
import org.gama.lang.test.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
class InMemoryBlobSupportTest {
	
	@Test
	void length() throws SQLException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport(3);
		assertEquals(3, testInstance.length());
	}
	
	@Test
	void getBytes() throws SQLException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport("Hello".getBytes());
		assertEquals("ell", new String(testInstance.getBytes(2, 3)));
	}
	
	@Test
	void getBinaryStream() throws SQLException, IOException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport("Hello".getBytes());
		assertEquals("Hello", new String(IOs.toByteArray(testInstance.getBinaryStream())));
	}
	
	@Test
	void getBinaryStream_offsetAndLength() throws SQLException, IOException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport("Hello".getBytes());
		assertEquals("ell", new String(IOs.toByteArray(testInstance.getBinaryStream(2, 3))));
		Assertions.assertThrows(() -> testInstance.getBinaryStream(2, 55), Assertions.hasExceptionInCauses(SQLException.class)
				.andProjection(Assertions.hasMessage("Incompatible position or length with actual byte count : 5 vs 2 + 55")));
	}
	
	@Test
	void position() throws SQLException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		assertEquals(2, testInstance.position("ell".getBytes(), 1));
		assertEquals(10, testInstance.position("ld !".getBytes(), 3));
	}
	
	@Test
	void position_blob() throws SQLException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		assertEquals(2, testInstance.position(new InMemoryBlobSupport("ell".getBytes()), 1));
		assertEquals(10, testInstance.position(new InMemoryBlobSupport("ld !".getBytes()), 3));
	}
	
	@Test
	void setBytes() throws SQLException {
		InMemoryBlobSupport testInstance;

		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "Lord ".getBytes());
		assertEquals("Hello Lord  !", new String(testInstance.getBuffer()));

		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "Sir   ".getBytes());
		assertEquals("Hello Sir   !", new String(testInstance.getBuffer()));

		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(10, "ms".getBytes());
		assertEquals("Hello worms !", new String(testInstance.getBuffer()));

		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "everybody !".getBytes());
		assertEquals("Hello everybody !", new String(testInstance.getBuffer()));
	}
	
	@Test
	void setBytes_offsetAndLength() throws SQLException {
		InMemoryBlobSupport testInstance;
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "Lord Sir   ms".getBytes(), 0, 5);
		assertEquals("Hello Lord  !", new String(testInstance.getBuffer()));
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "Lord Sir   ms".getBytes(), 5, 5);
		assertEquals("Hello Sir   !", new String(testInstance.getBuffer()));
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(10, "Lord Sir   ms".getBytes(), 11, 2);
		assertEquals("Hello worms !", new String(testInstance.getBuffer()));
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "everybody !".getBytes(), 0, 11);
		assertEquals("Hello everybody !", new String(testInstance.getBuffer()));
	}
	
	@Test
	void setBinaryStream() throws SQLException, IOException {
		InMemoryBlobSupport testInstance;
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBinaryStream(7).write("Lord Sir   ms".getBytes(), 0, 5);
		assertEquals("Hello Lord  !", new String(testInstance.getBuffer()));
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBinaryStream(7).write("Lord Sir   ms".getBytes(), 5, 5);
		assertEquals("Hello Sir   !", new String(testInstance.getBuffer()));
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBinaryStream(10).write("Lord Sir   ms".getBytes(), 11, 2);
		assertEquals("Hello worms !", new String(testInstance.getBuffer()));
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBinaryStream(7).write("everybody !".getBytes(), 0, 11);
		assertEquals("Hello everybody !", new String(testInstance.getBuffer()));
	}
	
	@Test
	void truncate() throws SQLException {
		InMemoryBlobSupport testInstance;
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.truncate(5);
		assertEquals("Hello", new String(testInstance.getBuffer()));
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.truncate(50);
		assertEquals("Hello world !", new String(testInstance.getBuffer()));
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.truncate(testInstance.length());
		assertEquals("Hello world !", new String(testInstance.getBuffer()));
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.truncate(testInstance.length() -1);
		assertEquals("Hello world ", new String(testInstance.getBuffer()));
	}
	
	@Test
	void free() throws SQLException {
		InMemoryBlobSupport testInstance;
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.free();
		Assertions.assertThrows(testInstance::getBinaryStream, Assertions.hasExceptionInCauses(SQLException.class)
				.andProjection(Assertions.hasMessage("Blob data is no more available because it was freed")));
	}
}