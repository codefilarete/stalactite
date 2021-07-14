package org.gama.stalactite.sql.binder;

import java.io.IOException;
import java.sql.SQLException;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.gama.lang.exception.Exceptions;
import org.gama.lang.io.IOs;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Guillaume Mary
 */
class InMemoryBlobSupportTest {
	
	@Test
	void length() throws SQLException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport(3);
		assertThat(testInstance.length()).isEqualTo(3);
	}
	
	@Test
	void getBytes() throws SQLException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport("Hello".getBytes());
		assertThat(new String(testInstance.getBytes(2, 3))).isEqualTo("ell");
	}
	
	@Test
	void getBinaryStream() throws SQLException, IOException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport("Hello".getBytes());
		assertThat(new String(IOs.toByteArray(testInstance.getBinaryStream()))).isEqualTo("Hello");
	}
	
	@Test
	void getBinaryStream_offsetAndLength() throws SQLException, IOException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport("Hello".getBytes());
		assertThat(new String(IOs.toByteArray(testInstance.getBinaryStream(2, 3)))).isEqualTo("ell");
		assertThatThrownBy(() -> testInstance.getBinaryStream(2, 55))
				.extracting(t -> Exceptions.findExceptionInCauses(t, SQLException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Incompatible position or length with actual byte count : 5 vs 2 + 55");
	}
	
	@Test
	void position() throws SQLException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		assertThat(testInstance.position("ell".getBytes(), 1)).isEqualTo(2);
		assertThat(testInstance.position("ld !".getBytes(), 3)).isEqualTo(10);
	}
	
	@Test
	void position_blob() throws SQLException {
		InMemoryBlobSupport testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		assertThat(testInstance.position(new InMemoryBlobSupport("ell".getBytes()), 1)).isEqualTo(2);
		assertThat(testInstance.position(new InMemoryBlobSupport("ld !".getBytes()), 3)).isEqualTo(10);
	}
	
	@Test
	void setBytes() throws SQLException {
		InMemoryBlobSupport testInstance;

		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "Lord ".getBytes());
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello Lord  !");

		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "Sir   ".getBytes());
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello Sir   !");

		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(10, "ms".getBytes());
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello worms !");

		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "everybody !".getBytes());
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello everybody !");
	}
	
	@Test
	void setBytes_offsetAndLength() throws SQLException {
		InMemoryBlobSupport testInstance;
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "Lord Sir   ms".getBytes(), 0, 5);
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello Lord  !");
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "Lord Sir   ms".getBytes(), 5, 5);
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello Sir   !");
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(10, "Lord Sir   ms".getBytes(), 11, 2);
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello worms !");
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBytes(7, "everybody !".getBytes(), 0, 11);
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello everybody !");
	}
	
	@Test
	void setBinaryStream() throws SQLException, IOException {
		InMemoryBlobSupport testInstance;
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBinaryStream(7).write("Lord Sir   ms".getBytes(), 0, 5);
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello Lord  !");
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBinaryStream(7).write("Lord Sir   ms".getBytes(), 5, 5);
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello Sir   !");
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBinaryStream(10).write("Lord Sir   ms".getBytes(), 11, 2);
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello worms !");
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.setBinaryStream(7).write("everybody !".getBytes(), 0, 11);
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello everybody !");
	}
	
	@Test
	void truncate() throws SQLException {
		InMemoryBlobSupport testInstance;
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.truncate(5);
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello");
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.truncate(50);
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello world !");
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.truncate(testInstance.length());
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello world !");
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.truncate(testInstance.length() -1);
		assertThat(new String(testInstance.getBuffer())).isEqualTo("Hello world ");
	}
	
	@Test
	void free() throws SQLException {
		InMemoryBlobSupport testInstance;
		
		testInstance = new InMemoryBlobSupport("Hello world !".getBytes());
		testInstance.free();
		assertThatThrownBy(testInstance::getBinaryStream)
				.extracting(t -> Exceptions.findExceptionInCauses(t, SQLException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Blob data is no more available because it was freed");
	}
}