package org.gama.sql.binder;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class HSQLDBParameterBindersTest {
	
	private DataSource dataSource;
	
	@BeforeEach
	public void createDataSource() {
		dataSource = new HSQLDBInMemoryDataSource();
	}
	
	@Test
	public void testBinaryStreamBinder() throws SQLException {
		InputStream inputStream = new ByteArrayInputStream("Hello world !".getBytes());
		LinkedHashSet<InputStream> valuesToInsert = Arrays.asSet(inputStream, null);
		ParameterBinder<InputStream> binarystreamBinder = HSQLDBParameterBinders.BINARYSTREAM_BINDER;
		Set<InputStream> databaseContent = DefaultParameterBindersTest.insertAndSelect(dataSource, binarystreamBinder, "blob", valuesToInsert);
		assertEquals(Arrays.asSet(null, "Hello world !"), DefaultParameterBindersTest.convertInputStreamToString(databaseContent));
	}
	
}