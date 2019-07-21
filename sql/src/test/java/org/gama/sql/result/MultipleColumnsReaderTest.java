package org.gama.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;

import org.gama.lang.collection.Arrays;
import org.gama.lang.test.Assertions;
import org.junit.jupiter.api.Test;

import static org.gama.lang.collection.Maps.asMap;

/**
 * @author Guillaume Mary
 */
class MultipleColumnsReaderTest {
	
	@Test
	void read() throws SQLException {
		MultipleColumnsReader<Map<String, Object>> testInstance = new MultipleColumnsReader<>(Arrays.asSet(
				new SingleColumnReader<>("key1", ResultSet::getLong),
				new SingleColumnReader<>("key2", ResultSet::getString)),
				Function.identity());
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				asMap("key1", (Object) 13L).add("key2", "a"),
				asMap("key1", (Object) 17L).add("key2", "b"),
				asMap("key1", (Object) 19L).add("key2", "c")
		));
		resultSet.next();
		Map<String, Object> readValues = testInstance.read(resultSet);
		Assertions.assertEquals(asMap("key1", (Object) 13L).add("key2", "a"), readValues);
		
		resultSet.next();
		readValues = testInstance.read(resultSet);
		Assertions.assertEquals(asMap("key1", (Object) 17L).add("key2", "b"), readValues);
	}
	
}