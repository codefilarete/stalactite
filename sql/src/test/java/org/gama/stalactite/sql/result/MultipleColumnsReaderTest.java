package org.codefilarete.stalactite.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;

import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.tool.collection.Maps.asMap;

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
		assertThat(readValues).isEqualTo(asMap("key1", (Object) 13L).add("key2", "a"));
		
		resultSet.next();
		readValues = testInstance.read(resultSet);
		assertThat(readValues).isEqualTo(asMap("key1", (Object) 17L).add("key2", "b"));
	}
	
}