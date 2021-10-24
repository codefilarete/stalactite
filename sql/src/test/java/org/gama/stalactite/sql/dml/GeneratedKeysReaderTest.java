package org.gama.stalactite.sql.dml;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.result.InMemoryResultSet;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.gama.lang.collection.Maps.asMap;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
class GeneratedKeysReaderTest {
	
	@Test
	void read() {
		GeneratedKeysReader<Long> testInstance = new GeneratedKeysReader<>("key", ResultSet::getLong);
		List<Long> readValues = testInstance.read(new InMemoryResultSet(Arrays.asList(
				asMap("key", 13L),
				asMap("key", 17L),
				asMap("key", 19L)
		)));
		assertThat(readValues).containsExactly(13L, 17L, 19L);
	}
	
	@Test
	void read_writeOperation() throws SQLException {
		GeneratedKeysReader<Long> testInstance = new GeneratedKeysReader<Long>("key", ResultSet::getLong) {
			@Override
			List<Long> read(ResultSet generatedKeys) {
				return Collections.emptyList();
			}
		};
		WriteOperation<String> dummySQL = new WriteOperation<>(new StringParamedSQL("dummySQL", new HashMap<>()), mock(ConnectionProvider.class), writeCount -> {});
		dummySQL.preparedStatement = mock(PreparedStatement.class);
		testInstance.read(dummySQL);
		Mockito.verify(dummySQL.preparedStatement).getGeneratedKeys();
	}
}