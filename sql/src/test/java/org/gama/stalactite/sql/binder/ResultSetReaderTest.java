package org.codefilarete.stalactite.sql.binder;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.codefilarete.tool.function.Hanger.Holder;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * @author Guillaume Mary
 */
class ResultSetReaderTest {
	
	@Test
	void get_exceptionHandling() throws SQLException {
		ResultSetReader<Integer> resultSetReader = (resultSet, columnName) -> (Integer) new Holder("A").get();
		ResultSet resultSetMock = Mockito.mock(ResultSet.class);
		Mockito.when(resultSetMock.getObject("XX")).thenReturn("my too long String value");
		Assertions.assertThatThrownBy(() -> resultSetReader.get(resultSetMock, "XX"))
				.hasMessage("Error while reading column 'XX' : trying to read 'my too long Str...' as java.lang.Integer but was java.lang.String");
	}
}