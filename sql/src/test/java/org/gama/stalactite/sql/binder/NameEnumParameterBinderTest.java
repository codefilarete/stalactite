package org.gama.stalactite.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Month;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.sql.result.InMemoryResultSet;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class NameEnumParameterBinderTest {
	
	@Test
	void get() throws SQLException {
		ParameterBinder<Month> registeredBinder = new NameEnumParameterBinder<>(Month.class);
		ResultSet preparedStatementMock = new InMemoryResultSet(Arrays.asList(Maps.asMap("month", "JANUARY")));
		preparedStatementMock.next();
		Month readMonth = registeredBinder.get(preparedStatementMock, "month");
		assertThat(readMonth).isEqualTo(Month.JANUARY);
	}
	
	@Test
	void set() throws SQLException {
		ParameterBinder<Month> registeredBinder = new NameEnumParameterBinder<>(Month.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		registeredBinder.set(preparedStatementMock, 1, Month.JANUARY);
		
		ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
		verify(preparedStatementMock).setString(anyInt(), valueCaptor.capture());
		assertThat(valueCaptor.getValue()).isEqualTo(Month.JANUARY.name());
	}
}