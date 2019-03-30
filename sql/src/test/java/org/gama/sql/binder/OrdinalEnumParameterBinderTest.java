package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Month;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.sql.result.InMemoryResultSet;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class OrdinalEnumParameterBinderTest {
	
	@Test
	void get() throws SQLException {
		ParameterBinder<Month> registeredBinder = new OrdinalEnumParameterBinder<>(Month.class);
		ResultSet preparedStatementMock = new InMemoryResultSet(Arrays.asList(Maps.asMap("month", Month.JANUARY.ordinal())));
		preparedStatementMock.next();
		Month readMonth = registeredBinder.get(preparedStatementMock, "month");
		assertEquals(Month.JANUARY, readMonth);
	}
	
	@Test
	void set() throws SQLException {
		ParameterBinder<Month> registeredBinder = new OrdinalEnumParameterBinder<>(Month.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		registeredBinder.set(preparedStatementMock, 1, Month.JANUARY);
		
		ArgumentCaptor<Integer> valueCaptor = ArgumentCaptor.forClass(Integer.class);
		verify(preparedStatementMock).setInt(anyInt(), valueCaptor.capture());
		assertEquals(Month.JANUARY.ordinal(), valueCaptor.getValue().intValue());
	}
}