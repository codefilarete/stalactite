package org.gama.stalactite.sql.binder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.lang.trace.ModifiableBoolean;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
class NullSafeguardPreparedStatementWriterTest {
	
	@Test
	public void testSet_nonNullValueIsPassedAsArgument_surrogateIsInvoked() throws SQLException {
		ModifiableBoolean isSurrogateInvoked = new ModifiableBoolean(false);
		NullSafeguardPreparedStatementWriter<Object> testInstance =
				new NullSafeguardPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> isSurrogateInvoked.setTrue());
		testInstance.set(mock(PreparedStatement.class), 42, 666);
		assertTrue(isSurrogateInvoked.getValue());
	}
	
	@Test
	public void testSet_nullIsPassedAsArgument_nonNPEIsThrown() {
		ModifiableBoolean isSurrogateInvoked = new ModifiableBoolean(false);
		NullSafeguardPreparedStatementWriter<Object> testInstance =
				new NullSafeguardPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> {
			isSurrogateInvoked.setTrue();
		});
		IllegalArgumentException thrownException = assertThrows(IllegalArgumentException.class,
				() -> testInstance.set(mock(PreparedStatement.class), 42, null));
		assertEquals("Trying to pass null as primitive value", thrownException.getMessage());
	}
	
}