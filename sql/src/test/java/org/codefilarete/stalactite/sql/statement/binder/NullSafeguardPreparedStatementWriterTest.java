package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.tool.trace.MutableBoolean;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
class NullSafeguardPreparedStatementWriterTest {
	
	@Test
	void cc() {
		System.out.println(DefaultPreparedStatementWriters.SET_LONG_WRITER.getType());
	}
	
	@Test
	public void testSet_nonNullValueIsPassedAsArgument_delegateIsInvoked() throws SQLException {
		MutableBoolean isDelegateInvoked = new MutableBoolean(false);
		NullSafeguardPreparedStatementWriter<Object> testInstance =
				new NullSafeguardPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> isDelegateInvoked.setTrue());
		testInstance.set(mock(PreparedStatement.class), 42, 666);
		assertThat(isDelegateInvoked.getValue()).isTrue();
	}
	
	@Test
	public void testSet_nullIsPassedAsArgument_nonNPEIsThrown() {
		MutableBoolean isDelegateInvoked = new MutableBoolean(false);
		NullSafeguardPreparedStatementWriter<Object> testInstance =
				new NullSafeguardPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> {
			isDelegateInvoked.setTrue();
		});
		assertThatThrownBy(() -> testInstance.set(mock(PreparedStatement.class), 42, null))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Trying to pass null as primitive value");
	}
	
}