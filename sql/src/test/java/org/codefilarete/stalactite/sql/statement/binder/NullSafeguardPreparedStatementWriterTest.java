package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.statement.binder.NullSafeguardPreparedStatementWriter;
import org.codefilarete.tool.trace.ModifiableBoolean;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
		assertThat(isSurrogateInvoked.getValue()).isTrue();
	}
	
	@Test
	public void testSet_nullIsPassedAsArgument_nonNPEIsThrown() {
		ModifiableBoolean isSurrogateInvoked = new ModifiableBoolean(false);
		NullSafeguardPreparedStatementWriter<Object> testInstance =
				new NullSafeguardPreparedStatementWriter<>((preparedStatement, valueIndex, value) -> {
			isSurrogateInvoked.setTrue();
		});
		assertThatThrownBy(() -> testInstance.set(mock(PreparedStatement.class), 42, null))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Trying to pass null as primitive value");
	}
	
}