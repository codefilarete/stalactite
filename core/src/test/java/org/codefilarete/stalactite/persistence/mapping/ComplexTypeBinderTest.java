package org.codefilarete.stalactite.persistence.mapping;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.codefilarete.tool.Strings;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.stalactite.sql.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class ComplexTypeBinderTest {
	
	@Test
	void get() throws SQLException {
		// Creating our test instance : will persist List<String> as a String (stupid case)
		ComplexTypeBinder<List<String>> testInstance = new ComplexTypeBinder<>(DefaultParameterBinders.STRING_BINDER,
				s -> Arrays.asList(Strings.cutTail(Strings.cutHead(s, 1), 1).toString().split(",\\s")),
				Object::toString);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(Maps.forHashMap(String.class, Object.class)
			.add("listColumn", Arrays.asList("a", "b").toString())));
		resultSet.next();
		List<String> readValue = testInstance.get(resultSet, "listColumn");
		assertThat(readValue).isEqualTo(Arrays.asList("a", "b"));
	}
	
	@Test
	void set() throws SQLException {
		// Creating our test instance : will persist List<String> as a String (stupid case)
		ComplexTypeBinder<List<String>> testInstance = new ComplexTypeBinder<>(DefaultParameterBinders.STRING_BINDER,
				s -> Arrays.asList(Strings.cutTail(Strings.cutHead(s, 1), 1).toString().split(",\\s")),
				Object::toString);
		
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		testInstance.set(preparedStatementMock, 42, Arrays.asList("a", "b"));
		verify(preparedStatementMock).setString(42, Arrays.asList("a", "b").toString());
	}
}