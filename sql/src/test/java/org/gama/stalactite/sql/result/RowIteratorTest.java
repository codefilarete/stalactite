package org.codefilarete.stalactite.sql.result;

import org.codefilarete.tool.collection.Maps;
import org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders;
import org.codefilarete.stalactite.sql.dml.SQLStatement.BindingException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.tool.collection.Arrays.asList;

/**
 * @author Guillaume Mary
 */
class RowIteratorTest {
	
	@Test
	void convert_columnReaderIsNotCompatibleWithColumnContent_throwsException() {
		RowIterator testInstance = new RowIterator(null,
				Maps.asMap("toto", DefaultResultSetReaders.INTEGER_READER));
		assertThatThrownBy(() -> {
			InMemoryResultSet rs = new InMemoryResultSet(asList(Maps.asMap("toto", "string value")));
			rs.next();
			testInstance.convert(rs);
		})
				.isInstanceOf(BindingException.class)
				.hasMessage("Error while reading column 'toto' : trying to read 'string value' as java.lang.Integer but was java.lang.String")
				.hasCause(new ClassCastException("java.lang.String cannot be cast to java.lang.Integer"));
	}
}