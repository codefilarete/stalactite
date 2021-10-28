package org.gama.stalactite.sql.result;

import org.gama.lang.collection.Maps;
import org.gama.stalactite.sql.binder.DefaultResultSetReaders;
import org.gama.stalactite.sql.dml.SQLStatement.BindingException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.gama.lang.collection.Arrays.asList;

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