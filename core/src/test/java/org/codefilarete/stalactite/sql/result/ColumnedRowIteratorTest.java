package org.codefilarete.stalactite.sql.result;

import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.query.api.Selectable.selectableString;
import static org.codefilarete.tool.collection.Arrays.asList;

class ColumnedRowIteratorTest {

	@Test
	void convert_columnReaderIsNotCompatibleWithColumnContent_throwsException() {
		Selectable<String> selectable = selectableString("toto");
		ColumnedRowIterator testInstance = new ColumnedRowIterator(
				Maps.asMap(selectable, DefaultResultSetReaders.INTEGER_READER),
				Maps.asMap(selectable, "toto"));
		assertThatThrownBy(() -> {
			InMemoryResultSet rs = new InMemoryResultSet(asList(Maps.asMap("toto", "string value")));
			rs.next();
			testInstance.convert(rs);
		})
				.isInstanceOf(SQLStatement.BindingException.class)
				.hasMessage("Error while reading column 'toto' : trying to read 'string value' as java.lang.Integer but was java.lang.String")
				.hasCause(new ClassCastException("java.lang.String cannot be cast to java.lang.Integer"));
	}
}
