package org.codefilarete.stalactite.sql.order;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.stalactite.query.builder.QuotingDMLNameProvider;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder.InsertStatement;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class InsertCommandBuilderTest {
	
	@Test
	<T extends Table<T>> void toSQL() {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, String> columnB = totoTable.addColumn("b", String.class);
		Insert<T> insert = new Insert<>(totoTable)
				.set(columnB, "tata");
		
		InsertCommandBuilder<T> testInstance = new InsertCommandBuilder<>(insert, new DefaultDialect());
		
		assertThat(testInstance.toSQL()).isEqualTo("insert into Toto(b) values (?)");
	}
	
	@Test
	<T extends Table<T>> void toSQL_keywordsAreEscaped() {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, String> columnB = totoTable.addColumn("b", String.class);
		Insert<T> insert = new Insert<>(totoTable)
				.set(columnB, "tata");
		
		DefaultDialect dialect = new DefaultDialect() {
			@Override
			protected DMLNameProviderFactory newDMLNameProviderFactory() {
				return QuotingDMLNameProvider::new;
			}
		};
		InsertCommandBuilder<T> testInstance = new InsertCommandBuilder<>(insert, dialect);
		
		assertThat(testInstance.toSQL()).isEqualTo("insert into `Toto`(`b`) values (?)");
	}
	
	@Test
	<T extends Table<T>> void toStatement() throws SQLException {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, String> columnB = totoTable.addColumn("b", String.class);
		Column<T, String> columnC = totoTable.addColumn("c", String.class);
		Insert<T> insert = new Insert<>(totoTable)
				.set(columnA, 42L)
				.set(columnB, "tata")
				.set(columnC, "toto");
		
		DefaultDialect dialect = new DefaultDialect();
		InsertCommandBuilder<T> testInstance = new InsertCommandBuilder<>(insert, dialect);
		
		InsertStatement<T> result = testInstance.toStatement();
		assertThat(result.getSQL()).isEqualTo("insert into Toto(a, b, c) values (?, ?, ?)");
		
		assertThat(result.getValues()).isEqualTo(Maps.forHashMap(Column.class, Object.class).add(columnA, 42L).add(columnB, "tata").add(columnC, "toto"));
		assertThat(result.getParameterBinder(columnA)).isEqualTo(DefaultParameterBinders.LONG_BINDER);
		assertThat(result.getParameterBinder(columnB)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		assertThat(result.getParameterBinder(columnC)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		
		PreparedStatement mock = mock(PreparedStatement.class);
		result.applyValues(mock);
		verify(mock).setLong(1, 42L);
		verify(mock).setString(2, "tata");
		verify(mock).setString(3, "toto");
		
		// ensuring that column type override in registry is taken into account
		dialect.getColumnBinderRegistry().register(columnA, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result = testInstance.toStatement();
		assertThat(result.getParameterBinder(columnA)).isEqualTo(DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result.setValue(columnA, -42l);
		result.setValue(columnC, "toto");
		result.applyValues(mock);
		verify(mock).setLong(1, -42l);

	}
}