package org.codefilarete.stalactite.sql.order;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.stalactite.query.builder.QuotingDMLNameProvider;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.UpdateCommandBuilder.UpdateStatement;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.codefilarete.stalactite.query.model.Operators.eq;
import static org.codefilarete.stalactite.query.model.Operators.in;
import static org.codefilarete.stalactite.query.model.QueryEase.where;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class UpdateCommandBuilderTest {
	
	@Test
	<T extends Table<T>> void toSQL_singleTable() {
		T totoTable = (T) new Table("Toto");
		Column<T, String> columnA = totoTable.addColumn("a", String.class);
		Column<T, String> columnB = totoTable.addColumn("b", String.class);
		
		Update<T> update = new Update<>(totoTable, where(columnA, eq(44)).or(columnA, eq(columnB)));
		Dialect dialect = new DefaultDialect();
		UpdateCommandBuilder<T> testInstance = new UpdateCommandBuilder<>(update, dialect);
		// this is wrong SQL, but it shows how code behaves without giving column to update
		assertThat(testInstance.toSQL()).isEqualTo("update Toto set  where a = 44 or a = b");
		
		update = new Update<>(totoTable)
				.set(columnA, columnB);
		testInstance = new UpdateCommandBuilder<>(update, dialect);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto set a = b");
		
		
		update = new Update<>(totoTable)
				.set(columnA, "tata");
		testInstance = new UpdateCommandBuilder<>(update, dialect);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto set a = 'tata'");
	}
	
	
	@Test
	<T extends Table<T>> void toSQL_keywordsAreEscaped() {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, String> columnB = totoTable.addColumn("b", String.class);
		Update<T> update = new Update<>(totoTable)
				.set(columnB, "tata");
		
		DefaultDialect dialect = new DefaultDialect() {
			@Override
			protected DMLNameProviderFactory newDMLNameProviderFactory() {
				return QuotingDMLNameProvider::new;
			}
		};
		UpdateCommandBuilder<T> testInstance = new UpdateCommandBuilder<>(update, dialect);
		
		assertThat(testInstance.toSQL()).isEqualTo("update `Toto` set `b` = 'tata'");
	}
	
	@Test
	<T1 extends Table<T1>, T2 extends Table<T2>> void toSQL_multiTable() {
		T1 totoTable = (T1) new Table("Toto");
		Column<T1, String> columnA = totoTable.addColumn("a", String.class);
		Column<T1, String> columnB = totoTable.addColumn("b", String.class);
		Column<T1, String> columnC = totoTable.addColumn("c", String.class);
		T2 tataTable = (T2) new Table("Tata");
		Column<T2, Long> columnX = tataTable.addColumn("x", Long.class);
		Column<T2, String> columnY = tataTable.addColumn("y", String.class);
		Column<T2, String> columnZ = tataTable.addColumn("z", String.class);
		
		
		Update<T1> update1 = new Update<>(totoTable, where(columnA, eq(columnX)).or(columnA, eq(columnY)));
		Dialect dialect = new DefaultDialect();
		UpdateCommandBuilder<T1> testInstance = new UpdateCommandBuilder<>(update1, dialect);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto, Tata set  where Toto.a = Tata.x or Toto.a = Tata.y");
		
		Update<T1> update2 = new Update<>(totoTable, where(columnA, eq(columnX)).or(columnA, eq(columnY)))
				.set(columnC, columnZ);
		testInstance = new UpdateCommandBuilder<>(update2, dialect);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto, Tata set Toto.c = Tata.z where Toto.a = Tata.x or Toto.a = Tata.y");
	}
	
	@Test
	<T extends Table<T>> void toStatement() throws SQLException {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, Long> columnB = totoTable.addColumn("b", Long.class);
		Column<T, String> columnC = totoTable.addColumn("c", String.class);
		Column<T, Integer> columnD = totoTable.addColumn("d", Integer.class);
		
		Update<T> update = new Update<>(totoTable, where(columnA, in(42L, 43L)).or(columnA, eq(columnB)))
				.set(columnB, columnA)
				.set(columnC, "tata")
				.set(columnD, 666);
		Dialect dialect = new DefaultDialect();
		UpdateCommandBuilder<T> testInstance = new UpdateCommandBuilder<T>(update, dialect);
		
		UpdateStatement<T> result = testInstance.toStatement();
		assertThat(result.getSQL()).isEqualTo("update Toto set b = a, c = ?, d = ? where a in (?, ?) or a = b");
				
		assertThat(result.getValues()).isEqualTo(Maps.asMap(1, (Object) "tata").add(2, 666).add(3, 42L).add(4, 43L));
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		assertThat(result.getParameterBinder(2)).isEqualTo(DefaultParameterBinders.INTEGER_BINDER);
		assertThat(result.getParameterBinder(3)).isEqualTo(DefaultParameterBinders.LONG_BINDER);
		assertThat(result.getParameterBinder(4)).isEqualTo(DefaultParameterBinders.LONG_BINDER);
		
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		result.applyValues(preparedStatementMock);
		verify(preparedStatementMock).setString(1, "tata");
		verify(preparedStatementMock).setInt(2, 666);
		verify(preparedStatementMock).setLong(3, 42L);
		verify(preparedStatementMock).setLong(4, 43L);
		
		// test with post modification of pre-set column
		result.setValue(columnC, "tutu");
		result.applyValues(preparedStatementMock);
		verify(preparedStatementMock).setString(1, "tutu");
		verify(preparedStatementMock, times(2)).setInt(2, 666);
		verify(preparedStatementMock, times(2)).setLong(3, 42L);
		verify(preparedStatementMock, times(2)).setLong(4, 43L);
	}
	
	@Test
	<T extends Table<T>> void toStatement_withPlaceHolderInCriteria() throws SQLException {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, Long> columnB = totoTable.addColumn("b", Long.class);
		Column<T, String> columnC = totoTable.addColumn("c", String.class);
		Column<T, Integer> columnD = totoTable.addColumn("d", Integer.class);
		
		Update<T> update = new Update<>(totoTable, where(columnC, Operators.likeArgNamed("criterion", String.class)))
				.set(columnB, columnA)
				.set(columnC, "tata")
				.set(columnD, 666)
				.set("criterion", "t");
		Dialect dialect = new DefaultDialect();
		UpdateCommandBuilder<T> testInstance = new UpdateCommandBuilder<T>(update, dialect);
		
		UpdateStatement<T> result = testInstance.toStatement();
		assertThat(result.getSQL()).isEqualTo("update Toto set b = a, c = ?, d = ? where c like ?");
		
		assertThat(result.getValues()).containsAllEntriesOf(Maps.asMap(1, (Object) "tata").add(2, 666));
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		assertThat(result.getParameterBinder(2)).isEqualTo(DefaultParameterBinders.INTEGER_BINDER);
		assertThat(result.getParameterBinder(3)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		result.applyValues(preparedStatementMock);
		verify(preparedStatementMock).setString(1, "tata");
		verify(preparedStatementMock).setInt(2, 666);
		verify(preparedStatementMock).setString(3, "t");
		
		// test with modification of already-set columns
		result.setValue(columnC, "tutu");
		result.applyValues(preparedStatementMock);
		verify(preparedStatementMock, times(1)).setString(1, "tata");
		verify(preparedStatementMock, times(1)).setString(1, "tutu");
		verify(preparedStatementMock, times(2)).setInt(2, 666);
		verify(preparedStatementMock, times(2)).setString(3, "t");
	}
	
	@Test
	<T extends Table<T>> void toStatement_placeholderValueIsNotSet_throwsException() {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, Long> columnB = totoTable.addColumn("b", Long.class);
		Column<T, String> columnC = totoTable.addColumn("c", String.class);
		Column<T, Integer> columnD = totoTable.addColumn("d", Integer.class);
		
		Update<T> update = new Update<>(totoTable, where(columnC, Operators.likeArgNamed("criterion", String.class)))
				.set(columnB, columnA)
				.set(columnC, "tata")
				.set(columnD, 666);
		Dialect dialect = new DefaultDialect();
		UpdateCommandBuilder<T> testInstance = new UpdateCommandBuilder<T>(update, dialect);
		
		UpdateStatement<T> result = testInstance.toStatement();
		assertThat(result.getSQL()).isEqualTo("update Toto set b = a, c = ?, d = ? where c like ?");
		
		assertThat(result.getValues()).containsAllEntriesOf(Maps.asMap(1, (Object) "tata").add(2, 666));
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		assertThat(result.getParameterBinder(2)).isEqualTo(DefaultParameterBinders.INTEGER_BINDER);
		assertThat(result.getParameterBinder(3)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		assertThatCode(() -> result.applyValues(preparedStatementMock))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("Statement expect values for placeholders: criterion");
	}
	
	@Test
	<T extends Table<T>> void toStatement_givenPlaceholderNameDoesntMatchAnyExistingOne_throwsException() {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, Long> columnB = totoTable.addColumn("b", Long.class);
		Column<T, String> columnC = totoTable.addColumn("c", String.class);
		Column<T, Integer> columnD = totoTable.addColumn("d", Integer.class);
		
		Update<T> update = new Update<>(totoTable, where(columnC, Operators.likeArgNamed("criterion", String.class)))
				.set(columnB, columnA)
				.set(columnC, "tata")
				.set(columnD, 666)
				.set("xxx", "t");
		UpdateCommandBuilder<T> testInstance = new UpdateCommandBuilder<T>(update, new DefaultDialect());
		
		assertThatCode(testInstance::toStatement)
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("No placeholder named \"xxx\" found in statement, available are [criterion]");
	}
	
	@Test
	<T extends Table<T>> void toStatement_columnParameterBinderIsOverridden_columnParameterBinderIsUsed() throws SQLException {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, Long> columnB = totoTable.addColumn("b", Long.class);
		Column<T, String> columnC = totoTable.addColumn("c", String.class);
		Column<T, Integer> columnD = totoTable.addColumn("d", Integer.class);
		
		Update<T> update = new Update<>(totoTable)
				.set(columnB, columnA)
				.set(columnC, "tata")
				.set(columnD, 666);
		Dialect dialect = new DefaultDialect();
		UpdateCommandBuilder<T> testInstance = new UpdateCommandBuilder<T>(update, dialect);
		
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		// ensuring that column type override in registry is taken into account
		dialect.getColumnBinderRegistry().register(columnD, DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
		UpdateStatement<T> result = testInstance.toStatement();
		assertThat(result.getParameterBinder(2)).isEqualTo(DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
		result.setValue(columnD, -666);
		result.applyValues(preparedStatementMock);
		verify(preparedStatementMock).setInt(2, -666);
	}
}