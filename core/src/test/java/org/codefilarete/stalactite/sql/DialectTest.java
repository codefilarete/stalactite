package org.codefilarete.stalactite.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;

import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.query.builder.OperatorSQLBuilderFactory;
import org.codefilarete.stalactite.query.builder.SQLAppender;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.model.Operators.like;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DialectTest {
	
	@Test
	void operatorPrintIsOverridden_itIsTakenIntoAccountInDeleteAndQuery() throws SQLException {
		Dialect testInstance = new Dialect();
		QuerySQLBuilderFactoryBuilder querySQLBuilderFactoryBuilder = new QuerySQLBuilderFactoryBuilder(
				testInstance.getColumnBinderRegistry(),
				testInstance.getSqlTypeRegistry().getJavaTypeToSqlTypeMapping());
		querySQLBuilderFactoryBuilder.withOperatorSQLBuilderFactory(
				new OperatorSQLBuilderFactory() {
					@Override
					public OperatorSQLBuilder operatorSQLBuilder() {
						return new OperatorSQLBuilder() {
							
							/** Overridden to write "like" in upper case, just to check and demonstrate how to branch some behavior on operator print */
							@Override
							public void cat(Column column, ConditionalOperator operator, SQLAppender sql) {
								if (operator instanceof Like) {
									sql.cat("LIKE ").catValue(((Like) operator).getValue().toString());
								} else {
									super.cat(column, operator, sql);
								}
							}
						};
					}
				});
		testInstance.setQuerySQLBuilderFactory(querySQLBuilderFactoryBuilder.build());
		
		Connection connectionMock = Mockito.mock(Connection.class);
		ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(preparedStatementMock.executeQuery()).thenReturn(new InMemoryResultSet(Collections.emptySet()));
		when(connectionMock.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatementMock);
		PersistenceContext persistenceContext = new PersistenceContext(() -> connectionMock, testInstance);
		Table dummyTable = new Table("dummyTable");
		Column dummyColumn = dummyTable.addColumn("dummyColumn", String.class);
		
		// Checking that operator override is taken into Query rendering
		persistenceContext.newQuery(QueryEase.select(dummyColumn).from(dummyTable).where(dummyColumn, like("x")), String.class)
				.execute(Accumulators.getFirst());
		assertThat(sqlCaptor.getValue()).isEqualTo("select dummyTable.dummyColumn from dummyTable where dummyTable.dummyColumn LIKE 'x'");
		
		// Checking that operator override is taken into Delete rendering
		persistenceContext.delete(dummyTable).where(dummyColumn, like("x")).execute();
		assertThat(sqlCaptor.getValue()).isEqualTo("delete from dummyTable where dummyColumn LIKE ?");
	}
	
	@Test
	void userDefinedOperatorCanBeTakenIntoAccountByOperatorSQLBuilderOverride() throws SQLException {
		Dialect testInstance = new Dialect();
		
		class MyOperator extends UnitaryOperator<String> {
			
			public MyOperator(String value) {
				super(value);
			}
		}
		
		QuerySQLBuilderFactoryBuilder querySQLBuilderFactoryBuilder = new QuerySQLBuilderFactoryBuilder(
				testInstance.getColumnBinderRegistry(),
				testInstance.getSqlTypeRegistry().getJavaTypeToSqlTypeMapping());
		querySQLBuilderFactoryBuilder.withOperatorSQLBuilderFactory(
				new OperatorSQLBuilderFactory() {
					@Override
					public OperatorSQLBuilder operatorSQLBuilder() {
						return new OperatorSQLBuilder() {
							
							/** Overridden to write "like" in upper case, just to check and demonstrate how to branch some behavior on operator print */
							@Override
							public void cat(Column column, ConditionalOperator operator, SQLAppender sql) {
								if (operator instanceof MyOperator) {
									sql.cat("myOperator ").catValue(((MyOperator) operator).getValue());
								} else {
									super.cat(column, operator, sql);
								}
							}
						};
					}
				});
		testInstance.setQuerySQLBuilderFactory(querySQLBuilderFactoryBuilder.build());
		
		Connection connectionMock = Mockito.mock(Connection.class);
		ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(preparedStatementMock.executeQuery()).thenReturn(new InMemoryResultSet(Collections.emptySet()));
		when(connectionMock.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatementMock);
		PersistenceContext persistenceContext = new PersistenceContext(() -> connectionMock, testInstance);
		Table dummyTable = new Table("dummyTable");
		Column dummyColumn = dummyTable.addColumn("dummyColumn", String.class);
		
		// Checking that operator override is taken into Query rendering
		persistenceContext.newQuery(QueryEase.select(dummyColumn).from(dummyTable).where(dummyColumn, new MyOperator("42")), String.class)
				.execute(Accumulators.getFirst());
		assertThat(sqlCaptor.getValue()).isEqualTo("select dummyTable.dummyColumn from dummyTable where dummyTable.dummyColumn myOperator '42'");
		
		// Checking that operator override is taken into Delete rendering
		persistenceContext.delete(dummyTable).where(dummyColumn, new MyOperator("42")).execute();
		assertThat(sqlCaptor.getValue()).isEqualTo("delete from dummyTable where dummyColumn myOperator ?");
	}
}