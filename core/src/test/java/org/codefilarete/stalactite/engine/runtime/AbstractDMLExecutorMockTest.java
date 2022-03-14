package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperation.RowCountListener;
import org.codefilarete.stalactite.test.PairSetList;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.function.ThrowingBiFunction;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractDMLExecutorMockTest extends DMLExecutorTest {
	
	protected JdbcMock jdbcMock;
	protected WriteOperationFactory noRowCountCheckWriteOperationFactory;
	
	@BeforeEach
	void init() {
		this.jdbcMock = new JdbcMock();
		
		noRowCountCheckWriteOperationFactory = new WriteOperationFactory() {
			
			@Override
			protected <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																		   ConnectionProvider connectionProvider,
																		   ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																		   RowCountListener rowCountListener) {
				// we d'ont care about row count checker in thoses tests, so every statement will be created without it
				return new WriteOperation<ParamType>(sqlGenerator, connectionProvider, NOOP_COUNT_CHECKER) {
					@Override
					protected void prepareStatement(Connection connection) throws SQLException {
						this.preparedStatement = statementProvider.apply(connection, getSQL());
					}
				};
			}
		};
	}
	
	protected static class JdbcMock {
		
		protected PreparedStatement preparedStatement;
		protected ArgumentCaptor<Integer> valueCaptor;
		protected ArgumentCaptor<Integer> indexCaptor;
		protected ArgumentCaptor<String> sqlCaptor;
		protected ConnectionProvider transactionManager;
		protected Connection connection;
		
		protected JdbcMock() {
			try {
				preparedStatement = mock(PreparedStatement.class);
				when(preparedStatement.executeLargeBatch()).thenReturn(new long[]{1});
				
				connection = mock(Connection.class);
				// PreparedStatement.getConnection() must gives that instance of connection because of SQLOperation that checks
				// weither or not it should prepare statement
				when(preparedStatement.getConnection()).thenReturn(connection);
				sqlCaptor = ArgumentCaptor.forClass(String.class);
				when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatement);
				when(connection.prepareStatement(sqlCaptor.capture(), anyInt())).thenReturn(preparedStatement);
				
				valueCaptor = ArgumentCaptor.forClass(Integer.class);
				indexCaptor = ArgumentCaptor.forClass(Integer.class);
				
				DataSource dataSource = mock(DataSource.class);
				when(dataSource.getConnection()).thenReturn(connection);
				transactionManager = new CurrentThreadConnectionProvider(dataSource);
			} catch (SQLException e) {
				// this should not happen since every thing is mocked, left as safeguard, and avoid catching
				// exception by caller which don't know what to do with the exception else same thing as here
				throw Exceptions.asRuntimeException(e);
			}
		}
	}
	
	public static void assertCapturedPairsEqual(JdbcMock jdbcMock, PairSetList<Integer, Integer> expectedPairs) {
		List<Duo<Integer, Integer>> obtainedPairs = PairSetList.toPairs(jdbcMock.indexCaptor.getAllValues(), jdbcMock.valueCaptor.getAllValues());
		List<Set<Duo<Integer, Integer>>> obtained = new ArrayList<>();
		int startIndex = 0;
		// rearranging obtainedPairs into some packets, as those of expectedPairs
		for (Set<Duo<Integer, Integer>> expectedPair : expectedPairs.asList()) {
			obtained.add(new HashSet<>(obtainedPairs.subList(startIndex, startIndex += expectedPair.size())));
		}
		assertThat(obtained).isEqualTo(expectedPairs.asList());
	}
}
