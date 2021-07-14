package org.gama.stalactite.sql.result;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.sql.test.DerbyInMemoryDataSource;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.sql.test.MariaDBEmbeddableDataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ResultSetIteratorTest {
	
	public static Object[][] dataSources() {
		return new Object[][] {
				{ new HSQLDBInMemoryDataSource() },
				{ new DerbyInMemoryDataSource() },
				{ new MariaDBEmbeddableDataSource(3406) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testHasNext_emptyResultSet(DataSource dataSource) throws SQLException {
		Connection connection = dataSource.getConnection();
		ensureTable(connection);
		
		PreparedStatement selectStmnt = connection.prepareStatement("select name from Toto");
		ResultSet selectStmntRs = selectStmnt.executeQuery();
		
		ResultSetIterator<String> resultSetIterator = new ResultSetIterator<String>(selectStmntRs) {
			@Override
			public String convert(ResultSet rs) throws SQLException {
				return rs.getString("name");
			}
		};
		assertThat(resultSetIterator.hasNext()).isFalse();
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testHasNext_filledResultSet(DataSource dataSource) throws SQLException {
		Connection connection = dataSource.getConnection();
		ensureTable(connection);

		PreparedStatement insertDataStmnt = connection.prepareStatement("insert into Toto(name) values ('a'), ('b'), ('c')");
		insertDataStmnt.execute();
		connection.commit();
		
		PreparedStatement selectStmnt = connection.prepareStatement("select name from Toto");
		ResultSet selectStmntRs = selectStmnt.executeQuery();
		
		ResultSetIterator<String> resultSetIterator = new ResultSetIterator<String>(selectStmntRs) {
			@Override
			public String convert(ResultSet rs) throws SQLException {
				return rs.getString("name");
			}
		};
		assertThat(resultSetIterator.hasNext()).isTrue();
		resultSetIterator.next();
		assertThat(resultSetIterator.hasNext()).isTrue();
		resultSetIterator.next();
		assertThat(resultSetIterator.hasNext()).isTrue();
		resultSetIterator.next();
		assertThat(resultSetIterator.hasNext()).isFalse();
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testNext_withoutCallToHasNext_throwsNoSuchElementException(DataSource dataSource) throws SQLException {
		Connection connection = dataSource.getConnection();
		ensureTable(connection);
		
		PreparedStatement insertDataStmnt = connection.prepareStatement("insert into Toto(name) values ('a'), ('b'), ('c')");
		insertDataStmnt.execute();
		connection.commit();
		
		PreparedStatement selectStmnt = connection.prepareStatement("select name from Toto");
		ResultSet selectStmntRs = selectStmnt.executeQuery();
		
		ResultSetIterator<String> resultSetIterator = new ResultSetIterator<String>(selectStmntRs) {
			@Override
			public String convert(ResultSet rs) throws SQLException {
				return rs.getString("name");
			}
		};
		// No call to hasNext() throws NoSuchElementException
		assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(resultSetIterator::next);
		
		// Multiple calls to next() without calling hasNext() throw NoSuchElementException
		assertThat(resultSetIterator.hasNext()).isTrue();
		resultSetIterator.next();
		assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(resultSetIterator::next);
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testConvert(DataSource dataSource) throws SQLException {
		Connection connection = dataSource.getConnection();
		ensureTable(connection);
		
		PreparedStatement insertDataStmnt = connection.prepareStatement("insert into Toto(name) values ('a'), ('b'), ('c')");
		insertDataStmnt.execute();
		connection.commit();
		
		PreparedStatement selectStmnt = connection.prepareStatement("select name from Toto");
		ResultSet selectStmntRs = selectStmnt.executeQuery();
		
		ResultSetIterator<String> resultSetIterator = new ResultSetIterator<String>(selectStmntRs) {
			@Override
			public String convert(ResultSet rs) throws SQLException {
				return rs.getString("name");
			}
		};
		assertThat(resultSetIterator.convert()).isEqualTo(Arrays.asList("a", "b", "c"));
	}
	
	public void ensureTable(Connection connection) throws SQLException {
		PreparedStatement createTableStmnt = connection.prepareStatement("create table Toto(name VARCHAR(10))");
		createTableStmnt.execute();
	}
}