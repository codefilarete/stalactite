package org.gama.stalactite.sql.result;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Database integration test template for {@link ResultSetIterator}, see implementations in sql-adapter submodules
 *
 * @author Guillaume Mary
 */
abstract class ResultSetIteratorITTest {

	protected DataSource dataSource;

	@BeforeEach
	abstract void createDataSource();
	
	@Test
	void hasNext_emptyResultSet() throws SQLException {
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
	
	@Test
	void hasNext_filledResultSet() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.setAutoCommit(false);
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
	
	@Test
	void next_withoutCallToHasNext_throwsNoSuchElementException() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.setAutoCommit(false);
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
	
	@Test
	void convert() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.setAutoCommit(false);
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
	
	private void ensureTable(Connection connection) throws SQLException {
		PreparedStatement createTableStmnt = connection.prepareStatement("create table Toto(name VARCHAR(10))");
		createTableStmnt.execute();
	}
}