package org.gama.sql.result;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import org.gama.lang.collection.Arrays;
import org.gama.sql.test.DerbyInMemoryDataSource;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.sql.test.MariaDBEmbeddableDataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
		assertFalse(resultSetIterator.hasNext());
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
		assertTrue(resultSetIterator.hasNext());
		resultSetIterator.next();
		assertTrue(resultSetIterator.hasNext());
		resultSetIterator.next();
		assertTrue(resultSetIterator.hasNext());
		resultSetIterator.next();
		assertFalse(resultSetIterator.hasNext());
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testNext_withoutCallToHasNext_throwsNoSucheElementException(DataSource dataSource) throws SQLException {
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
		assertThrows(NoSuchElementException.class, resultSetIterator::next);
		
		// Multiple calls to next() without calling hasNext() throw NoSuchElementException
		assertTrue(resultSetIterator.hasNext());
		resultSetIterator.next();
		assertThrows(NoSuchElementException.class, resultSetIterator::next);
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
		assertEquals(Arrays.asList("a", "b", "c"), resultSetIterator.convert());
	}
	
	public void ensureTable(Connection connection) throws SQLException {
		PreparedStatement createTableStmnt = connection.prepareStatement("create table Toto(name VARCHAR(10))");
		createTableStmnt.execute();
	}
}