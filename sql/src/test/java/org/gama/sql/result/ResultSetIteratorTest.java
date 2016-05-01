package org.gama.sql.result;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.lang.exception.Exceptions;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResultSetIteratorTest {
	
	private Connection connection;
	private PreparedStatement selectStmnt;
	
	@Before
	public void setUp() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		connection = hsqldbInMemoryDataSource.getConnection();
		PreparedStatement createTableStmnt = connection.prepareStatement("create table Toto(id IDENTITY , name VARCHAR(10));");
		createTableStmnt.execute();
		selectStmnt = connection.prepareStatement("select name from Toto");
	}
	
	@Test
	public void testHasNext_emptyResultSet() throws Exception {
		ResultSet selectStmntRs = selectStmnt.executeQuery();
		
		ResultSetIterator<String> resultSetIterator = new ResultSetIterator<String>(selectStmntRs) {
			@Override
			public String convert(ResultSet rs) {
				try {
					return rs.getString(1);
				} catch (SQLException e) {
					throw Exceptions.asRuntimeException(e);
				}
			}
		};
		assertFalse(resultSetIterator.hasNext());
	}
	
	@Test
	public void testHasNext_filledResultSet() throws Exception {
		PreparedStatement insertDataStmnt = connection.prepareStatement("insert into Toto(name) values ('a'), ('b'), ('c')");
		insertDataStmnt.execute();
		connection.commit();
		
		ResultSet selectStmntRs = selectStmnt.executeQuery();
		
		ResultSetIterator<String> resultSetIterator = new ResultSetIterator<String>(selectStmntRs) {
			@Override
			public String convert(ResultSet rs) {
				try {
					return rs.getString(1);
				} catch (SQLException e) {
					throw Exceptions.asRuntimeException(e);
				}
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
}