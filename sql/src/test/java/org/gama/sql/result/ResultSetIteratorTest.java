package org.gama.sql.result;

import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ResultSetIteratorTest {
	
	private Connection connection;
	private PreparedStatement selectStmnt;
	
	@BeforeMethod
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
		
		ResultSetIterator<ResultSet> resultSetIterator = new ResultSetIterator<ResultSet>(selectStmntRs) {
			@Override
			public ResultSet convert(ResultSet rs) {
				return rs;
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
		
		ResultSetIterator<ResultSet> resultSetIterator = new ResultSetIterator<ResultSet>(selectStmntRs) {
			@Override
			public ResultSet convert(ResultSet rs) {
				return rs;
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