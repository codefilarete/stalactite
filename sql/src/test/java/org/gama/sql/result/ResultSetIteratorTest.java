package org.gama.sql.result;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.sql.test.DerbyInMemoryDataSource;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.sql.test.MariaDBEmbeddableDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(DataProviderRunner.class)
public class ResultSetIteratorTest {
	
	@DataProvider
	public static Object[][] dataSources() {
		return new Object[][] {
				{ new HSQLDBInMemoryDataSource() },
				{ new DerbyInMemoryDataSource() },
				{ new MariaDBEmbeddableDataSource(3406) },
		};
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testHasNext_emptyResultSet(DataSource dataSource) throws Exception {
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
	
	@Test
	@UseDataProvider("dataSources")
	public void testHasNext_filledResultSet(DataSource dataSource) throws Exception {
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
	
	public void ensureTable(Connection connection) throws SQLException {
		PreparedStatement createTableStmnt = connection.prepareStatement("create table Toto(name VARCHAR(10))");
		createTableStmnt.execute();
	}
}