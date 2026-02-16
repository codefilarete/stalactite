package org.codefilarete.stalactite.sql.h2.statement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Random;
import java.util.function.Predicate;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.stalactite.sql.statement.SQLOperationITTest;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.h2.test.H2DatabaseHelper;
import org.h2.engine.SessionLocal;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.schema.Schema;
import org.h2.table.Table;

/**
 * @author Guillaume Mary
 */
class SQLOperationH2Test extends SQLOperationITTest {
    
    @Override
	public DataSource giveDataSource() {
        return new ConcurrentH2InMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new H2DatabaseHelper();
	}
	
    @Override
	protected void lockTable(Connection lockingConnection) {
		SessionLocal session;
		try {
			session = (SessionLocal) lockingConnection.unwrap(JdbcConnection.class).getSession();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		Schema schema = session.getDatabase().getSchema(session.getCurrentSchemaName());
		Table table = schema.findTableOrView(session, "TOTO");
		table.lock(session, Table.EXCLUSIVE_LOCK);
	}
	
	@Override
	protected String giveLockStatement() {
    	// H2 doesn't seem to have an SQL order to lock a table, else we must access its Table and Session objects to do it, see lockTable(..) method override
        return null;
    }

    @Override
	protected Predicate<Throwable> giveCancelOperationPredicate() {
        return Objects::isNull;
    }
	
	/**
	 * Dedicated H2 Datasource for concurrent access to simulate multiple user accessing same table and lock
	 */
	public static class ConcurrentH2InMemoryDataSource extends UrlAwareDataSource {
		
		private final JdbcDataSource delegate;
		
		public ConcurrentH2InMemoryDataSource() {
			// random URL to avoid conflict between tests
			// we need a LOCK_TIMEOUT larger than the wait we put in test code because default value makes triggering an org.h2.jdbc.JdbcSQLTimeoutException
			// while select acquires read lock, the exception then cancels the select statement which then makes our test irelevant 
			super("jdbc:h2:mem:test" + Integer.toHexString(new Random().nextInt()) + ";LOCK_TIMEOUT=5000");
			delegate = new JdbcDataSource();
			delegate.setUrl(getUrl());
			delegate.setUser("sa");
			delegate.setPassword("");
			setDelegate(delegate);
		}
	}
}
