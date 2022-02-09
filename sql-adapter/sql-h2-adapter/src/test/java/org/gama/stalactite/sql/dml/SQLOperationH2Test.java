package org.codefilarete.stalactite.sql.dml;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.function.Predicate;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Engine;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SQLOperationH2Test extends SQLOperationITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new ConcurrentH2InMemoryDataSource();
    }
	
    @Override
	protected void lockTable(Connection lockingConnection) {
		Session session = null;
		try {
			session = (Session) lockingConnection.unwrap(JdbcConnection.class).getSession();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		Schema schema = session.getDatabase().getSchema(session.getCurrentSchemaName());
		Table table = schema.findTableOrView(session, "TOTO");
		table.lock(session, true, true);
	}
	
	@Override
    String giveLockStatement() {
    	// H2 doesn't seem to have an SQL order to lock a table, else we must access its Table and Session objects to do it, see lockTable(..) method override
        return null;
    }

    @Override
    Predicate<Throwable> giveCancelOperationPredicate() {
        return Objects::isNull;
    }
	
	/**
	 * Dedicated H2 Datasource for concurrent access to simulaite multiple user accessing same table and lock
	 */
	public static class ConcurrentH2InMemoryDataSource extends UrlAwareDataSource implements Closeable {
		
		private final JdbcDataSource delegate;
		private Session session;
		
		public ConcurrentH2InMemoryDataSource() {
			// random URL to avoid conflict between tests
			// setting DB_CLOSE_ON_EXIT=false to avoid JdbcSQLNonTransientConnectionException
			// "The database is open in exclusive mode; can not open additional connections [90135-200]"
//			super("jdbc:h2:mem:test" + Integer.toHexString(new Random().nextInt()) + ";DB_CLOSE_ON_EXIT=false");
			// we need a LOCK_TIMEOUT larger than the wait we put in test code because default value makes triggering an org.h2.jdbc.JdbcSQLTimeoutException
			// while select acquires read lock, the exception then cancels the select statement which then makes our test irelevant 
//			super("jdbc:h2:" + Files.newTemporaryFolder() + ";AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=true;LOCK_TIMEOUT=5000");
			super("jdbc:h2:mem:test" + Integer.toHexString(new Random().nextInt()) + ";LOCK_TIMEOUT=5000");
			delegate = new JdbcDataSource();
			delegate.setUrl(getUrl());
			delegate.setUser("sa");
			delegate.setPassword("");
			setDelegate(delegate);
		}
		
		protected void lockTable() {
			Properties info = new Properties();
			info.setProperty("user", "sa");
			session = Engine.getInstance().createSession(new ConnectionInfo(getUrl(), info));
			System.out.println(session.getId()*10);
			Schema schema = session.getDatabase().getSchema(session.getCurrentSchemaName());
			Table table = schema.findTableOrView(session, "TOTO");
			table.lock(session, true, true);
		}
		
		@Override
		public void close() throws IOException {
			// nothing to do because H2DataSource doesn't need to be closed
		}
	}
}
