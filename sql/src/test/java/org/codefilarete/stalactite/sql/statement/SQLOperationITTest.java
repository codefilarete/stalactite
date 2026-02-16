package org.codefilarete.stalactite.sql.statement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.test.DatabaseIntegrationTest;
import org.codefilarete.tool.Duo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Database integration test template for {@link SQLOperation}, see implementations in sql-adapter submodules
 * 
 * @author Guillaume Mary
 */
public abstract class SQLOperationITTest extends DatabaseIntegrationTest {
	
	protected BiFunction<PreparedSQL, ConnectionProvider, ReadOperation<Integer>> readOperationFactory;
	
	protected abstract String giveLockStatement();
	
	protected String giveCreateTableStatement() {
		return "create table Toto(id bigint)";
	}
	
	protected abstract Predicate<Throwable> giveCancelOperationPredicate();
	
	/**
	 * Overridden to use a {@link CurrentThreadConnectionProvider}
	 */
	@Override
	protected void setConnectionProvider() {
		DataSource dataSource = giveDataSource();
		this.connectionProvider = new CurrentThreadConnectionProvider(dataSource);
		clearDatabaseSchema();
	}
	
	@AfterEach
	protected void cleanThreadAttachment() {
		try {
			this.connectionProvider.giveConnection().close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@BeforeEach
	protected void createReadOperationFactory() {
		this.readOperationFactory = ReadOperation::new;
	}
	
	@Test
	protected void cancel() throws SQLException, InterruptedException {
		// we're going to take a lock on a table that another thread wants to read
		try (Connection lockingConnection = connectionProvider.giveConnection()) {
			// activate manual transaction
			lockingConnection.setAutoCommit(false);
			lockingConnection.prepareStatement(giveCreateTableStatement()).execute();
			lockingConnection.commit();	// some databases require a commit to have DDL elements available to other transactions, like DML elements (PostgreSQL)
			// current Thread locks table
			lockTable(lockingConnection);
			
			CountDownLatch threadStartedMarker = new CountDownLatch(1);
			AtomicBoolean isSelectExecuted = new AtomicBoolean(false);
			Duo<Runnable, SQLOperation> testInstance = createLockingStatement();
			Throwable[] capturedException = new Throwable[1];
			Thread selectCommandThread = new Thread(() -> {
				try (SQLOperation<?> localTestInstance = testInstance.getRight()) {
					threadStartedMarker.countDown();
					testInstance.getLeft().run();
					isSelectExecuted.set(true);
				} catch (Throwable t) {
					capturedException[0] = t;
				} finally {
					((CurrentThreadConnectionProvider) connectionProvider).releaseConnection();
				}
			});
			selectCommandThread.start();
			
			// we wait for the thread which is itself waiting for lock
			threadStartedMarker.await(1, TimeUnit.SECONDS);
			// we wait a bit to let select order starts 
			selectCommandThread.join(200);
			// really ensure that select is stuck
			assertThat(isSelectExecuted.get()).isFalse();
			// this must free select order
			testInstance.getRight().cancel();
			// The Thread should still be stuck because the statement is cancelled, not him
			selectCommandThread.join(200);	// waiting to let the Thread eventually access the isSelectExecuted.set(true) code
			assertThat(isSelectExecuted.get()).isFalse();
			// we let thread stops else it generates unexpected exception (caused by test exit)
			selectCommandThread.join(200);
			
			assertThat(giveCancelOperationPredicate().test(capturedException[0])).withThreadDumpOnError().isTrue();
			lockingConnection.rollback();    // release lock for clean test exit
			
			((CurrentThreadConnectionProvider) connectionProvider).releaseConnection();
		}
	}
	
	protected Duo<Runnable, SQLOperation> createLockingStatement() {
		ReadOperation<Integer> testInstance = readOperationFactory.apply(new PreparedSQL("select * from Toto", new HashMap<>()), connectionProvider);
		return new Duo<>(testInstance::execute, testInstance);
	}
	
	protected void lockTable(Connection lockingConnection) throws SQLException {
		lockingConnection.prepareStatement(giveLockStatement()).execute();
	}
}
