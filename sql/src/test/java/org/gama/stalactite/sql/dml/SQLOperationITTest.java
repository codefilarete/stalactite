package org.gama.stalactite.sql.dml;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.DataSourceConnectionProvider;
import org.gama.stalactite.sql.SimpleConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Database integration test template for {@link SQLOperation}, see implementations in sql-adapter submodules
 * 
 * @author Guillaume Mary
 */
abstract class SQLOperationITTest {

	protected DataSource dataSource;
	
	protected BiFunction<PreparedSQL, ConnectionProvider, ReadOperation<Integer>> readOperationFactory;

	@BeforeEach
	abstract void createDataSource();
	
	abstract String giveLockStatement();

	abstract Predicate<Throwable> giveCancelOperationPredicate();
	
	@BeforeEach
	protected void createReadOperationFactory() {
		this.readOperationFactory = ReadOperation::new;
	}
	
	@Test
	void cancel() throws SQLException, InterruptedException {
		ConnectionProvider connectionProvider = new DataSourceConnectionProvider(dataSource);
		
		// we're going to take a lock on a table that another thread wants to read
		try (Connection lockingConnection = connectionProvider.giveConnection()) {
			// activate manual transaction
			lockingConnection.setAutoCommit(false);
			lockingConnection.prepareStatement("create table Toto(id bigint)").execute();
			lockingConnection.commit();	// some databases require a commit to have DDL elements available to other transactions, like DML elements (PostgreSQL)
			// current Thread locks table
			lockTable(lockingConnection);
			
			CountDownLatch threadStartedMarker = new CountDownLatch(1);
			AtomicBoolean isSelectExecuted = new AtomicBoolean(false);
			Connection connection = dataSource.getConnection();
			ReadOperation<Integer> testInstance = readOperationFactory.apply(new PreparedSQL("select * from toto", new HashMap<>()), new SimpleConnectionProvider(connection));
			Throwable[] capturedException = new Throwable[1];
			Thread selectCommandThread = new Thread(() -> {
				try (ReadOperation<Integer> localTestInstance = testInstance) {
					threadStartedMarker.countDown();
					localTestInstance.execute();
					isSelectExecuted.set(true);
				} catch (Throwable t) {
					capturedException[0] = t;
				}
			});
			selectCommandThread.start();
			
			// we wait for the thread which is itself waiting for lock
			threadStartedMarker.await();
			// we wait a bit to let select order starts 
			selectCommandThread.join(200);
			// really ensure that select is stuck
			assertThat(isSelectExecuted.get()).isFalse();
			// this must free select order
			testInstance.cancel();
			// The Thread should still be stuck because the statment is cancelled, not him
			selectCommandThread.join(200);	// waiting to let the Thread eventually access the isSelectExecuted.set(true) code
			assertThat(isSelectExecuted.get()).isFalse();
			// we let thread stops else it generates unexpected exception (caused by test exit)
			selectCommandThread.join(200);
			
			assertThat(giveCancelOperationPredicate().test(capturedException[0])).isTrue();
			lockingConnection.rollback();    // release lock for clean test exit
		}
	}
	
	protected void lockTable(Connection lockingConnection) throws SQLException {
		lockingConnection.prepareStatement(giveLockStatement()).execute();
	}
}