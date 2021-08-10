package org.gama.stalactite.sql.dml;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.gama.lang.trace.ModifiableBoolean;
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
		DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(dataSource);
		
		// we're going to take a lock on a table that another thread wants to read
		try (Connection lockingConnection = connectionProvider.getCurrentConnection()) {
			// activate manual transaction
			lockingConnection.setAutoCommit(false);
			lockingConnection.prepareStatement("create table Toto(id bigint)").execute();
			lockingConnection.commit();	// some databases require a commit to have DDL elements available to other transactions, like DML elements (PostgreSQL)
			lockingConnection.prepareStatement(giveLockStatement()).execute();
			
			CountDownLatch countDownLatch = new CountDownLatch(1);
			ModifiableBoolean isSelectExecuted = new ModifiableBoolean(false);
			Connection connection = dataSource.getConnection();
			ReadOperation<Integer> testInstance = readOperationFactory.apply(new PreparedSQL("select * from toto", new HashMap<>()), new SimpleConnectionProvider(connection));
			Throwable[] capturedException = new Throwable[1];
			Thread thread = new Thread(() -> {
				try (ReadOperation<Integer> localTestInstance = testInstance) {
					countDownLatch.countDown();
					localTestInstance.execute();
					isSelectExecuted.setTrue();
				} catch (Throwable t) {
					capturedException[0] = t;
				}
			});
			thread.start();
			
			// we wait for the thread which is itself waiting for lock
			countDownLatch.await();
			// we add a little wait to let select order to be started 
			TimeUnit.MILLISECONDS.sleep(200);
			// really ensure that select is stuck
			assertThat(isSelectExecuted.isTrue()).isFalse();
			// this must free select order
			doCancel(testInstance);
			// we let thread stops else it generates non expected exception (caused by test exit)
			TimeUnit.MILLISECONDS.sleep(200);
			
			assertThat(giveCancelOperationPredicate().test(capturedException[0])).isTrue();
			lockingConnection.commit();    // release lock for clean test exit
		}
	}
	
	protected void doCancel(ReadOperation<Integer> testInstance) throws SQLException {
		testInstance.cancel();
	}
}