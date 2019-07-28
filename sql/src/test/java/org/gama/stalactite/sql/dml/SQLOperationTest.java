package org.gama.stalactite.sql.dml;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.gama.lang.exception.Exceptions;
import org.gama.lang.trace.ModifiableBoolean;
import org.gama.stalactite.sql.DataSourceConnectionProvider;
import org.gama.stalactite.sql.SimpleConnectionProvider;
import org.gama.stalactite.sql.test.DerbyInMemoryDataSource;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.sql.test.MariaDBEmbeddableDataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
public class SQLOperationTest {
	
	public static Object[][] dataSources() {
		return new Object[][] {
				{ new HSQLDBInMemoryDataSource(), "lock table Toto WRITE",
						(Predicate<Throwable>) Objects::isNull },
				{ new DerbyInMemoryDataSource(), "lock table Toto in EXCLUSIVE MODE" ,
						(Predicate<Throwable>) Objects::isNull },
				{ new MariaDBEmbeddableDataSource(3406), "lock table Toto WRITE",
						// MySQL throws an exception on query cancelation (https://mariadb.com/kb/en/library/multi-threading-and-statementcancel/), we check it.
						(Predicate<Throwable>) t -> Exceptions.findExceptionInCauses(t, SQLTransientException.class, "Query execution was interrupted") != null },
		};
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void cancel(DataSource dataSource, String lockStatement, Predicate<Throwable> cancelOperationThrowablePredicate) throws SQLException, InterruptedException {
		DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(dataSource);
		
		// we're going to take a lock on a table that another thread wants to read
		try (Connection lockingConnection = connectionProvider.getCurrentConnection()) {
			// activate manual transaction
			lockingConnection.setAutoCommit(false);
			lockingConnection.prepareStatement("create table Toto(id bigint)").execute();
			lockingConnection.prepareStatement(lockStatement).execute();
			
			CountDownLatch countDownLatch = new CountDownLatch(1);
			ModifiableBoolean isSelectExecuted = new ModifiableBoolean(false);
			ReadOperation<Integer> testInstance = new ReadOperation<>(new PreparedSQL("select * from toto", new HashMap<>()),
					new SimpleConnectionProvider(dataSource.getConnection()));
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
			assertFalse(isSelectExecuted.isTrue());
			// this must free select order
			testInstance.cancel();
			// we let thread stops else it generates non expected exception (caused by test exit)
			TimeUnit.MILLISECONDS.sleep(200);
			
			assertTrue(cancelOperationThrowablePredicate.test(capturedException[0]));
			lockingConnection.commit();    // release lock for clean test exit
		}
	}
}