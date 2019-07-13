package org.gama.sql.spring;

import javax.sql.DataSource;

import org.gama.lang.test.Assertions;
import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor.JdbcOperation;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
class PlatformTransactionManagerConnectionProviderTest {
	
	@Test
	void givenTransactionManagerIsNotSupported_throwException() {
		Assertions.assertThrows(() -> new PlatformTransactionManagerConnectionProvider(mock(PlatformTransactionManager.class)),
				Assertions.hasExceptionInCauses(UnsupportedOperationException.class));
	}
	
	@Test
	void getCurrentConnection_givenHibernateTransactionManager_itsDataSourceIsCalled() {
		ModifiableInt getDataSourceInvokationCounter = new ModifiableInt();
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new HibernateTransactionManager() {
			@Override
			public DataSource getDataSource() {
				getDataSourceInvokationCounter.increment();
				return mock(DataSource.class);	// to avoid NullPointerException
			}
		});
		
		// When
		testInstance.getCurrentConnection();
		// Then
		Assertions.assertEquals(1, getDataSourceInvokationCounter.getValue());
	}
	
	@Test
	void getCurrentConnection_givenJpaTransactionManager_itsDataSourceIsCalled() {
		ModifiableInt getDataSourceInvokationCounter = new ModifiableInt();
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new JpaTransactionManager() {
			@Override
			public DataSource getDataSource() {
				getDataSourceInvokationCounter.increment();
				return mock(DataSource.class);	// to avoid NullPointerException
			}
		});
		
		
		// When
		testInstance.getCurrentConnection();
		// Then
		Assertions.assertEquals(1, getDataSourceInvokationCounter.getValue());
	}
	
	@Test
	void getCurrentConnection_givenDataSourceTransactionManager_itsDataSourceIsCalled() {
		ModifiableInt getDataSourceInvokationCounter = new ModifiableInt();
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new DataSourceTransactionManager() {
			@Override
			public DataSource getDataSource() {
				getDataSourceInvokationCounter.increment();
				return mock(DataSource.class);	// to avoid NullPointerException
			}
		});
		
		
		// When
		testInstance.getCurrentConnection();
		// Then
		Assertions.assertEquals(1, getDataSourceInvokationCounter.getValue());
	}
	
	@Test
	void executeInNewTransaction_whenOperationSucceeds_commitIsInvoked() {
		PlatformTransactionManager transactionManagerMock = mock(PlatformTransactionManager.class);
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(transactionManagerMock, mock(TransactionManagerDataSourceProvider.class));
		
		// When
		JdbcOperation jdbcOperationMock = mock(JdbcOperation.class);
		testInstance.executeInNewTransaction(jdbcOperationMock);
		
		// Then
		InOrder inOrder = inOrder(jdbcOperationMock, transactionManagerMock);
		// action is invoked
		inOrder.verify(jdbcOperationMock).execute();
		// transaction is commited
		inOrder.verify(transactionManagerMock).commit(any());
	}
	
	@Test
	void executeInNewTransaction_whenOperationFails_rollbackIsInvoked() {
		PlatformTransactionManager transactionManagerMock = mock(PlatformTransactionManager.class);
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(transactionManagerMock, mock(TransactionManagerDataSourceProvider.class));
		
		// When
		JdbcOperation jdbcOperationMock = mock(JdbcOperation.class);
		doAnswer(new ThrowsException(new RuntimeException())).when(jdbcOperationMock).execute();
		
		Assertions.assertThrows(() -> testInstance.executeInNewTransaction(jdbcOperationMock), Assertions.hasExceptionInCauses(RuntimeException.class));
		
		// Then
		InOrder inOrder = inOrder(jdbcOperationMock, transactionManagerMock);
		// action is invoked
		inOrder.verify(jdbcOperationMock).execute();
		// transaction is rolled back
		inOrder.verify(transactionManagerMock).rollback(any());
	}
}