package org.gama.stalactite.sql.spring;

import javax.sql.DataSource;

import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.trace.ModifiableInt;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor.JdbcOperation;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
		assertThatThrownBy(() -> new PlatformTransactionManagerConnectionProvider(mock(PlatformTransactionManager.class)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class))
				.isNotNull();
	}
	
	@Test
	void giveConnection_givenHibernateTransactionManager_itsDataSourceIsCalled() {
		ModifiableInt getDataSourceInvokationCounter = new ModifiableInt();
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new HibernateTransactionManager() {
			@Override
			public DataSource getDataSource() {
				getDataSourceInvokationCounter.increment();
				return mock(DataSource.class);	// to avoid NullPointerException
			}
		});
		
		// When
		testInstance.giveConnection();
		// Then
		assertThat(getDataSourceInvokationCounter.getValue()).isEqualTo(1);
	}
	
	@Test
	void giveConnection_givenJpaTransactionManager_itsDataSourceIsCalled() {
		ModifiableInt getDataSourceInvokationCounter = new ModifiableInt();
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new JpaTransactionManager() {
			@Override
			public DataSource getDataSource() {
				getDataSourceInvokationCounter.increment();
				return mock(DataSource.class);	// to avoid NullPointerException
			}
		});
		
		
		// When
		testInstance.giveConnection();
		// Then
		assertThat(getDataSourceInvokationCounter.getValue()).isEqualTo(1);
	}
	
	@Test
	void giveConnection_givenDataSourceTransactionManager_itsDataSourceIsCalled() {
		ModifiableInt getDataSourceInvokationCounter = new ModifiableInt();
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new DataSourceTransactionManager() {
			@Override
			public DataSource getDataSource() {
				getDataSourceInvokationCounter.increment();
				return mock(DataSource.class);	// to avoid NullPointerException
			}
		});
		
		
		// When
		testInstance.giveConnection();
		// Then
		assertThat(getDataSourceInvokationCounter.getValue()).isEqualTo(1);
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
		
		assertThatThrownBy(() -> testInstance.executeInNewTransaction(jdbcOperationMock))
				.extracting(t -> Exceptions.findExceptionInCauses(t, RuntimeException.class))
				.isNotNull();
		
		// Then
		InOrder inOrder = inOrder(jdbcOperationMock, transactionManagerMock);
		// action is invoked
		inOrder.verify(jdbcOperationMock).execute();
		// transaction is rolled back
		inOrder.verify(transactionManagerMock).rollback(any());
	}
}