package org.codefilarete.stalactite.spring.transaction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.engine.CurrentThreadTransactionalConnectionProvider;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.CommitListener;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.RollbackListener;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.function.Hanger.Holder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		StalactitePlatformTransactionManagerTest.StalactiteTransactionalContextConfiguration.class
})
class StalactitePlatformTransactionManagerTest {
	
	@Autowired
	private PersistenceContext persistenceContext;
	
	@Autowired
	private StalactitePlatformTransactionManager testInstance;
	
	@Autowired
	private DataSource dataSource;
	
	@Autowired
	private Table personTable;
	
	@Nested
	@TestMethodOrder(OrderAnnotation.class)
	class WithPersistenceContext {
		
		// Adding @Transactional marks the methods to be transactional with rollback after their execution,
		// without it StalactitePlatformTransactionManager won't find any running transaction nor pending connection in
		// TransactionSynchronizationManager, making it throw a NullPointerException
		// This is required because PersistenceContext.insert(..) is not marked as @Transactional
		// as SimpleStalactiteRepository.save(..) is.
		@Transactional
		@Test
		@Order(1)
		void createData() {
			Column<Table, Long> idColumn = personTable.getColumn("id");
			Column<Table, String> nameColumn = personTable.getColumn("name");
			persistenceContext.insert(personTable)
					.set(idColumn, 42L)
					.set(nameColumn, "Toto")
					.execute();
		}
		
		// Adding @Transactional marks the methods to be transactional with rollback after their execution,
		// without it StalactitePlatformTransactionManager won't find any running transaction nor pending connection in
		// TransactionSynchronizationManager, making it throw a NullPointerException
		// This is required because PersistenceContext.insert(..) is not marked as @Transactional
		// as SimpleStalactiteRepository.save(..) is.
		@Transactional
		@Test
		@Order(2)
		void createSameDataAgain() {
			Column<Table, Long> idColumn = personTable.getColumn("id");
			Column<Table, String> nameColumn = personTable.getColumn("name");
			// trying to insert : this will throw an exception if data already exists due to primary key conflict
			persistenceContext.insert(personTable)
					.set(idColumn, 42L)
					.set(nameColumn, "Toto")
					.execute();
		}
	}
	
	@Nested
	class GiveConnection {
		
		@Transactional
		@Test
		void returnsActiveTransactionConnection() {
			Connection currentConnection = testInstance.giveConnection();
			ConnectionHolder resource = (ConnectionHolder) TransactionSynchronizationManager.getResource(dataSource);
			// we get the right exception because the method is @Transactional
			assertThat(currentConnection).isSameAs(resource.getConnection());
		}
		
		@Test
		void noActiveTransaction_throwsException() {
			// it throws an exception because the method is not @Transactional
			assertThatThrownBy(testInstance::giveConnection)
					.extracting(t -> Exceptions.findExceptionInCauses(t, IllegalStateException.class), InstanceOfAssertFactories.THROWABLE)
					.hasMessage("No active transaction");
		}
	}
	
	
	@Nested
	class ExecuteInNewTransaction {
		
		@Transactional    // necessary to mimic real-life usage
		@Test
		void whenOperationSucceeds_commitIsInvoked() throws SQLException {
			Connection currentConnection = testInstance.giveConnection();
			
			CommitListener commitListener = mock(CommitListener.class);
			testInstance.addCommitListener(commitListener);
			RollbackListener rollbackListener = mock(RollbackListener.class);
			testInstance.addRollbackListener(rollbackListener);
			
			
			Holder<Connection> capturedSeparateConnection = new Holder<>();
			// When (we capture passed connection for further checking)
			testInstance.executeInNewTransaction(capturedSeparateConnection::set);
			
			assertThat(capturedSeparateConnection.get()).isNotSameAs(currentConnection);
			verify(capturedSeparateConnection.get()).commit();
			verify(capturedSeparateConnection.get(), never()).rollback();
			verify(currentConnection, never()).commit();
			verify(currentConnection, never()).rollback();
			verify(commitListener).beforeCommit();
			verify(commitListener).afterCommit();
			verify(rollbackListener, never()).beforeRollback();
			verify(rollbackListener, never()).afterRollback();
		}
		
		@Transactional    // necessary to mimic real-life usage
		@Test
		void whenOperationFails_rollbackIsInvoked() throws SQLException {
			Connection currentConnection = testInstance.giveConnection();
			
			CommitListener commitListener = mock(CommitListener.class);
			testInstance.addCommitListener(commitListener);
			RollbackListener rollbackListener = mock(RollbackListener.class);
			testInstance.addRollbackListener(rollbackListener);
			
			
			Holder<Connection> capturedSeparateConnection = new Holder<>();
			// When
			try {
				testInstance.executeInNewTransaction(currentSeparateConnection -> {
					capturedSeparateConnection.set(currentSeparateConnection);
					throw new RuntimeException("any kind of exception, only created to rollback connection");
				});
			} catch (RuntimeException ignored) {
				
			}
			
			assertThat(capturedSeparateConnection.get()).isNotSameAs(currentConnection);
			verify(capturedSeparateConnection.get(), never()).commit();
			verify(capturedSeparateConnection.get()).rollback();
			verify(currentConnection, never()).commit();
			verify(currentConnection, never()).rollback();
			verify(commitListener, never()).beforeCommit();
			verify(commitListener, never()).afterCommit();
			// before rollback is not invoked because Spring doesn't invoke beforeCompletion(STATUS_ROLLED_BACK)
			verify(rollbackListener, never()).beforeRollback();
			verify(rollbackListener).afterRollback();
		}
	}
	
	public static class StalactiteTransactionalContextConfiguration {
		
		@Bean
		public DataSource dataSource() throws SQLException {
			HSQLDBInMemoryDataSource dataSource = spy(new HSQLDBInMemoryDataSource());
			when(dataSource.getConnection()).thenAnswer((Answer<Connection>) invocation -> {
				try {
					return spy((Connection) invocation.callRealMethod());
				} catch (Throwable e) {
					throw new RuntimeException(e);
				}
			});
			
			return dataSource;
		}
		
		@Bean
		public StalactitePlatformTransactionManager transactionManager(DataSource dataSource) {
			return new StalactitePlatformTransactionManager(dataSource);
		}
		
		@Bean
		public PersistenceContext persistenceContext(StalactitePlatformTransactionManager transactionManager) {
			Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
			dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
			dialect.getSqlTypeRegistry().put(Identifier.class, "int");
			
			return new PersistenceContext(transactionManager, dialect);
		}
		
		@Bean
		public Table personTable(Schema schema) {
			Table personTable = new Table(schema, "Person");
			personTable.addColumn("id", long.class);
			personTable.addColumn("name", String.class);
			return personTable;
		}
		
		@Bean
		public Schema schema() {
			return new Database().new Schema();
		}
		
		@EventListener
		public void onApplicationEvent(ContextRefreshedEvent event) {
			DataSource dataSource = event.getApplicationContext().getBean(DataSource.class);
			Schema schema = event.getApplicationContext().getBean(Schema.class);
			Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
			// Note that we use a CurrentThreadTransactionalConnectionProvider instead of existing StalactitePlatformTransactionManager
			// because Transaction doesn't exist yet, even by marking @Transactional current method 
			DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlTableGenerator(), dialect.getDdlSequenceGenerator(), new CurrentThreadTransactionalConnectionProvider(dataSource));
			ddlDeployer.getDdlGenerator().setTables(schema.getTables());
			ddlDeployer.getDdlGenerator().setSequences(schema.getSequences());
			ddlDeployer.deployDDL();
		}
	}
}