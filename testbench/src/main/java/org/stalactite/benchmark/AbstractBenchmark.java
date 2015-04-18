package org.stalactite.benchmark;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.*;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.stalactite.Logger;
import org.stalactite.lang.collection.Collections;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.lang.trace.Chrono;
import org.stalactite.benchmark.connection.MySQLDataSourceFactory;
import org.stalactite.benchmark.connection.MySQLDataSourceFactory.Property;
import org.stalactite.persistence.engine.DDLDeployer;
import org.stalactite.persistence.engine.PersistenceContext;

import com.zaxxer.hikari.HikariDataSource;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractBenchmark<Data> {
	
	protected static final Logger LOGGER = Logger.getLogger(AbstractBenchmark.class);
	
	protected int quantity;
	protected int iteration;
	protected DataSourceTransactionManager dataSourceTransactionManager;
	
	public AbstractBenchmark() {
		this(20, 20000);
	}
	
	public AbstractBenchmark(int iteration, int quantity) {
		this.iteration = iteration;
		this.quantity = quantity;
	}
	
	public void run() throws SQLException, ExecutionException, InterruptedException {
		DataSource dataSource = new MySQLDataSourceFactory().newDataSource("localhost:3306", "sandbox", "appadmin", "Interview?!", new EnumMap<>(Property.class));
		HikariDataSource pooledDataSource = new HikariDataSource();
		pooledDataSource.setDataSource(dataSource);
		dataSource = pooledDataSource;
		dataSourceTransactionManager = new DataSourceTransactionManager(dataSource);
		
		SpringTransactionManager transactionManager = new SpringTransactionManager(dataSourceTransactionManager);
		final PersistenceContext persistenceContext = new PersistenceContext(transactionManager, new HSQLBDDialect());
		PersistenceContext.setCurrent(persistenceContext);
		
		IMappingBuilder totoMappingBuilder = newMappingBuilder();
		persistenceContext.add(totoMappingBuilder.getClassMappingStrategy());
		
		TransactionTemplate transactionTemplate = new TransactionTemplate(dataSourceTransactionManager);
		transactionTemplate.execute(new TransactionCallbackWithoutResult() {
			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				try {
					DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
					ddlDeployer.dropDDL();
					ddlDeployer.deployDDL();
				} catch (SQLException e) {
					Exceptions.throwAsRuntimeException(e);
				}
			}
		});
		
		for (int i = 0; i < iteration; i++) {
			List<Data> data = generateData(quantity);
			insertData(data);
		}
	}
	
	private void insertData(List<Data> data) throws InterruptedException, ExecutionException {
		ExecutorService dataInserterExecutor = Executors.newFixedThreadPool(4);
		List<Callable<Void>> dataInserters = new ArrayList<>();
		List<List<Data>> blocks = Collections.parcel(data, 100);
		for (List<Data> block : blocks) {
			dataInserters.add(newCallableDataPersister(block));
		}
		Chrono c = new Chrono();
		List<Future<Void>> futures = null;
		try {
			futures = dataInserterExecutor.invokeAll(dataInserters);
		} catch (InterruptedException e) {
			Exceptions.throwAsRuntimeException(e);
		}
		try {
			for (Future<Void> future : futures) {
				future.get();
			}
		} finally {
			dataInserterExecutor.shutdown();
		}
		LOGGER.info("Données insérées en " + c);
	}
	
	protected List<Data> generateData(int quantity) throws InterruptedException, ExecutionException {
		ExecutorService dataGeneratorExecutor = Executors.newFixedThreadPool(4);
		List<Callable<Data>> dataGenerators = new ArrayList<>();
		for (int i = 0; i < quantity; i++) {
			dataGenerators.add(newCallableDataGenerator());
		}
		
		Chrono c = new Chrono();
		List<Data> data = new ArrayList<>();
		List<Future<Data>> futures = null;
		try {
			futures = dataGeneratorExecutor.invokeAll(dataGenerators);
		} catch (InterruptedException e) {
			Exceptions.throwAsRuntimeException(e);
		}
		try {
			for (Future<Data> future : futures) {
				data.add(future.get());
			}
		} finally {
			dataGeneratorExecutor.shutdown();
		}
		LOGGER.info("Données générées " + c);
		return data;
	}
	
	protected abstract IMappingBuilder newMappingBuilder();
	
	protected abstract Callable<Data> newCallableDataGenerator();
	
	protected abstract Callable<Void> newCallableDataPersister(List<Data> data);
	
	public abstract class CallableDataPersister<Data> implements Callable<Void> {
		
		private final List<Data> data;
		protected final PersistenceContext persistenceContext;
		
		public CallableDataPersister(List<Data> data, PersistenceContext persistenceContext) {
			this.data = data;
			this.persistenceContext = persistenceContext;
		}
		
		@Override
		public Void call() throws Exception {
			PersistenceContext.setCurrent(persistenceContext);
			try {
				TransactionTemplate transactionTemplate = new TransactionTemplate(dataSourceTransactionManager);
				transactionTemplate.execute(new TransactionCallbackWithoutResult() {
					@Override
					protected void doInTransactionWithoutResult(TransactionStatus status) {
						persist(PersistenceContext.getCurrent(), data);
					}
				});
			} finally {
				PersistenceContext.clearCurrent();
			}
			return null;
		}
		
		protected abstract void persist(PersistenceContext persistenceContext, List<Data> data);
	}
	
}
