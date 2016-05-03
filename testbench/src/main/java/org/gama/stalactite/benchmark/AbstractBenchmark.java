package org.gama.stalactite.benchmark;

import com.zaxxer.hikari.HikariDataSource;
import org.gama.lang.collection.Collections;
import org.gama.lang.exception.Exceptions;
import org.gama.lang.trace.Chrono;
import org.gama.stalactite.Logger;
import org.gama.stalactite.benchmark.connection.MySQLDataSourceFactory;
import org.gama.stalactite.benchmark.connection.MySQLDataSourceFactory.Property;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.*;

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
		DataSource dataSource = new MySQLDataSourceFactory().newDataSource("localhost:3306", "sandbox", "dev", "dev", new EnumMap<>(Property.class));
		HikariDataSource pooledDataSource = new HikariDataSource();
		pooledDataSource.setDataSource(dataSource);
		dataSource = pooledDataSource;
		dataSourceTransactionManager = new DataSourceTransactionManager(dataSource);
		
		SpringConnectionProvider transactionManager = new SpringConnectionProvider(dataSourceTransactionManager);
		final PersistenceContext persistenceContext = new PersistenceContext(transactionManager, new HSQLBDDialect());
		PersistenceContexts.setCurrent(persistenceContext);
		
		appendMappingStrategy(persistenceContext);
		
		TransactionTemplate transactionTemplate = new TransactionTemplate(dataSourceTransactionManager);
		transactionTemplate.execute(new TransactionCallbackWithoutResult() {
			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				try {
					dropAndDeployDDL(persistenceContext);
				} catch (SQLException e) {
					throw Exceptions.asRuntimeException(e);
				}
			}
		});
		
		for (int i = 0; i < iteration; i++) {
			List<Data> data = generateData(quantity);
			insertData(data);
		}
	}
	
	protected void dropAndDeployDDL(PersistenceContext persistenceContext) throws SQLException {
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.dropDDL();
		ddlDeployer.deployDDL();
	}
	
	protected void appendMappingStrategy(PersistenceContext persistenceContext) {
		IMappingBuilder totoMappingBuilder = newMappingBuilder();
		persistenceContext.add(totoMappingBuilder.getClassMappingStrategy());
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
			throw Exceptions.asRuntimeException(e);
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
			throw Exceptions.asRuntimeException(e);
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
	
	public abstract class CallableDataPersister<D> implements Callable<Void> {
		
		private final List<D> data;
		protected final PersistenceContext persistenceContext;
		
		public CallableDataPersister(List<D> data, PersistenceContext persistenceContext) {
			this.data = data;
			this.persistenceContext = persistenceContext;
		}
		
		@Override
		public Void call() throws Exception {
			PersistenceContexts.setCurrent(persistenceContext);
			try {
				TransactionTemplate transactionTemplate = new TransactionTemplate(dataSourceTransactionManager);
				transactionTemplate.execute(new TransactionCallbackWithoutResult() {
					@Override
					protected void doInTransactionWithoutResult(TransactionStatus status) {
						persist(PersistenceContexts.getCurrent(), data);
					}
				});
			} finally {
				PersistenceContexts.clearCurrent();
			}
			return null;
		}
		
		protected abstract void persist(PersistenceContext persistenceContext, List<D> data);
	}
	
}
