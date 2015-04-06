package org.stalactite.benchmark;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.stalactite.Logger;
import org.stalactite.benchmark.DynamicClassMappingBuilder.DynamicEntity;
import org.stalactite.lang.collection.Collections;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.lang.trace.Chrono;
import org.stalactite.persistence.engine.PersistenceContext;

import com.zaxxer.hikari.HikariDataSource;

/**
 * @author Guillaume Mary
 */
public class BenchmarkDynamic {
	
	private static final Logger LOGGER = Logger.getLogger(BenchmarkDynamic.class);
	private int quantity;
	private int iteration;
	private DynamicClassMappingBuilder dynamicClassMappingBuilder;
	
	public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
		new BenchmarkDynamic().run();
	}
	
	BenchmarkDynamic() {
		quantity = 20000;
		iteration = 20;
	}
	
	public void run() throws SQLException, ExecutionException, InterruptedException {
//		DriverManagerDataSource dataSource = new DriverManagerDataSource("jdbc:hsqldb:mem:test", "sa", "");
//		DataSource dataSource = new DriverManagerDataSource("jdbc:mysql://localhost:3306/sandbox", "appadmin", "Interview?!");
//		IsolationLevelDataSourceAdapter x = new IsolationLevelDataSourceAdapter();
//		x.setTargetDataSource(dataSource);
//		x.setIsolationLevel(TransactionDefinition.ISOLATION_READ_UNCOMMITTED);
//		dataSource = x;
//		DataSource dataSource = new SimpleDriverDataSource(new Driver(), "jdbc:mysql://localhost:3306/sandbox", "appadmin", "Interview?!");
		DataSource dataSource = new DriverManagerDataSource("jdbc:mysql://localhost:3306/sandbox?rewriteBatchStatements=true", "appadmin", "Interview?!") {
			@Override
			public void setLoginTimeout(int timeout) throws SQLException {
				
			}
		};
		HikariDataSource pooledDataSource = new HikariDataSource();
		pooledDataSource.setDataSource(dataSource);
		dataSource = pooledDataSource;
		DataSourceTransactionManager dataSourceTransactionManager = new DataSourceTransactionManager(dataSource);
		
		SpringTransactionManager transactionManager = new SpringTransactionManager(dataSourceTransactionManager);
		final PersistenceContext persistenceContext = new PersistenceContext(transactionManager, new HSQLBDDialect());
		PersistenceContext.setCurrent(persistenceContext);
		
		dynamicClassMappingBuilder = new DynamicClassMappingBuilder();
		persistenceContext.add(dynamicClassMappingBuilder.getClassMappingStrategy());
		
		TransactionTemplate transactionTemplate = new TransactionTemplate(dataSourceTransactionManager);
		transactionTemplate.execute(new TransactionCallbackWithoutResult() {
			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				try {
					persistenceContext.dropDDL();
					persistenceContext.deployDDL();
				} catch (SQLException e) {
					Exceptions.throwAsRuntimeException(e);
				}
			}
		});
		
		for (int i = 0; i < iteration; i++) {
			List<DynamicEntity> data = generateData(quantity);
			insertData(dataSourceTransactionManager, persistenceContext, data);
		}
	}
	
	private void insertData(DataSourceTransactionManager dataSourceTransactionManager, PersistenceContext persistenceContext, List<DynamicEntity> data) throws InterruptedException, ExecutionException {
		ExecutorService dataInserterExecutor = Executors.newFixedThreadPool(4);
		List<CallableDataInsert> dataInserters = new ArrayList<>();
		List<List<DynamicEntity>> blocks = Collections.parcel(data, 100);
		for (List<DynamicEntity> block : blocks) {
			dataInserters.add(new CallableDataInsert(block, dataSourceTransactionManager, persistenceContext));
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
	
	private List<DynamicEntity> generateData(int quantity) throws InterruptedException, ExecutionException {
		ExecutorService dataGeneratorExecutor = Executors.newFixedThreadPool(4);
		List<CallableDataGenerator> dataGenerators = new ArrayList<>();
		DataGenerator dataGenerator = new DataGenerator(null);
		for (int i = 0; i < quantity; i++) {
			dataGenerators.add(new CallableDataGenerator(dataGenerator));
		}
		
		Chrono c = new Chrono();
		List<DynamicEntity> data = new ArrayList<>();
		List<Future<DynamicEntity>> futures = null;
		try {
			futures = dataGeneratorExecutor.invokeAll(dataGenerators);
		} catch (InterruptedException e) {
			Exceptions.throwAsRuntimeException(e);
		}
		try {
			for (Future<DynamicEntity> future : futures) {
				data.add(future.get());
			}
		} finally {
			dataGeneratorExecutor.shutdown();
		}
		LOGGER.info("Données générées " + c);
		
		return data;
	}
	
	public class CallableDataGenerator implements Callable<DynamicEntity> {
		
		private final DataGenerator dataGenerator;
		
		public CallableDataGenerator(DataGenerator dataGenerator) {
			this.dataGenerator = dataGenerator;
		}
		
		@Override
		public DynamicEntity call() throws Exception {
			DynamicEntity dynamicEntity = dynamicClassMappingBuilder.dynamicType.newInstance();
			for (Field field : dynamicClassMappingBuilder.fields) {
				Object value = dataGenerator.randomInteger(null);
				field.set(dynamicEntity, value);
			}
			return dynamicEntity;
		}
	}
	
	public class CallableDataInsert implements Callable<Void> {
		
		private final List<? extends DynamicEntity> data;
		private final PlatformTransactionManager dataSourceTransactionManager;
		private final PersistenceContext persistenceContext;
		
		public CallableDataInsert(List<DynamicEntity> data, PlatformTransactionManager dataSourceTransactionManager, PersistenceContext persistenceContext) {
			this.data = data;
			this.dataSourceTransactionManager = dataSourceTransactionManager;
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
						persistenceContext.getPersister(dynamicClassMappingBuilder.dynamicType).persist((Iterable) data);
					}
				});
			} finally {
				PersistenceContext.clearCurrent();
			}
			return null;
		}
	}
}
