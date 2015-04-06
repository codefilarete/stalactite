package org.stalactite.benchmark;

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
import org.stalactite.benchmark.TotoMappingBuilder.Toto;
import org.stalactite.lang.collection.Collections;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.lang.trace.Chrono;
import org.stalactite.persistence.engine.PersistenceContext;

import com.zaxxer.hikari.HikariDataSource;

/**
 * @author Guillaume Mary
 */
public class Benchmark {
	
	private static final Logger LOGGER = Logger.getLogger(Benchmark.class);
	private int quantity;
	private int iteration;
	
	public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
		new Benchmark().run();
	}
	
	Benchmark() {
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
		
		TotoMappingBuilder totoMappingBuilder = new TotoMappingBuilder();
		persistenceContext.add(totoMappingBuilder.getClassMappingStrategy());
		
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
			List<Toto> data = generateData(quantity);
			insertData(dataSourceTransactionManager, persistenceContext, data);
		}
	}
	
	private static void insertData(DataSourceTransactionManager dataSourceTransactionManager, PersistenceContext persistenceContext, List<Toto> data) throws InterruptedException, ExecutionException {
		ExecutorService dataInserterExecutor = Executors.newFixedThreadPool(4);
		List<CallableDataInsert> dataInserters = new ArrayList<>();
		List<List<Toto>> blocks = Collections.parcel(data, 100);
		for (List<Toto> block : blocks) {
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
	
	private List<Toto> generateData(int quantity) throws InterruptedException, ExecutionException {
		ExecutorService dataGeneratorExecutor = Executors.newFixedThreadPool(4);
		List<CallableDataGenerator> dataGenerators = new ArrayList<>();
		DataGenerator dataGenerator = new DataGenerator(null);
		for (int i = 0; i < quantity; i++) {
			dataGenerators.add(new CallableDataGenerator(dataGenerator));
		}
		
		Chrono c = new Chrono();
		List<Toto> data = new ArrayList<>();
		List<Future<Toto>> futures = null;
		try {
			futures = dataGeneratorExecutor.invokeAll(dataGenerators);
		} catch (InterruptedException e) {
			Exceptions.throwAsRuntimeException(e);
		}
		try {
			for (Future<Toto> future : futures) {
				data.add(future.get());
			}
		} finally {
			dataGeneratorExecutor.shutdown();
		}
		LOGGER.info("Données générées " + c);
		return data;
	}
	
	public class CallableDataGenerator implements Callable<Toto> {
		
		private final DataGenerator dataGenerator;
		
		public CallableDataGenerator(DataGenerator dataGenerator) {
			this.dataGenerator = dataGenerator;
		}
		
		@Override
		public Toto call() throws Exception {
			Toto toto = new Toto();
//			toto.setId(dataGenerator.randomInteger(iteration * quantity));
//			toto.setA(dataGenerator.randomText(totoTable.a));
//			toto.setB(dataGenerator.randomInteger(totoTable.b));
			for (int i = 0; i < Toto.QUESTION_COUNT; i++) {
				Object value;
//				if (i%2 == 0) {
//					value = dataGenerator.randomText(100);
//				} else {
					value = dataGenerator.randomInteger(null);
//				}
				toto.put((long) i, value);
			}
			return toto;
		}
	}
	
	public static class CallableDataInsert implements Callable<Void> {
		
		private final List<Toto> data;
		private final PlatformTransactionManager dataSourceTransactionManager;
		private final PersistenceContext persistenceContext;
		
		public CallableDataInsert(List<Toto> data, PlatformTransactionManager dataSourceTransactionManager, PersistenceContext persistenceContext) {
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
						persistenceContext.getPersister(Toto.class).persist(data);
					}
				});
			} finally {
				PersistenceContext.clearCurrent();
			}
			return null;
		}
	}
}
