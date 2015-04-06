package org.stalactite.benchmark;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.stalactite.Logger;
import org.stalactite.benchmark.DynamicClassMappingBuilder.DynamicEntity;
import org.stalactite.persistence.engine.PersistenceContext;

/**
 * @author Guillaume Mary
 */
public class BenchmarkDynamic extends AbstractBenchmark<DynamicEntity> {
	
	private static final Logger LOGGER = Logger.getLogger(BenchmarkDynamic.class);
	private DynamicClassMappingBuilder dynamicClassMappingBuilder;
	
	public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
		new BenchmarkDynamic().run();
	}
	
	BenchmarkDynamic() {
		super(20, 20000);
	}
	
	@Override
	protected Callable<DynamicEntity> newCallableDataGenerator() {
		return new CallableDataGenerator();
	}
	
	@Override
	protected Callable<Void> newCallableDataPersister(List<DynamicEntity> data) {
		return new CallableDataInsert(data, PersistenceContext.getCurrent());
	}
	
	@Override
	protected IMappingBuilder newMappingBuilder() {
		this.dynamicClassMappingBuilder = new DynamicClassMappingBuilder();
		return this.dynamicClassMappingBuilder;
	}
	
	public class CallableDataGenerator implements Callable<DynamicEntity> {
		
		private final DataGenerator dataGenerator = new DataGenerator(null);
		
		public CallableDataGenerator() {
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
	
	public class CallableDataInsert extends CallableDataPersister<DynamicEntity> {
		
		public CallableDataInsert(List<DynamicEntity> data, PersistenceContext persistenceContext) {
			super(data, persistenceContext);
		}
		
		protected void persist(PersistenceContext persistenceContext, List<DynamicEntity> data) {
			persistenceContext.getPersister(dynamicClassMappingBuilder.dynamicType).persist((Iterable) data);
		}
	}
}
