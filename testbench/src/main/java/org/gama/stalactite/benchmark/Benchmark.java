package org.gama.stalactite.benchmark;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.gama.stalactite.benchmark.TotoMappingBuilder.Toto;
import org.gama.stalactite.persistence.engine.PersistenceContext;

/**
 * @author Guillaume Mary
 */
public class Benchmark extends AbstractBenchmark<Toto> {
	
	public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
		new Benchmark().run();
	}
	
	Benchmark() {
		super(20, 20000);
	}
	
	@Override
	protected Callable<Toto> newCallableDataGenerator() {
		return new CallableDataGenerator();
	}
	
	@Override
	protected Callable<Void> newCallableDataPersister(List<Toto> data) {
		return new CallableDataInsert(data, PersistenceContext.getCurrent());
	}
	
	@Override
	protected IMappingBuilder newMappingBuilder() {
		return new TotoMappingBuilder();
	}
	
	public class CallableDataGenerator implements Callable<Toto> {
		
		private final DataGenerator dataGenerator = new DataGenerator(null);
		
		public CallableDataGenerator() {
		}
		
		@Override
		public Toto call() throws Exception {
			Toto toto = new Toto();
			for (int i = 0; i < Toto.QUESTION_COUNT; i++) {
				Object value = dataGenerator.randomInteger(null);
				toto.put((long) i, value);
			}
			return toto;
		}
	}
	
	public class CallableDataInsert extends CallableDataPersister<Toto> {
		
		public CallableDataInsert(List<Toto> data, PersistenceContext persistenceContext) {
			super(data, persistenceContext);
		}
		
		@Override
		protected void persist(PersistenceContext persistenceContext, List<Toto> data) {
			persistenceContext.getPersister(Toto.class).persist(data);
		}

	}
}
