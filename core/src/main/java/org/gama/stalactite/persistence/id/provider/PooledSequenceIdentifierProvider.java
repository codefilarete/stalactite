package org.gama.stalactite.persistence.id.provider;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.id.generator.sequence.PooledSequenceIdentifierGenerator;
import org.gama.stalactite.persistence.id.generator.sequence.PooledSequenceIdentifierGeneratorOptions;
import org.gama.stalactite.persistence.sql.Dialect;

/**
 * Provider which will get its values from a database sequence-like (not a real SQL sequence): HiLo algorithm is used
 * by {@link PooledSequenceIdentifierGenerator}
 * 
 * @author Guillaume Mary
 * @see PooledSequenceIdentifierGenerator
 */
public class PooledSequenceIdentifierProvider extends PooledIdentifierProvider<Long> {
	
	private final PooledSequenceIdentifierGenerator sequenceIdentifierGenerator;
	
	/**
	 * @param initialValues the initial values for filling this queue. Can be empty, not null.
	 * @param threshold the threshold below (excluded) which the backgound service is called for filling this queue
	 * @param executor the executor capable of running a background filling of this queue
	 * @param timeOut the time-out after which the {@link #giveNewIdentifier()} gives up and returns null because of an empty queue
	 * @param sequenceIdentifierGenerator the sequence accessor that will provide values
	 */
	public PooledSequenceIdentifierProvider(Collection<Long> initialValues, int threshold, Executor executor, Duration timeOut,
											PooledSequenceIdentifierGenerator sequenceIdentifierGenerator) {
		super(initialValues, threshold, executor, timeOut);
		this.sequenceIdentifierGenerator = sequenceIdentifierGenerator;
	}
	
	/**
	 * @param initialValues the initial values for filling this queue. Can be empty, not null.
	 * @param threshold the threshold below (excluded) which the backgound service is called for filling this queue
	 * @param executor the executor capable of running a background filling of this queue
	 * @param timeOut the time-out after which the {@link #giveNewIdentifier()} gives up and returns null because of an empty queue
	 * @param options parameter for the internal {@link PooledSequenceIdentifierGenerator}
	 * @param dialect parameter for the internal {@link PooledSequenceIdentifierGenerator}
	 * @param separateTransactionExecutor parameter for the internal {@link PooledSequenceIdentifierGenerator}
	 * @param jdbcBatchSize parameter for the internal {@link PooledSequenceIdentifierGenerator}
	 * @see PooledSequenceIdentifierGenerator#PooledSequenceIdentifierGenerator(PooledSequenceIdentifierGeneratorOptions, Dialect, SeparateTransactionExecutor, int) 
	 */
	public PooledSequenceIdentifierProvider(Collection<Long> initialValues, int threshold, Executor executor, Duration timeOut,
											PooledSequenceIdentifierGeneratorOptions options, Dialect dialect, SeparateTransactionExecutor separateTransactionExecutor, int jdbcBatchSize) {
		this(initialValues, threshold, executor, timeOut, new PooledSequenceIdentifierGenerator(options, dialect, separateTransactionExecutor, jdbcBatchSize));
	}
	
	@Override
	protected Collection<Long> retrieveSomeValues() {
		// Our sequence generator is already pooled, but we don't now really how, so we take benefit from our queue by transfering sequence values
		// to us. Moreover since we are supposed to be asynchronous, even if the sequence pooling is not perfect it doesn't matter. 
		
		// We decide to sink all possible values from the sequence
		int poolSize = sequenceIdentifierGenerator.getOptions().getPoolSize();
		List<Long> result = new ArrayList<>(poolSize);
		for (int i = 0; i < poolSize; i++) {
			result.add((Long) sequenceIdentifierGenerator.generate());
		}
		return result;
	}
}
