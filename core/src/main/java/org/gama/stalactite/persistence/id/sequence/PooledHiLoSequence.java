package org.gama.stalactite.persistence.id.sequence;

import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.id.sequence.SequencePersister.Sequence;
import org.gama.stalactite.persistence.sql.Dialect;

/**
 * Long identifier generator for an entity class.
 * Store the state of its sequence in a table (which can be shared, see {@link SequencePersister}
 * 
 * Inspired by "enhanced table generator" with Hilo Optimizer from Hibernate.
 * 
 * @author Guillaume Mary
 */
public class PooledHiLoSequence implements org.gama.lang.function.Sequence<Long> {
	
	private LongPool sequenceState;
	
	private SequencePersister persister;
	
	private PooledHiLoSequenceOptions options;
	
	private final Dialect dialect;
	private final SeparateTransactionExecutor separateTransactionExecutor;
	private final int jdbcBatchSize;
	
	public PooledHiLoSequence(PooledHiLoSequenceOptions options, Dialect dialect, SeparateTransactionExecutor separateTransactionExecutor, int jdbcBatchSize) {
		this.dialect = dialect;
		this.separateTransactionExecutor = separateTransactionExecutor;
		this.jdbcBatchSize = jdbcBatchSize;
		configure(options);
	}
	
	public void configure(PooledHiLoSequenceOptions options) {
		this.persister = new SequencePersister(options.getStorageOptions(), dialect, separateTransactionExecutor, jdbcBatchSize);
		this.options = options;
	}
	
	public SequencePersister getPersister() {
		return persister;
	}
	
	public PooledHiLoSequenceOptions getOptions() {
		return options;
	}
	
	/**
	 * Synchronized because multiple Thread may access this instance to insert their entities. 
	 * 
	 * @return never null
	 */
	@Override
	public synchronized Long next() {
		if (sequenceState == null) {
			// No state yet so we create one
			initSequenceState();
		}
		return sequenceState.nextValue();
	}
	
	private void initSequenceState() {
		String sequenceName = getSequenceName();
		Sequence existingSequence = this.persister.select(sequenceName);
		long initialValue = existingSequence == null ? this.options.getInitialValue() : existingSequence.getStep();
		// we decrement initialValue to compensate for LongPool incrementing first value on its next() call
		this.sequenceState = new LongPool(options.getPoolSize(), --initialValue) {
			@Override
			void onBoundReached() {
				persister.reservePool(sequenceName, getPoolSize());
			}
		};
		// if no sequence exists we consider that a new boundary is reached in order to insert initial state
		if (existingSequence == null) {
			sequenceState.onBoundReached();
		}
	}
	
	protected String getSequenceName() {
		return this.options.getSequenceName();
	}
	
	/**
	 * Range of long. Used as a pool for incrementable long values.
	 * Notifies when upper bound is reached.
	 * @see #onBoundReached() 
	 */
	private static abstract class LongPool {
		/** Pool size. Doesn't change */
		private final int poolSize;
		/** Incremented value */
		private long currentValue;
		/** Upper bound for current range. Is incremented by poolSize when reached by currentValue */
		protected long upperBound;
		
		public LongPool(int poolSize, long currentValue) {
			this.poolSize = poolSize;
			this.currentValue = currentValue;
			nextBound();
		}
		
		public int getPoolSize() {
			return poolSize;
		}
		
		public long getCurrentValue() {
			return currentValue;
		}
		
		/**
		 * Returns nextValue: currentValue + 1.
		 * Calls {@link #onBoundReached()} if the upperBound is reached.
		 * 
		 * @return currentValue + 1
		 */
		public long nextValue() {
			if (currentValue == upperBound) {
				onBoundReached();
				nextBound();
			}
			currentValue++;
			return currentValue;
		}
		
		/**
		 * Changes upper bound: currentValue + poolSize
		 */
		protected void nextBound() {
			this.upperBound = currentValue + poolSize;
		}
		
		/**
		 * Called when upper bound is reached
		 */
		abstract void onBoundReached();
	}
}
