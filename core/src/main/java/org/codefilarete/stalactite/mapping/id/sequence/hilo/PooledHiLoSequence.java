package org.codefilarete.stalactite.mapping.id.sequence.hilo;

import org.codefilarete.stalactite.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequencePersister.Sequence;
import org.codefilarete.stalactite.sql.Dialect;

/**
 * Long identifier generator for an entity class with pooling system.
 * This class reserves a range of identifiers (see {@link PooledHiLoSequenceOptions#getPoolSize()} and consumes them in
 * memory. Each time its pool is empty it goes back to the database to ask for another set of identifiers. Though if
 * JVM is shutdown while pool is not totally consumed, then a bunch of identifiers are definitively lost for the system.
 * 
 * It stores the state of its sequence in a table (which can be shared between sequences, see {@link PooledHiLoSequencePersister}).
 * Stored state is the highest value reserved by current instance. Then external systems may use upper values without
 * constraint but to write their own highest reserved value. 
 * 
 * Inspired by "enhanced table generator" with Hilo Optimizer from Hibernate.
 * 
 * @author Guillaume Mary
 */
public class PooledHiLoSequence implements org.codefilarete.tool.function.Sequence<Long> {
	
	private LongPool sequenceState;
	
	private final PooledHiLoSequencePersister persister;
	
	private final PooledHiLoSequenceOptions options;
	
	public PooledHiLoSequence(PooledHiLoSequenceOptions options, Dialect dialect, SeparateTransactionExecutor separateTransactionExecutor, int jdbcBatchSize) {
		this(options, new PooledHiLoSequencePersister(options.getStorageOptions(), dialect, separateTransactionExecutor, jdbcBatchSize));
	}

	public PooledHiLoSequence(PooledHiLoSequenceOptions options, PooledHiLoSequencePersister persister) {
		this.options = options;
		this.persister = persister;
	}
	
	public PooledHiLoSequencePersister getPersister() {
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
		String sequenceName = this.options.getSequenceName();
		Sequence existingSequence = this.persister.select(sequenceName);
		long initialValue = existingSequence == null ? this.options.getInitialValue() : existingSequence.getStep();
		// we decrement initialValue to compensate for LongPool incrementing first value on its next() call
		this.sequenceState = new LongPool(options.getPoolSize(), --initialValue) {
			@Override
			void onBoundReached() {
				persister.reservePool(sequenceName, getPoolSize());
			}
		};
		// we consider that a new boundary is reached in order to insert next state
		sequenceState.onBoundReached();
	}
	
	/**
	 * Range of long. Used as a pool for incrementable long values.
	 * Notifies when upper bound is reached.
	 * @see #onBoundReached() 
	 */
	private abstract static class LongPool {
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
