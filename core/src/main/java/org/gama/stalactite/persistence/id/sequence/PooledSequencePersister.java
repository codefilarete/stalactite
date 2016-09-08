package org.gama.stalactite.persistence.id.sequence;

import java.util.Map;

import org.gama.lang.collection.Maps;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.sequence.PooledSequencePersister.PooledSequence;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Database.Schema;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Persister dedicated to pooled entity identifiers.
 * The same instance can be shared, as long as each calls {@link #reservePool(String, int)} with a different name parameter to avoid
 * sequence name collision.
 * 
 * @author Guillaume Mary
 */
public class PooledSequencePersister extends Persister<PooledSequence, String> {
	
	/**
	 * Constructor with default table and column names.
	 * @param dialect the {@link Dialect} to use for database dialog
	 * @param separateTransactionExecutor a transaction provider that mmust give a new and separate transaction
	 * @param jdbcBatchSize the JDBC batch size, not really usefull for this class since it doesn't do massive insert
	 * @see PooledSequencePersistenceOptions#DEFAULT
	 */
	public PooledSequencePersister(Dialect dialect, SeparateTransactionExecutor separateTransactionExecutor, int jdbcBatchSize) {
		this(PooledSequencePersistenceOptions.DEFAULT, dialect, separateTransactionExecutor, jdbcBatchSize);
	}
	
	public PooledSequencePersister(PooledSequencePersistenceOptions storageOptions, Dialect dialect, SeparateTransactionExecutor separateTransactionExecutor, int jdbcBatchSize) {
		// we reuse default PersistentContext
		super(new PooledSequencePersisterConfigurer().buildConfiguration(storageOptions),
				dialect, separateTransactionExecutor, jdbcBatchSize);
	}
	
	public long reservePool(String sequenceName, int poolSize) {
		SequenceBoundJdbcOperation jdbcOperation = new SequenceBoundJdbcOperation(sequenceName, poolSize);
		// the operation is executed in a new and parallel transaction in order to manage concurrent accesses
		((SeparateTransactionExecutor) getConnectionProvider()).executeInNewTransaction(jdbcOperation);
		return jdbcOperation.getUpperBound();
	}
	
	private PooledSequence readBound(String sequenceName) {
		return select(sequenceName);
	}
	
	private static class SequenceTable extends Table {
		
		private final Column nextValColumn;
		private final Column sequenceNameColumn;
		
		public SequenceTable(Schema schema, String name, String sequenceNameColName, String nextValColName) {
			super(schema, name);
			sequenceNameColumn = new Column(sequenceNameColName, String.class);
			sequenceNameColumn.setPrimaryKey(true);
			nextValColumn = new Column(nextValColName, Long.class);
		}
		
		public Map<PropertyAccessor, Column> getPooledSequenceFieldMapping() {
			return Maps.asMap((PropertyAccessor) PooledSequence.SEQUENCE_NAME_FIELD, sequenceNameColumn)
						.add(PooledSequence.UPPER_BOUND_FIELD, nextValColumn);
		}
	}
	
	/**
	 * POJO which represents a line in the sequence table
	 */
	public static class PooledSequence {
		
		private static final PropertyAccessor<PooledSequence, String> SEQUENCE_NAME_FIELD;
		private static final PropertyAccessor<PooledSequence, Long> UPPER_BOUND_FIELD;
		
		
		static {
			SEQUENCE_NAME_FIELD = PropertyAccessor.forProperty(PooledSequence.class, "sequenceName");
			UPPER_BOUND_FIELD = PropertyAccessor.forProperty(PooledSequence.class, "upperBound");
		}
		
		private String sequenceName;
		private long upperBound;
		
		private PooledSequence() {
		}
		
		public PooledSequence(String sequenceName) {
			this.sequenceName = sequenceName;
		}
		
		public long getUpperBound() {
			return upperBound;
		}
		
		public void setUpperBound(long upperBound) {
			this.upperBound = upperBound;
		}
		
		public String getSequenceName() {
			return sequenceName;
		}
	}
	
	/**
	 * Class aimed at executing update operations of PooledSequences.
	 */
	private class SequenceBoundJdbcOperation implements SeparateTransactionExecutor.JdbcOperation {
		private final String sequenceName;
		private final int poolSize;
		private PooledSequence pool;
		
		public SequenceBoundJdbcOperation(String sequenceName, int poolSize) {
			this.sequenceName = sequenceName;
			this.poolSize = poolSize;
		}
		
		@Override
		public void execute() {
			pool = readBound(sequenceName);
			if (pool != null) {
				pool.setUpperBound(pool.getUpperBound() + poolSize);
				updateRoughly(pool);
			} else {
				pool = new PooledSequence(sequenceName);
				pool.setUpperBound(poolSize);
				insert(pool);
			}
		}
		
		public long getUpperBound() {
			return pool.getUpperBound();
		}
	}
	
	private static class PooledSequencePersisterConfigurer {
		
		private ClassMappingStrategy<PooledSequence, String> buildConfiguration(PooledSequencePersistenceOptions storageOptions) {
			// Sequence table creation
			SequenceTable sequenceTable = new SequenceTable(null, storageOptions.getTable(), storageOptions.getSequenceNameColumn(), storageOptions.getValueColumn());
			// Strategy building
			// NB: no id generator here because we manage ids (see reservePool)
			return new ClassMappingStrategy<>(PooledSequence.class,
					sequenceTable,
					sequenceTable.getPooledSequenceFieldMapping(),
					PooledSequence.SEQUENCE_NAME_FIELD,
					AlreadyAssignedIdentifierManager.INSTANCE);
		}
	}
}
