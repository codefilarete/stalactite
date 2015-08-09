package org.gama.stalactite.persistence.id.sequence;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.TransactionManager;
import org.gama.stalactite.persistence.engine.TransactionManager.JdbcOperation;
import org.gama.stalactite.persistence.id.AutoAssignedIdentifierGenerator;
import org.gama.stalactite.persistence.id.sequence.PooledSequencePersister.PooledSequence;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Database.Schema;
import org.gama.stalactite.persistence.structure.Table;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Persister dedicated to pooled entity identifiers.
 * The same instance can be shared by multiple IdentifierGenerator, as long as each calls {@link #reservePool(String, int)}
 * with different parameters, there's no risk of identifier collision.
 * 
 * @author Guillaume Mary
 */
public class PooledSequencePersister extends Persister<PooledSequence> {
	
	private final TransactionManager transactionManager;
	
	/**
	 * Constructor with default table and column names.
	 * @see PooledSequencePersistenceOptions#DEFAULT
	 * @param dialect
	 */
	public PooledSequencePersister(Dialect dialect, TransactionManager transactionManager, int jdbcBatchSize) {
		this(PooledSequencePersistenceOptions.DEFAULT, dialect, transactionManager, jdbcBatchSize);
	}
	
	public PooledSequencePersister(PooledSequencePersistenceOptions storageOptions, Dialect dialect, TransactionManager transactionManager, int jdbcBatchSize) {
		// we reuse default PersistentContext
		super(new PooledSequencePersisterConfigurer().buildConfiguration(storageOptions),
				dialect, transactionManager, jdbcBatchSize);
		this.transactionManager = transactionManager;
	}
	
	public long reservePool(String sequenceName, int poolSize) {
		SequenceBoundJdbcOperation jdbcOperation = new SequenceBoundJdbcOperation(sequenceName, poolSize);
		// the operation is executed in a new and parallel transaction in order to manage concurrent accesses
		transactionManager.executeInNewTransaction(jdbcOperation);
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
		
		public Map<Field, Column> getPooledSequenceFieldMapping() {
			return Maps.asMap(PooledSequence.SEQUENCE_NAME_FIELD, sequenceNameColumn)
						.add(PooledSequence.UPPER_BOUND_FIELD, nextValColumn);
		}
	}
	
	/**
	 * POJO which represents a line in the sequence table
	 */
	public static class PooledSequence {
		
		private static final Field SEQUENCE_NAME_FIELD;
		private static final Field UPPER_BOUND_FIELD;
		
		static {
			Map<String, Field> pooledSequenceClassFields = Reflections.mapFieldsOnName(PooledSequence.class);
			SEQUENCE_NAME_FIELD = pooledSequenceClassFields.get("sequenceName");
			UPPER_BOUND_FIELD = pooledSequenceClassFields.get("upperBound");
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
	}
	
	/**
	 * Class aimed at executing update operations of PooledSequences.
	 */
	private class SequenceBoundJdbcOperation implements JdbcOperation {
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
		
		private ClassMappingStrategy<PooledSequence> buildConfiguration(PooledSequencePersistenceOptions storageOptions) {
			// Sequence table creation
			SequenceTable sequenceTable = new SequenceTable(null, storageOptions.getTable(), storageOptions.getSequenceNameColumn(), storageOptions.getValueColumn());
			// Strategy building
			// NB: no id generator here because we manage ids (see reservePool)
			ClassMappingStrategy<PooledSequence> mappingStrategy = new ClassMappingStrategy<>(PooledSequence.class,
					sequenceTable,
					sequenceTable.getPooledSequenceFieldMapping(),
					PooledSequence.SEQUENCE_NAME_FIELD,
					new AutoAssignedIdentifierGenerator());
			return mappingStrategy;
		}
	}
}
