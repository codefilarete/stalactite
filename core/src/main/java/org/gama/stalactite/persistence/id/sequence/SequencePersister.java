package org.gama.stalactite.persistence.id.sequence;

import java.util.Map;

import org.gama.lang.collection.Maps;
import org.gama.reflection.Accessors;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.sequence.SequencePersister.Sequence;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Database.Schema;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Persister dedicated to {@link Sequence}.
 * 
 * The same instance can be shared, as long as each calls {@link #reservePool(String, int)} with a different name parameter to avoid
 * sequence name collision.
 * 
 * @author Guillaume Mary
 */
public class SequencePersister extends Persister<Sequence, String> {
	
	/**
	 * Constructor with default table and column names.
	 * @param dialect the {@link Dialect} to use for database dialog
	 * @param separateTransactionExecutor a transaction provider that mmust give a new and separate transaction
	 * @param jdbcBatchSize the JDBC batch size, not really usefull for this class since it doesn't do massive insert
	 * @see SequenceStorageOptions#DEFAULT
	 */
	public SequencePersister(Dialect dialect, SeparateTransactionExecutor separateTransactionExecutor, int jdbcBatchSize) {
		this(SequenceStorageOptions.DEFAULT, dialect, separateTransactionExecutor, jdbcBatchSize);
	}
	
	public SequencePersister(SequenceStorageOptions storageOptions, Dialect dialect, SeparateTransactionExecutor separateTransactionExecutor, int jdbcBatchSize) {
		// we reuse default PersistentContext
		super(new SequencePersisterConfigurer().buildConfiguration(storageOptions),
				dialect, separateTransactionExecutor, jdbcBatchSize);
	}
	
	public long reservePool(String sequenceName, int poolSize) {
		SequenceBoundJdbcOperation jdbcOperation = new SequenceBoundJdbcOperation(sequenceName, poolSize);
		// the operation is executed in a new and parallel transaction in order to manage concurrent accesses
		((SeparateTransactionExecutor) getConnectionProvider()).executeInNewTransaction(jdbcOperation);
		return jdbcOperation.getUpperBound();
	}
	
	private Sequence readStep(String sequenceName) {
		return select(sequenceName);
	}
	
	private static class SequenceTable extends Table {
		
		private final Column nextValColumn;
		private final Column sequenceNameColumn;
		
		public SequenceTable(Schema schema, String name, String sequenceNameColName, String nextValColName) {
			super(schema, name);
			sequenceNameColumn = addColumn(sequenceNameColName, String.class);
			sequenceNameColumn.setPrimaryKey(true);
			nextValColumn = addColumn(nextValColName, Long.class);
		}
		
		public Map<PropertyAccessor, Column> getPooledSequenceFieldMapping() {
			return Maps.asMap((PropertyAccessor) Sequence.SEQUENCE_NAME_FIELD, sequenceNameColumn)
						.add(Sequence.VALUE_FIELD, nextValColumn);
		}
	}
	
	/**
	 * POJO which represents a line in the sequence table which is composed of 2 columns: one for the name of the sequence, the second for its value.
	 */
	public static class Sequence {
		
		private static final PropertyAccessor<Sequence, String> SEQUENCE_NAME_FIELD;
		private static final PropertyAccessor<Sequence, Long> VALUE_FIELD;
		
		
		static {
			SEQUENCE_NAME_FIELD = Accessors.forProperty(Sequence.class, "sequenceName");
			VALUE_FIELD = Accessors.forProperty(Sequence.class, "step");
		}
		
		private String sequenceName;
		private long step;
		
		private Sequence() {
		}
		
		public Sequence(String sequenceName) {
			this.sequenceName = sequenceName;
		}
		
		public long getStep() {
			return step;
		}
		
		public void setStep(long step) {
			this.step = step;
		}
		
		public String getSequenceName() {
			return sequenceName;
		}
	}
	
	/**
	 * Class aimed at executing update operations of {@link Sequence}.
	 */
	private class SequenceBoundJdbcOperation implements SeparateTransactionExecutor.JdbcOperation {
		private final String sequenceName;
		private final int stepSize;
		private Sequence sequence;
		
		public SequenceBoundJdbcOperation(String sequenceName, int nextStepSize) {
			this.sequenceName = sequenceName;
			this.stepSize = nextStepSize;
		}
		
		@Override
		public void execute() {
			sequence = readStep(sequenceName);
			if (sequence != null) {
				sequence.setStep(sequence.getStep() + stepSize);
				updateById(sequence);
			} else {
				sequence = new Sequence(sequenceName);
				sequence.setStep(stepSize);
				insert(sequence);
			}
		}
		
		public long getUpperBound() {
			return sequence.getStep();
		}
	}
	
	private static class SequencePersisterConfigurer {
		
		private ClassMappingStrategy<Sequence, String> buildConfiguration(SequenceStorageOptions storageOptions) {
			// Sequence table creation
			SequenceTable sequenceTable = new SequenceTable(null, storageOptions.getTable(), storageOptions.getSequenceNameColumn(), storageOptions.getValueColumn());
			// Strategy building
			// NB: no id generator here because we manage ids (see reservePool)
			return new ClassMappingStrategy<>(Sequence.class,
					sequenceTable,
					sequenceTable.getPooledSequenceFieldMapping(),
					Sequence.SEQUENCE_NAME_FIELD,
					new AlreadyAssignedIdentifierManager(String.class));
		}
	}
}
