package org.codefilarete.stalactite.persistence.mapping.id.sequence;

import java.util.Map;

import org.codefilarete.stalactite.persistence.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.persistence.engine.runtime.Persister;
import org.codefilarete.stalactite.persistence.mapping.id.sequence.SequencePersister.Sequence;
import org.codefilarete.stalactite.persistence.mapping.id.sequence.SequencePersister.SequenceTable;
import org.codefilarete.stalactite.persistence.mapping.ClassMappingStrategy;
import org.codefilarete.stalactite.persistence.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;

/**
 * Persister dedicated to {@link Sequence}.
 * 
 * The same instance can be shared, as long as each calls {@link #reservePool(String, int)} with a different name parameter to avoid
 * sequence name collision.
 * 
 * @author Guillaume Mary
 */
public class SequencePersister extends Persister<Sequence, String, SequenceTable> {
	
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
				dialect, new ConnectionConfigurationSupport(separateTransactionExecutor, jdbcBatchSize));
	}
	
	public long reservePool(String sequenceName, int poolSize) {
		SequenceBoundJdbcOperation jdbcOperation = new SequenceBoundJdbcOperation(sequenceName, poolSize);
		// the operation is executed in a new and parallel transaction in order to manage concurrent accesses
		((SeparateTransactionExecutor) getConnectionProvider()).executeInNewTransaction(jdbcOperation);
		return jdbcOperation.getUpperBound();
	}
	
	public static class SequenceTable extends Table<SequenceTable> {
		
		private final Column<SequenceTable, Long> nextValColumn;
		private final Column<SequenceTable, String> sequenceNameColumn;
		
		public SequenceTable(Schema schema, String name, String sequenceNameColName, String nextValColName) {
			super(schema, name);
			sequenceNameColumn = addColumn(sequenceNameColName, String.class);
			sequenceNameColumn.setPrimaryKey(true);
			nextValColumn = addColumn(nextValColName, long.class);
		}
		
		public Map<PropertyAccessor<Sequence, Object>, Column<SequenceTable, Object>> getPooledSequenceFieldMapping() {
			return (Map) Maps
					.forHashMap(PropertyAccessor.class, Column.class)
					.add(Sequence.SEQUENCE_NAME_FIELD, sequenceNameColumn)
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
			SEQUENCE_NAME_FIELD = PropertyAccessor.fromMethodReference(Sequence::getSequenceName, Sequence::setSequenceName);
			VALUE_FIELD = PropertyAccessor.fromMethodReference(Sequence::getStep, Sequence::setStep);
		}
		
		private String sequenceName;
		private long step;
		
		private Sequence() {
		}
		
		public Sequence(String sequenceName) {
			this.sequenceName = sequenceName;
		}
		
		public Sequence(String sequenceName, long step) {
			this.sequenceName = sequenceName;
			this.step = step;
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
		
		public void setSequenceName(String sequenceName) {
			this.sequenceName = sequenceName;
		}
	}
	
	/**
	 * Class aimed at executing update operations of {@link Sequence}.
	 */
	private class SequenceBoundJdbcOperation implements SeparateTransactionExecutor.JdbcOperation {
		private final String sequenceName;
		private final int stepSize;
		private Sequence sequence;
		
		private SequenceBoundJdbcOperation(String sequenceName, int nextStepSize) {
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
		
		private Sequence readStep(String sequenceName) {
			return select(sequenceName);
		}
		
		public long getUpperBound() {
			return sequence.getStep();
		}
	}
	
	private static class SequencePersisterConfigurer {
		
		private ClassMappingStrategy<Sequence, String, SequenceTable> buildConfiguration(SequenceStorageOptions storageOptions) {
			// Sequence table creation
			SequenceTable sequenceTable = new SequenceTable(null, storageOptions.getTable(), storageOptions.getSequenceNameColumn(), storageOptions.getValueColumn());
			// Strategy building
			// NB: no id generator here because we manage ids (see reservePool)
			return new ClassMappingStrategy<>(
					Sequence.class,
					sequenceTable,
					sequenceTable.getPooledSequenceFieldMapping(),
					Sequence.SEQUENCE_NAME_FIELD,
					// Setting a "Noop" identifier manager because we use a insert(..), updateById(..) and select(..) for simple case
					// (no complex graph nor chain of code on persisted instance)
					new AlreadyAssignedIdentifierManager<>(String.class, c -> {}, c -> true));
		}
	}
}
