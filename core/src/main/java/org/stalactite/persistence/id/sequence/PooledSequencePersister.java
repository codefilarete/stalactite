package org.stalactite.persistence.id.sequence;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Maps;
import org.stalactite.persistence.engine.DDLDeployer;
import org.stalactite.persistence.engine.PersistenceContext;
import org.stalactite.persistence.engine.Persister;
import org.stalactite.persistence.engine.TransactionManager.JdbcOperation;
import org.stalactite.persistence.id.AutoAssignedIdentifierGenerator;
import org.stalactite.persistence.id.sequence.PooledSequencePersister.PooledSequence;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.sql.ddl.DDLParticipant;
import org.stalactite.persistence.structure.Database.Schema;
import org.stalactite.persistence.structure.Table;

/**
 * Persister dédié aux réservoirs d'identifiant par entité.
 * La même instance peut être partagé par plusieurs IdentifierGenerator du moment que chacun appelle {@link #reservePool(String, int)}
 * avec des paramètres différents, il n'y a pas de risque de collision.
 * 
 * @author mary
 */
public class PooledSequencePersister extends Persister<PooledSequence> implements DDLParticipant {
	
	/**
	 * Constructeur avec noms de table et colonne par défaut
	 */
	public PooledSequencePersister() {
		this(PooledSequencePersistenceOptions.DEFAULT);
	}
	
	public PooledSequencePersister(PooledSequencePersistenceOptions storageOptions) {
		// on réutilise le context par défaut, la stratégie est mise plus bas 
		super(PersistenceContext.getCurrent(), null);
		
		// création de la table de stockage des séquences
		SequenceTable sequenceTable = new SequenceTable(null, storageOptions.getTable(), storageOptions.getSequenceNameColumn(), storageOptions.getValueColumn());
		// Construction de la stratégie
		// NB: pas de générateur d'id car c'est nous qui gérons les identifiants (cf reservePool)
		ClassMappingStrategy<PooledSequence> mappingStrategy = new ClassMappingStrategy<>(PooledSequence.class,
				sequenceTable,
				sequenceTable.getPooledSequenceFieldMapping(),
				new AutoAssignedIdentifierGenerator());
		setMappingStrategy(mappingStrategy);
		getPersistenceContext().add(mappingStrategy);
	}
	
	@Override
	public List<String> getCreationScripts() {
		DDLDeployer ddlDeployer = new DDLDeployer(getPersistenceContext());
		return ddlDeployer.getDDLGenerator().getCreationScripts();
	}
	
	@Override
	public List<String> getDropScripts() {
		DDLDeployer ddlDeployer = new DDLDeployer(getPersistenceContext());
		return ddlDeployer.getDDLGenerator().getDropScripts();
	}
	
	public long reservePool(String sequenceName, int poolSize) {
		SequenceBoundJdbcOperation jdbcOperation = new SequenceBoundJdbcOperation(sequenceName, poolSize);
		// on exécute l'opération dans une nouvelle et commitante transaction afin de gérer les accès concurrents
		getPersistenceContext().executeInNewTransaction(jdbcOperation);
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
	 * POJO qui représente une ligne de la table des séquences
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
	 * Classe qui concentre les opérations de mise à jour des PooledSequences.
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
				update(pool);
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
}
