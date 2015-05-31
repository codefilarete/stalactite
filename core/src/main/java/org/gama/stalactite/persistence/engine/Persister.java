package org.gama.stalactite.persistence.engine;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.SteppingIterator;
import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.lang.collection.ValueFactoryMap;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.Logger;
import org.gama.stalactite.persistence.engine.listening.IDeleteListener;
import org.gama.stalactite.persistence.engine.listening.IInsertListener;
import org.gama.stalactite.persistence.engine.listening.ISelectListener;
import org.gama.stalactite.persistence.engine.listening.IUpdateListener;
import org.gama.stalactite.persistence.engine.listening.IUpdateRouglyListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.id.AutoAssignedIdentifierGenerator;
import org.gama.stalactite.persistence.id.IdentifierGenerator;
import org.gama.stalactite.persistence.id.PostInsertIdentifierGenerator;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.StatementValues;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.sql.dml.DeleteOperation;
import org.gama.stalactite.persistence.sql.dml.InsertOperation;
import org.gama.stalactite.persistence.sql.dml.SelectOperation;
import org.gama.stalactite.persistence.sql.dml.UpdateOperation;
import org.gama.stalactite.persistence.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.sql.result.RowIterator;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * CRUD Persistent features dedicated to an entity class. Entry point for persistent operations on entities.
 * 
 * @author Guillaume Mary
 */
public class Persister<T> {
	
	private static Logger LOGGER = Logger.getLogger(Persister.class);
	
	private PersistenceContext persistenceContext;
	private DMLGenerator dmlGenerator;
	private ClassMappingStrategy<T> mappingStrategy;
	private int batchSize;
	private IIdentifierFixer<T> identifierFixer;
	private PersisterListener<T> persisterListener = new PersisterListener<>();
	
	public Persister(PersistenceContext persistenceContext, ClassMappingStrategy<T> mappingStrategy) {
		this.persistenceContext = persistenceContext;
		setMappingStrategy(mappingStrategy);
		this.dmlGenerator = new DMLGenerator();
		this.batchSize = persistenceContext.getJDBCBatchSize();
	}
	
	public PersistenceContext getPersistenceContext() {
		return persistenceContext;
	}
	
	/**
	 * Return JDBC batch size to use. Initially took from PersistentContext.
	 * @return the JDBC batch size to use
	 */
	public int getBatchSize() {
		return batchSize;
	}
	
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	
	/**
	 * Overriden by subclasses that can't pass strategy as constructor arg
	 * @param mappingStrategy a ClassMappingStrategy
	 */
	protected void setMappingStrategy(ClassMappingStrategy<T> mappingStrategy) {
		this.mappingStrategy = mappingStrategy;
		configureIdentifierFixer();
	}
	
	protected void configureIdentifierFixer() {
		// preparing identifier instance 
		if (mappingStrategy != null) {
			IdentifierGenerator identifierGenerator = this.mappingStrategy.getIdentifierGenerator();
			if (!(identifierGenerator instanceof AutoAssignedIdentifierGenerator)
					&& !(identifierGenerator instanceof PostInsertIdentifierGenerator)) {
				this.identifierFixer = new IdentifierFixer(identifierGenerator);
			} else {
				this.identifierFixer = NoopIdentifierFixer.INSTANCE;
			}
		} // else // rare cases but necessary
	}
	
	public void setPersisterListener(PersisterListener<T> persisterListener) {
		// prevent from null instance
		if (persisterListener != null) {
			this.persisterListener = persisterListener;
		}
	}
	
	/** should never be null for simplicity and performance (skip "if" on each CRUD method) */ /**
	 * 
	 * @return not null
	 */
	public PersisterListener<T> getPersisterListener() {
		return persisterListener;
	}
	
	public void persist(T t) {
		// determine insert or update operation
		if (isNew(t)) {
			insert(t);
		} else {
			updateRoughly(t);
		}
	}
	
	public void persist(Iterable<T> iterable) {
		// determine insert or update operation
		List<T> toInsert = new ArrayList<>(20);
		List<T> toUpdate = new ArrayList<>(20);
		for (T t : iterable) {
			if (isNew(t)) {
				toInsert.add(t);
			} else {
				toUpdate.add(t);
			}
		}
		if (!toInsert.isEmpty()) {
			insert(toInsert);
		}
		if (!toUpdate.isEmpty()) {
			updateRoughly(toUpdate);
		}
	}
	
	public void insert(T t) {
		insert(Collections.singletonList(t));
	}
	
	public void insert(Iterable<T> iterable) {
		IInsertListener<T> insertListener = getPersisterListener().getInsertListener();
		insertListener.beforeInsert(iterable);
		final InsertOperation insertOperation = dmlGenerator.buildInsert(mappingStrategy.getTargetTable().getColumns());
		List<StatementValues> statementValues = new ArrayList<>();
		for (T t : iterable) {
			identifierFixer.fixId(t);
			statementValues.add(mappingStrategy.getInsertValues(t));
		}
		applyValuesToOperation(insertOperation, statementValues);
		/*
		if (identifierGenerator instanceof PostInsertIdentifierGenerator) {
			// TODO: lire le résultat de l'exécution et injecter l'identifiant sur le bean
		}
		*/
		insertListener.afterInsert(iterable);
	}
	
	public void updateRoughly(T t) {
		updateRoughly(Collections.singletonList(t));
	}
	
	/**
	 * Update roughly some instances: no difference are computed, only update statements (full column) are applied.
	 * @param iterable iterable of instances
	 */
	public void updateRoughly(Iterable<T> iterable) {
		IUpdateRouglyListener<T> updateRouglyListener = getPersisterListener().getUpdateRouglyListener();
		updateRouglyListener.beforeUpdateRoughly(iterable);
		Table targetTable = mappingStrategy.getTargetTable();
		// we never update primary key (by principle and for persistent bean cache based on id (on what else ?)) 
		Set<Column> columnsToUpdate = targetTable.getColumnsNoPrimaryKey();
		final UpdateOperation updateOperation = dmlGenerator.buildUpdate(columnsToUpdate, mappingStrategy.getVersionedKeys());
		List<StatementValues> statementValues = new ArrayList<>();
		for (T t : iterable) {
			statementValues.add(mappingStrategy.getUpdateValues(t, null, true));
		}
		applyValuesToOperation(updateOperation, statementValues);
		updateRouglyListener.afterUpdateRoughly(iterable);
	}
	
	public void update(T modified, T unmodified, boolean allColumnsStatement) {
		update(Collections.singletonList((Entry<T, T>) new HashMap.SimpleImmutableEntry<>(modified, unmodified)), allColumnsStatement);
	}
	
	/**
	 * Update instances that has changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 * 
	 * @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be..
	 *                            Pass true gives more chance for JDBC batch to be used. 
	 */
	public void update(Iterable<Entry<T, T>> differencesIterable, boolean allColumnsStatement) {
		IUpdateListener<T> updateListener = getPersisterListener().getUpdateListener();
		updateListener.beforeUpdate(differencesIterable);
		// cache for UpdateOperation instances : key is Columns to be updated
		Map<Set<Column>, UpdateOperation> updateOperationCache = new ValueFactoryHashMap<Set<Column>, UpdateOperation>() {
			@Override
			public UpdateOperation createInstance(Set<Column> input) {
				return dmlGenerator.buildUpdate(input, mappingStrategy.getVersionedKeys());
			}
		};
		// UpdateOperations and values to apply
		// NB: LinkedHashMap is used to keep order of treatment, not a huge requirement, rather for simplicity and unit test assert
		LinkedHashMap<UpdateOperation, List<StatementValues>> delegateStorage = new LinkedHashMap<>(50);
		Map<UpdateOperation, List<StatementValues>> operationsToApply = new ValueFactoryMap<UpdateOperation, List<StatementValues>>(delegateStorage) {
			@Override
			public List<StatementValues> createInstance(UpdateOperation input) {
				return new ArrayList<>();
			}
		};
		
		// building UpdateOperations and update values
		for (Entry<T, T> next : differencesIterable) {
			T modified = next.getKey();
			T unmodified = next.getValue();
			// finding differences between modified instance and unmodified one
			StatementValues updateValues = mappingStrategy.getUpdateValues(modified, unmodified, allColumnsStatement);
			Set<Column> columnsToUpdate = updateValues.getUpsertValues().keySet();
			if (!columnsToUpdate.isEmpty()) {
				UpdateOperation updateOperation = updateOperationCache.get(columnsToUpdate);
				operationsToApply.get(updateOperation).add(updateValues);
			} // else nothing to do (no modification)
		}
		
		// applying all update operations
		for (Entry<UpdateOperation, List<StatementValues>> updateOperationEntry : operationsToApply.entrySet()) {
			applyValuesToOperation(updateOperationEntry.getKey(), updateOperationEntry.getValue());
		}
		updateListener.afterUpdate(differencesIterable);
	}
	
	public void delete(T t) {
		delete(Collections.singletonList(t));
	}
	
	public void delete(Iterable<T> iterable) {
		IDeleteListener<T> deleteListener = getPersisterListener().getDeleteListener();
		deleteListener.beforeDelete(iterable);
		Table targetTable = mappingStrategy.getTargetTable();
		final DeleteOperation deleteOperation = dmlGenerator.buildDelete(targetTable, mappingStrategy.getVersionedKeys());
		List<StatementValues> statementValues = new ArrayList<>();
		for (T t : iterable) {
			statementValues.add(mappingStrategy.getDeleteValues(t));
		}
		applyValuesToOperation(deleteOperation, statementValues);
		deleteListener.afterDelete(iterable);
	}
	
	/**
	 * Indicates if a bean is persisted or not, implemented on null identifier 
	 * @param t a bean
	 * @return true if bean's id is null, false otherwise
	 */
	public boolean isNew(T t) {
		return mappingStrategy.getId(t) == null;
	}
	
	protected int[] execute(WriteOperation operation, StatementValues writeValues) {
		apply(operation, writeValues);
		return execute(operation);
	}
	
	protected void applyValuesToOperation(WriteOperation writeOperation, List<StatementValues> statementValues) {
		SteppingIterator<StatementValues> valuesIterator = new BatchingIterator<>(statementValues, writeOperation);
		while(valuesIterator.hasNext()) {
			StatementValues updateValues = valuesIterator.next();
			apply(writeOperation, updateValues);
		}
	}
	
	protected void apply(WriteOperation operation, StatementValues writeValues) {
		try {
			operation.apply(writeValues, getConnection());
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
		}
	}
	
	protected int[] execute(WriteOperation operation) {
		try {
			return operation.execute();
		} catch (SQLException e) {
			if (e.getMessage() != null && e.getMessage().contains("Deadlock")) {
				execute(operation);
			} else {
				Exceptions.throwAsRuntimeException(e);
			}
			return null;
		}
	}
	
	public T select(Serializable id) {
		if (id != null) {
			ISelectListener<T> selectListener = getPersisterListener().getSelectListener();
			selectListener.beforeSelect(id);
			Table targetTable = mappingStrategy.getTargetTable();
			SelectOperation selectStatement = dmlGenerator.buildSelect(targetTable,
					targetTable.getColumns().asSet(),
					Arrays.asList(targetTable.getPrimaryKey()));
			StatementValues selectValues = mappingStrategy.getSelectValues(id);
			T result = execute(selectStatement, selectValues, mappingStrategy);
			selectListener.afterSelect(result);
			return result;
		} else {
			throw new IllegalArgumentException("Non selectable entity " + mappingStrategy.getClassToPersist() + " because of null id");
		}
	}
	
	protected T execute(SelectOperation operation, StatementValues whereValues, ClassMappingStrategy<T> classMappingStrategy) {
		try {
			operation.apply(whereValues, getConnection());
			RowIterator rowIterator = operation.execute();
			if (rowIterator.hasNext()) {
				return classMappingStrategy.transform(rowIterator.next());
			} else {
				return null;
			}
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
			// unreachable code
			return null;
		}
	}
	
	public Connection getConnection() throws SQLException {
		return getPersistenceContext().getCurrentConnection();
	}
	
	private interface IIdentifierFixer<T> {
		
		void fixId(T t);
		
	}
	
	private class IdentifierFixer implements IIdentifierFixer<T> {
		
		private IdentifierGenerator identifierGenerator;
		
		private IdentifierFixer(IdentifierGenerator identifierGenerator) {
			this.identifierGenerator = identifierGenerator;
		}
		
		public void fixId(T t) {
			mappingStrategy.setId(t, this.identifierGenerator.generate());
		}
	}
	
	private static class NoopIdentifierFixer implements IIdentifierFixer {
		
		public static final NoopIdentifierFixer INSTANCE = new NoopIdentifierFixer();
		
		private NoopIdentifierFixer() {
			super();
		}
		
		@Override
		public final void fixId(Object o) {
		}
	}
	
	/**
	 * Classe qui permet d'itérer en déclenchant un batch JDBC à intervalle régulier.
	 */
	private class BatchingIterator<E> extends SteppingIterator<E> {
		private final WriteOperation writeOperation;
		
		public BatchingIterator(Iterable<E> iterable, WriteOperation writeOperation) {
			super(iterable, Persister.this.getBatchSize());
			this.writeOperation = writeOperation;
		}
		
		@Override
		protected void onStep() {
			LOGGER.debug("Triggering batch");
			execute(writeOperation);
		}
	}
}
