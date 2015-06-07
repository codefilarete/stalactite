package org.gama.stalactite.persistence.engine;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.SteppingIterator;
import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.lang.collection.ValueFactoryMap;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.Logger;
import org.gama.stalactite.persistence.engine.Persister.IIdentifierFixer;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.StatementValues;
import org.gama.stalactite.persistence.sql.dml.*;
import org.gama.stalactite.persistence.sql.result.RowIterator;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

/**
 * CRUD Persistent features dedicated to an entity class. Kind of sliding door of {@link Persister} aimed at running
 * actions for it.
 * 
 * @author Guillaume Mary
 */
public class PersisterExecutor<T> {
	
	private static Logger LOGGER = Logger.getLogger(PersisterExecutor.class);
	
	private final DMLGenerator dmlGenerator;
	private final ClassMappingStrategy<T> mappingStrategy;
	private final int batchSize;
	private final IIdentifierFixer<T> identifierFixer;
	private final TransactionManager transactionManager;
	
	public PersisterExecutor(ClassMappingStrategy<T> mappingStrategy, IIdentifierFixer<T> identifierFixer, int batchSize, TransactionManager transactionManager) {
		this.mappingStrategy = mappingStrategy;
		this.identifierFixer = identifierFixer;
		this.batchSize = batchSize;
		this.transactionManager = transactionManager;
		this.dmlGenerator = new DMLGenerator();
	}
	
	public ClassMappingStrategy<T> getMappingStrategy() {
		return mappingStrategy;
	}
	
	public void insert(Iterable<T> iterable) {
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
	}
	
	/**
	 * Update roughly some instances: no difference are computed, only update statements (full column) are applied.
	 * @param iterable iterable of instances
	 */
	public void updateRoughly(Iterable<T> iterable) {
		Table targetTable = mappingStrategy.getTargetTable();
		// we never update primary key (by principle and for persistent bean cache based on id (on what else ?)) 
		Set<Column> columnsToUpdate = targetTable.getColumnsNoPrimaryKey();
		final UpdateOperation updateOperation = dmlGenerator.buildUpdate(columnsToUpdate, mappingStrategy.getVersionedKeys());
		List<StatementValues> statementValues = new ArrayList<>();
		for (T t : iterable) {
			statementValues.add(mappingStrategy.getUpdateValues(t, null, true));
		}
		applyValuesToOperation(updateOperation, statementValues);
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
	}
	
	public void delete(Iterable<T> iterable) {
		Table targetTable = mappingStrategy.getTargetTable();
		final DeleteOperation deleteOperation = dmlGenerator.buildDelete(targetTable, mappingStrategy.getVersionedKeys());
		List<StatementValues> statementValues = new ArrayList<>();
		for (T t : iterable) {
			statementValues.add(mappingStrategy.getDeleteValues(t));
		}
		applyValuesToOperation(deleteOperation, statementValues);
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
			operation.apply(writeValues, transactionManager.getCurrentConnection());
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
	
	public List<T> select(Iterable<Serializable> ids) {
		Table targetTable = mappingStrategy.getTargetTable();
		SelectOperation selectStatement = dmlGenerator.buildSelect(targetTable,
				targetTable.getColumns().asSet(),
				Arrays.asList(targetTable.getPrimaryKey()));
		List<T> toReturn = new ArrayList<>(50);
		for (Serializable id : ids) {
			StatementValues selectValues = mappingStrategy.getSelectValues(id);
			toReturn.add(execute(selectStatement, selectValues, mappingStrategy));
		}
		return toReturn;
	}
	
	protected T execute(SelectOperation operation, StatementValues whereValues, ClassMappingStrategy<T> classMappingStrategy) {
		try {
			operation.apply(whereValues, transactionManager.getCurrentConnection());
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
	
	/**
	 * Class that executes JDBC write operation in a stepped way
	 */
	private class BatchingIterator<E> extends SteppingIterator<E> {
		private final WriteOperation writeOperation;
		
		public BatchingIterator(Iterable<E> iterable, WriteOperation writeOperation) {
			super(iterable, PersisterExecutor.this.batchSize);
			this.writeOperation = writeOperation;
		}
		
		@Override
		protected void onStep() {
			LOGGER.debug("Triggering batch");
			execute(writeOperation);
		}
	}
}
