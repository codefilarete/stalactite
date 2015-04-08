package org.stalactite.persistence.engine;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.stalactite.Logger;
import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.SteppingIterator;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.id.AutoAssignedIdentifierGenerator;
import org.stalactite.persistence.id.IdentifierGenerator;
import org.stalactite.persistence.id.PostInsertIdentifierGenerator;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.sql.dml.*;
import org.stalactite.persistence.sql.result.RowIterator;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class Persister<T> {
	
	private static Logger LOGGER = Logger.getLogger(Persister.class);
	
	private PersistenceContext persistenceContext;
	private DMLGenerator dmlGenerator;
	private ClassMappingStrategy<T> mappingStrategy;
	private int batchSize;
	private IIdentifierFixer<T> identifierFixer;
	/** should never be null */
	private IPersisterListener<T> persisterListener = new NoopPersisterListener<>();
	
	public Persister(PersistenceContext persistenceContext, ClassMappingStrategy<T> mappingStrategy) {
		this.persistenceContext = persistenceContext;
		setMappingStrategy(mappingStrategy);
		this.dmlGenerator = new DMLGenerator();
		this.batchSize = persistenceContext.getJDBCBatchSize();
	}
	
	public PersistenceContext getPersistenceContext() {
		return persistenceContext;
	}
	
	public int getBatchSize() {
		return batchSize;
	}
	
	/**
	 * Mis en place pour les sous-classe qui ne peuvent pas passer la stratégie dans le constructeur.
	 * @param mappingStrategy une ClassMappingStrategy
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
	
	public void setPersisterListener(IPersisterListener<T> persisterListener) {
		// prevent from null instance
		if (persisterListener == null) {
			persisterListener = new NoopPersisterListener<>();
		}
		this.persisterListener = persisterListener;
	}
	
	public void persist(T t) {
		// determine insert or update operation
		if (isNew(t)) {
			insert(t);
		} else {
			update(t);
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
			update(toUpdate);
		}
	}
	
	public void insert(T t) {
		insert(Collections.singletonList(t));
	}
	
	public void insert(Iterable<T> iterable) {
		persisterListener.beforeInsert(iterable);
		final InsertOperation insertOperation = dmlGenerator.buildInsert(mappingStrategy.getTargetTable().getColumns());
		SteppingIterator<T> objectsIterator = new BatchingIterator<>(iterable, insertOperation);
		while(objectsIterator.hasNext()) {
			T t = objectsIterator.next();
			identifierFixer.fixId(t);
			PersistentValues insertValues = mappingStrategy.getInsertValues(t);
			apply(insertOperation, insertValues);
		}
		/*
		if (identifierGenerator instanceof PostInsertIdentifierGenerator) {
			// TODO: lire le résultat de l'exécution et injecter l'identifiant sur le bean
		}
		*/
		persisterListener.afterInsert(iterable);
	}
	
	public void update(T t) {
		update(Collections.singletonList(t));
	}
	
	public void update(Iterable<T> iterable) {
		persisterListener.beforeUpdate(iterable);
		Table targetTable = mappingStrategy.getTargetTable();
		// we never update primary key (by principle and for persistent bean cache based on id (on what else ?)) 
		Set<Column> columnsToUpdate = targetTable.getColumnsNoPrimaryKey();
		final UpdateOperation updateOperation = dmlGenerator.buildUpdate(columnsToUpdate, mappingStrategy.getVersionedKeys());
		SteppingIterator<T> objectsIterator = new BatchingIterator<>(iterable, updateOperation);
		while(objectsIterator.hasNext()) {
			T t = objectsIterator.next();
			PersistentValues updateValues = mappingStrategy.getUpdateValues(t, null, true);
			apply(updateOperation, updateValues);
		}
		persisterListener.afterUpdate(iterable);
	}
	

	public void delete(T t) {
		delete(Collections.singletonList(t));
	}
	
	public void delete(Iterable<T> iterable) {
		persisterListener.beforeDelete(iterable);
		Table targetTable = mappingStrategy.getTargetTable();
		final DeleteOperation deleteOperation = dmlGenerator.buildDelete(targetTable, mappingStrategy.getVersionedKeys());
		SteppingIterator<T> objectsIterator = new BatchingIterator<>(iterable, deleteOperation);
		while(objectsIterator.hasNext()) {
			T t = objectsIterator.next();
			PersistentValues id = mappingStrategy.getVersionedKeyValues(t);
			if (!id.getWhereValues().isEmpty()) {
				PersistentValues deleteValues = mappingStrategy.getDeleteValues(t);
				apply(deleteOperation, deleteValues);
			}
		}
		persisterListener.afterDelete(iterable);
	}
	
	/**
	 * Indicates if a bean is persisted or not, implemented on null identifier 
	 * @param t a bean
	 * @return true if bean's id is null, false otherwise
	 */
	public boolean isNew(T t) {
		return mappingStrategy.getId(t) == null;
	}
	
	protected int[] execute(WriteOperation operation, PersistentValues writeValues) {
		apply(operation, writeValues);
		return execute(operation);
	}
	
	protected void apply(WriteOperation operation, PersistentValues writeValues) {
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
			persisterListener.beforeSelect(id);
			Table targetTable = mappingStrategy.getTargetTable();
			SelectOperation selectStatement = dmlGenerator.buildSelect(targetTable,
					targetTable.getColumns().asSet(),
					Arrays.asList(targetTable.getPrimaryKey()));
			PersistentValues selectValues = mappingStrategy.getSelectValues(id);
			T result = execute(selectStatement, selectValues, mappingStrategy);
			persisterListener.afterSelect(result);
			return result;
		} else {
			throw new IllegalArgumentException("Non selectable entity " + mappingStrategy.getClassToPersist() + " because of null id");
		}
	}
	
	protected T execute(SelectOperation operation, PersistentValues whereValues, ClassMappingStrategy<T> classMappingStrategy) {
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
