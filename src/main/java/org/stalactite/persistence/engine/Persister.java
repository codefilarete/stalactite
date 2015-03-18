package org.stalactite.persistence.engine;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;

import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.KeepOrderSet;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.id.AutoAssignedIdentifierGenerator;
import org.stalactite.persistence.id.IdentifierGenerator;
import org.stalactite.persistence.id.PostInsertIdentifierGenerator;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.sql.dml.DMLGenerator;
import org.stalactite.persistence.sql.dml.DeleteOperation;
import org.stalactite.persistence.sql.dml.InsertOperation;
import org.stalactite.persistence.sql.dml.SelectOperation;
import org.stalactite.persistence.sql.dml.UpdateOperation;
import org.stalactite.persistence.sql.dml.WriteOperation;
import org.stalactite.persistence.sql.result.RowIterator;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class Persister<T> {
	
	private PersistenceContext persistenceContext;
	private DMLGenerator dmlGenerator;
	private ClassMappingStrategy<T> mappingStrategy;
	
	public Persister(PersistenceContext persistenceContext, ClassMappingStrategy<T> mappingStrategy) {
		this.persistenceContext = persistenceContext;
		this.mappingStrategy = mappingStrategy;
		this.dmlGenerator = new DMLGenerator();
	}
	
	public PersistenceContext getPersistenceContext() {
		return persistenceContext;
	}
	
	/**
	 * Mis en place pour les sous-classe qui ne peuvent pas passer la stratégie dans le constructeur.
	 * @param mappingStrategy une ClassMappingStrategy
	 */
	protected void setMappingStrategy(ClassMappingStrategy<T> mappingStrategy) {
		this.mappingStrategy = mappingStrategy;
	}
	
	public void persist(T t) {
		// determine insert or update operation
		if (isNew(t)) {
			insert(t);
		} else {
			update(t);
		}
	}
	
	protected void insert(T t) {
		// preparing identifier instance 
		IdentifierGenerator identifierGenerator = mappingStrategy.getIdentifierGenerator();
		if (!(identifierGenerator instanceof AutoAssignedIdentifierGenerator)
				&& !(identifierGenerator instanceof PostInsertIdentifierGenerator)) {
			Serializable identifier = identifierGenerator.generate();
			mappingStrategy.setId(t, identifier);
		}
		InsertOperation insertStatement = dmlGenerator.buildInsert(mappingStrategy.getTargetTable().getColumns());
		PersistentValues insertValues = mappingStrategy.getInsertValues(t);
		execute(insertStatement, insertValues);
		/*
		if (identifierGenerator instanceof PostInsertIdentifierGenerator) {
			// TODO: lire le résultat de l'exécution et injecter l'identifiant sur le bean
		}
		*/
	}
	
	protected void update(T t) {
		PersistentValues updateValues = mappingStrategy.getUpdateValues(t, null, true);
		Table targetTable = mappingStrategy.getTargetTable();
		KeepOrderSet<Column> columns = targetTable.getColumns();
		// we never update primary key (by principle and for persistent bean cache based on id (on what else ?)) 
		LinkedHashSet<Column> columnsToUpdate = columns.asSet();
		columnsToUpdate.remove(targetTable.getPrimaryKey());
		UpdateOperation crudOperation = dmlGenerator.buildUpdate(columnsToUpdate, updateValues.getWhereValues().keySet());
		execute(crudOperation, updateValues);
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
		try {
			operation.apply(writeValues, getConnection());
			return operation.execute();
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
			return null;
		}
	}
	
	public void delete(T t) {
		Serializable id = mappingStrategy.getId(t);
		if (id != null) {
			Table targetTable = mappingStrategy.getTargetTable();
			DeleteOperation deleteStatement = dmlGenerator.buildDelete(targetTable,
											Arrays.asList(targetTable.getPrimaryKey()));
			PersistentValues deleteValues = mappingStrategy.getDeleteValues(t);
			execute(deleteStatement, deleteValues);
		}
	}
	
	public T select(Serializable id) {
		if (id != null) {
			Table targetTable = mappingStrategy.getTargetTable();
			SelectOperation selectStatement = dmlGenerator.buildSelect(targetTable,
					targetTable.getColumns().asSet(),
					Arrays.asList(targetTable.getPrimaryKey()));
			PersistentValues selectValues = mappingStrategy.getSelectValues(id);
			return execute(selectStatement, selectValues, mappingStrategy);
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
}
