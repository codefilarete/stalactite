package org.stalactite.persistence.engine;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;

import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.KeepOrderSet;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.sql.dml.DMLGenerator;
import org.stalactite.persistence.sql.dml.DeleteOperation;
import org.stalactite.persistence.sql.dml.InsertOperation;
import org.stalactite.persistence.sql.dml.SelectOperation;
import org.stalactite.persistence.sql.dml.UpdateOperation;
import org.stalactite.persistence.sql.dml.WriteOperation;
import org.stalactite.persistence.sql.result.RowIterator;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class Persister<T> {
	
	private PersistenceContext persistenceContext;
	
	private final DMLGenerator dmlGenerator;
	
	public Persister(PersistenceContext persistenceContext) {
		this.persistenceContext = persistenceContext;
		this.dmlGenerator = new DMLGenerator();
	}
	
	public PersistenceContext getPersistenceContext() {
		return persistenceContext;
	}
	
	public void persist(T t) {
		ClassMappingStrategy<T> mappingStrategy = ensureMappedClass((Class<T>) t.getClass());
		// determine insert or update statement
		Serializable id = mappingStrategy.getId(t);
		if (id == null) {
			if (!mappingStrategy.isIdGivenByDatabase()) {
				mappingStrategy.fixId(t);
			}
			InsertOperation insertStatement = dmlGenerator.buildInsert(mappingStrategy.getTargetTable().getColumns());
			PersistentValues insertValues = mappingStrategy.getInsertValues(t);
			execute(insertStatement, insertValues);
		} else {
			// update
			PersistentValues updateValues = mappingStrategy.getUpdateValues(t, null, true);
			KeepOrderSet<Column> columns = mappingStrategy.getTargetTable().getColumns();
			// we shouldn't update pirmary key
			LinkedHashSet<Column> columnsToUpdate = columns.asSet();
			columnsToUpdate.remove(mappingStrategy.getTargetTable().getPrimaryKey());
			UpdateOperation crudOperation = dmlGenerator.buildUpdate(columnsToUpdate, updateValues.getWhereValues().keySet());
			execute(crudOperation, updateValues);
		}
	}
	
	protected void execute(WriteOperation operation, PersistentValues writeValues) {
		try {
			operation.apply(writeValues, getConnection());
			operation.execute();
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
		}
	}
	
	public void delete(T t) {
		ClassMappingStrategy<T> mappingStrategy = ensureMappedClass((Class<T>) t.getClass());
		Serializable id = mappingStrategy.getId(t);
		if (id != null) {
			DeleteOperation deleteStatement = dmlGenerator.buildDelete(mappingStrategy.getTargetTable(),
											Arrays.asList(mappingStrategy.getTargetTable().getPrimaryKey()));
			PersistentValues deleteValues = mappingStrategy.getDeleteValues(t);
			execute(deleteStatement, deleteValues);
		}
	}
	
	public T select(Class<T> clazz, Serializable id) {
		ClassMappingStrategy<T> mappingStrategy = ensureMappedClass(clazz);
		if (id != null) {
			SelectOperation selectStatement = dmlGenerator.buildSelect(mappingStrategy.getTargetTable(),
											mappingStrategy.getTargetTable().getColumns().asSet(),
											Arrays.asList(mappingStrategy.getTargetTable().getPrimaryKey()));
			PersistentValues deleteValues = mappingStrategy.getSelectValues(id);
			return execute(selectStatement, deleteValues, mappingStrategy);
		} else {
			throw new IllegalArgumentException("Non selectable entity " + clazz + " because of null id");
		}
	}
	
	protected T execute(SelectOperation operation, PersistentValues insertValues, ClassMappingStrategy<T> classMappingStrategy) {
		try {
			operation.apply(insertValues, getConnection());
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
	
	protected ClassMappingStrategy<T> ensureMappedClass(Class<T> clazz) {
		ClassMappingStrategy<T> mappingStrategy = persistenceContext.getMappingStrategy(clazz);
		if (mappingStrategy == null) {
			throw new IllegalArgumentException("Unmapped entity " + clazz);
		} else {
			return mappingStrategy;
		}
	}
	
	
	public Connection getConnection() throws SQLException {
		return getPersistenceContext().getConnection();
	}
}
