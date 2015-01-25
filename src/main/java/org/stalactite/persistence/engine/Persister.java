package org.stalactite.persistence.engine;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;

import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.KeepOrderSet;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.mapping.ResultSetTransformer;
import org.stalactite.persistence.sql.ddl.DDLGenerator;
import org.stalactite.persistence.sql.dml.DMLGenerator;
import org.stalactite.persistence.sql.dml.DeleteOperation;
import org.stalactite.persistence.sql.dml.InsertOperation;
import org.stalactite.persistence.sql.dml.SelectOperation;
import org.stalactite.persistence.sql.dml.UpdateOperation;
import org.stalactite.persistence.sql.dml.WriteOperation;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class Persister<T> {
	
	private PersistenceContext persistenceContext;
	
	private final DMLGenerator dmlGenerator;
	private final DDLGenerator ddlGenerator;
	
	public Persister(PersistenceContext persistenceContext) {
		this.persistenceContext = persistenceContext;
		this.dmlGenerator = new DMLGenerator();
		this.ddlGenerator = new DDLGenerator(persistenceContext.getDialect().getJavaTypeToSqlTypeMapping());
	}
	
	public void create(Class<T> clazz) throws SQLException {
		ClassMappingStrategy<T> mappingStrategy = persistenceContext.getMappingStrategy(clazz);
		create(mappingStrategy.getTargetTable());
	}
	
	public void create(Table table) throws SQLException {
		String sqlCreateTable = this.ddlGenerator.generateCreateTable(table);
		try(Statement statement = getConnection().createStatement()) {
			statement.execute(sqlCreateTable);
		}
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
			return execute(selectStatement, deleteValues, clazz);
		} else {
			throw new IllegalArgumentException("Non selectable entity " + clazz + " because of null id");
		}
	}
	
	protected T execute(SelectOperation operation, PersistentValues insertValues, Class<T> clazz) {
		try {
			operation.apply(insertValues, getConnection());
			return operation.execute(new ResultSetTransformer<>(clazz));
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
			// unreachable code
			return null;
		} catch (NoSuchMethodException e) {
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
		return persistenceContext.getDataSource().getConnection();
	}
}
