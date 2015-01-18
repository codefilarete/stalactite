package org.stalactite.persistence.engine;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.sql.dml.CRUDStatement;
import org.stalactite.persistence.sql.dml.DMLGenerator;

/**
 * @author mary
 */
public class Persister<T> {
	
	private PersistenceContext persistenceContext;
	
	private final DMLGenerator dmlGenerator = new DMLGenerator();
	
	public Persister(PersistenceContext persistenceContext) {
		this.persistenceContext = persistenceContext;
	}
	
	public void persist(T t) {
		ClassMappingStrategy<T> mappingStrategy = persistenceContext.getMappingStrategy((Class<T>) t.getClass());
		if (mappingStrategy == null) {
			throw new IllegalArgumentException("Unmapped entity " + t.getClass());
		} else {
			// determine insert or update statement
			Serializable id = mappingStrategy.getId(t);
			if (id == null) {
				if (!mappingStrategy.isIdGivenByDatabase()) {
					mappingStrategy.fixId(t);
				}
				CRUDStatement insertStatement = dmlGenerator.buildInsert(mappingStrategy.getTargetTable().getColumns());
				PersistentValues insertValues = mappingStrategy.getInsertValues(t);
				execute(insertStatement, insertValues);
			} else {
				// update
				PersistentValues updateValues = mappingStrategy.getUpdateValues(t, null, true);
				CRUDStatement crudStatement = dmlGenerator.buildUpdate(mappingStrategy.getTargetTable().getColumns(),
						updateValues.getWhereValues().keySet());
				execute(crudStatement, updateValues);
			}
		}
	}
	
	protected void execute(CRUDStatement crudStatement, PersistentValues insertValues) {
		try {
			crudStatement.apply(insertValues, getConnection());
			crudStatement.executeWrite();
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
		}
	}
	
	public void delete(T t) {
		
	}
	
	
	public Connection getConnection() throws SQLException {
		return persistenceContext.getDataSource().getConnection();
	}
}
