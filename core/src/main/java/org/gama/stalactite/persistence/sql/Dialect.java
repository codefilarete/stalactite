package org.gama.stalactite.persistence.sql;

import org.gama.lang.Retryer;
import org.gama.stalactite.persistence.sql.ddl.DDLSchemaGenerator;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;

/**
 * @author Guillaume Mary
 */
public class Dialect {
	
	private JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;
	
	private ColumnBinderRegistry columnBinderRegistry;
	/** Helper to retry write operation, for instance with MySQL deadlock exception */
	private Retryer writeOperationRetryer = Retryer.NO_RETRY;
	/** Maximum number of values for a "in" operator */
	private int inOperatorMaxSize = 1000;
	
	private DDLSchemaGenerator ddlSchemaGenerator;
	
	private DMLGenerator dmlGenerator;
	
	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this(javaTypeToSqlTypeMapping, new ColumnBinderRegistry());
	}
	
	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, ColumnBinderRegistry columnBinderRegistry) {
		this.javaTypeToSqlTypeMapping = javaTypeToSqlTypeMapping;
		this.columnBinderRegistry = columnBinderRegistry;
		this.dmlGenerator = newDmlGenerator(columnBinderRegistry);
		this.ddlSchemaGenerator = newDdlSchemaGenerator();
	}
	
	protected DDLSchemaGenerator newDdlSchemaGenerator() {
		return new DDLSchemaGenerator(getJavaTypeToSqlTypeMapping());
	}
	
	public DDLSchemaGenerator getDdlSchemaGenerator() {
		return ddlSchemaGenerator;
	}
	
	protected DMLGenerator newDmlGenerator(ColumnBinderRegistry columnBinderRegistry) {
		return new DMLGenerator(columnBinderRegistry);
	}
	
	public DMLGenerator getDmlGenerator() {
		return dmlGenerator;
	}
	
	public JavaTypeToSqlTypeMapping getJavaTypeToSqlTypeMapping() {
		return javaTypeToSqlTypeMapping;
	}
	
	public ColumnBinderRegistry getColumnBinderRegistry() {
		return columnBinderRegistry;
	}
	
	public Retryer getWriteOperationRetryer() {
		return writeOperationRetryer;
	}
	
	public void setWriteOperationRetryer(Retryer writeOperationRetryer) {
		this.writeOperationRetryer = writeOperationRetryer;
	}
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	public void setInOperatorMaxSize(int inOperatorMaxSize) {
		this.inOperatorMaxSize = inOperatorMaxSize;
	}
}
