package org.gama.stalactite.persistence.sql;

import org.gama.lang.Retryer;
import org.gama.lang.bean.Objects;
import org.gama.stalactite.persistence.sql.ddl.DDLGenerator;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;

/**
 * Class that keeps objects necessary for "communication" with a Database at the SQL language level:
 * - column types for their creation: {@link JavaTypeToSqlTypeMapping} 
 * - column types for their read and write in {@link java.sql.PreparedStatement} and {@link java.sql.ResultSet}: {@link ColumnBinderRegistry}
 * - engines for SQL generation: {@link DDLGenerator} and {@link DMLGenerator}
 * 
 * @author Guillaume Mary
 */
public class Dialect {
	
	private JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;
	
	private ColumnBinderRegistry columnBinderRegistry;
	/** Helper to retry write operation, for instance with MySQL deadlock exception */
	private Retryer writeOperationRetryer = Retryer.NO_RETRY;
	/** Maximum number of values for a "in" operator */
	private int inOperatorMaxSize = 1000;
	
	private DDLTableGenerator ddlTableGenerator;
	
	private DMLGenerator dmlGenerator;
	
	/**
	 * Creates a default dialect, with a {@link DefaultTypeMapping} and a default {@link ColumnBinderRegistry}
	 */
	public Dialect() {
		this(new DefaultTypeMapping());
	}
	
	/**
	 * Creates a default dialect, with a default {@link ColumnBinderRegistry}
	 */
	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this(javaTypeToSqlTypeMapping, new ColumnBinderRegistry());
	}
	
	/**
	 * Creates a given {@link JavaTypeToSqlTypeMapping} and {@link ColumnBinderRegistry}
	 */
	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, ColumnBinderRegistry columnBinderRegistry) {
		this.javaTypeToSqlTypeMapping = javaTypeToSqlTypeMapping;
		this.columnBinderRegistry = columnBinderRegistry;
		this.dmlGenerator = newDmlGenerator(columnBinderRegistry);
		this.ddlTableGenerator = newDdlTableGenerator();
	}
	
	protected DDLTableGenerator newDdlTableGenerator() {
		return new DDLTableGenerator(getJavaTypeToSqlTypeMapping());
	}
	
	public DDLTableGenerator getDdlTableGenerator() {
		return ddlTableGenerator;
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
		this.writeOperationRetryer = Objects.preventNull(writeOperationRetryer, Retryer.NO_RETRY);
	}
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	public void setInOperatorMaxSize(int inOperatorMaxSize) {
		if (inOperatorMaxSize <= 0) {
			throw new IllegalArgumentException("SQL operator 'in' must contain at least 1 element");
		}
		this.inOperatorMaxSize = inOperatorMaxSize;
	}
}
