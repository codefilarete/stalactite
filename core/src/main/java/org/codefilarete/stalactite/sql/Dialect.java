package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.sql.ddl.DDLGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;

/**
 * Class that keeps objects necessary to "communicate" with a Database at the SQL language level:
 * - column types for their creation: {@link SqlTypeRegistry} 
 * - column types for their read and write in {@link java.sql.PreparedStatement} and {@link java.sql.ResultSet}: {@link ColumnBinderRegistry}
 * - engines for SQL generation: {@link DDLGenerator} and {@link DMLGenerator}
 * 
 * @author Guillaume Mary
 */
public class Dialect {
	
	private final SqlTypeRegistry sqlTypeRegistry;
	
	private final ColumnBinderRegistry columnBinderRegistry;
	
	/** Maximum number of values for a "in" operator */
	private int inOperatorMaxSize = 1000;
	
	private final DDLTableGenerator ddlTableGenerator;
	
	private final DMLGenerator dmlGenerator;
	
	private final WriteOperationFactory writeOperationFactory;
	
	private final ReadOperationFactory readOperationFactory;
	
	private QuerySQLBuilderFactory querySQLBuilderFactory;
	
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
	 * Creates a dialect with given {@link JavaTypeToSqlTypeMapping} and {@link ColumnBinderRegistry}
	 */
	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, ColumnBinderRegistry columnBinderRegistry) {
		this.sqlTypeRegistry = new SqlTypeRegistry(javaTypeToSqlTypeMapping);
		this.columnBinderRegistry = columnBinderRegistry;
		this.dmlGenerator = newDmlGenerator(columnBinderRegistry);
		this.ddlTableGenerator = newDdlTableGenerator();
		this.writeOperationFactory = newWriteOperationFactory();
		this.readOperationFactory = newReadOperationFactory();
		this.querySQLBuilderFactory = new QuerySQLBuilderFactoryBuilder(columnBinderRegistry, javaTypeToSqlTypeMapping).build();
	}
	
	protected DDLTableGenerator newDdlTableGenerator() {
		return new DDLTableGenerator(getSqlTypeRegistry());
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
	
	protected WriteOperationFactory newWriteOperationFactory() {
		return new WriteOperationFactory();
	}
	
	public WriteOperationFactory getWriteOperationFactory() {
		return writeOperationFactory;
	}
	
	protected ReadOperationFactory newReadOperationFactory() {
		return new ReadOperationFactory();
	}
	
	public ReadOperationFactory getReadOperationFactory() {
		return readOperationFactory;
	}
	
	public QuerySQLBuilderFactory getQuerySQLBuilderFactory() {
		return querySQLBuilderFactory;
	}
	
	/**
	 * Change {@link QuerySQLBuilderFactory}
	 * One can be interested in by using {@link QuerySQLBuilderFactoryBuilder}.
	 * 
	 * @param querySQLBuilderFactory
	 */
	public void setQuerySQLBuilderFactory(QuerySQLBuilderFactory querySQLBuilderFactory) {
		this.querySQLBuilderFactory = querySQLBuilderFactory;
	}
	
	public SqlTypeRegistry getSqlTypeRegistry() {
		return sqlTypeRegistry;
	}
	
	public ColumnBinderRegistry getColumnBinderRegistry() {
		return columnBinderRegistry;
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
	
	public <I> GeneratedKeysReader<I> buildGeneratedKeysReader(String keyName, Class<I> columnType) {
		return new GeneratedKeysReader<>(keyName, getColumnBinderRegistry().getBinder(columnType));
	}
	
	/**
	 * Indicates if this dialect supports what ANSI-SQL terms "row value constructor" syntax, also called tuple syntax.
	 * Basically, does it support syntax like <pre>"... where (FIRST_NAME, LAST_NAME) = ('John', 'Doe')"</pre>.
	 *
	 * @return true if this SQL dialect supports "row value constructor" syntax, false otherwise.
	 */
	public boolean supportsTupleCondition() {
		// returning false by default since most databases don't support it
		return false;
	}
}
