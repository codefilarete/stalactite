package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelectBuilder;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.sql.ddl.DDLGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;

/**
 * Class that keeps objects necessary to "communicate" with a Database at the SQL language level:
 * - column types for their creation: {@link SqlTypeRegistry} 
 * - column types for their read and write in {@link java.sql.PreparedStatement} and {@link java.sql.ResultSet}: {@link ColumnBinderRegistry}
 * - engines for SQL generation: {@link DDLGenerator} and {@link DMLGenerator}
 * 
 * @author Guillaume Mary
 */
public class DefaultDialect implements Dialect {
	
	private final SqlTypeRegistry sqlTypeRegistry;
	
	private final ColumnBinderRegistry columnBinderRegistry;
	
	/** Maximum number of values for a "in" operator */
	private int inOperatorMaxSize = 1000;
	
	private final DDLTableGenerator ddlTableGenerator;
	
	private final DDLSequenceGenerator ddlSequenceGenerator;
	
	private final DMLGenerator dmlGenerator;
	
	private final WriteOperationFactory writeOperationFactory;
	
	private final ReadOperationFactory readOperationFactory;
	
	private QuerySQLBuilderFactory querySQLBuilderFactory;
	
	private final DMLNameProviderFactory dmlNameProviderFactory;
	
	/**
	 * Creates a default dialect, with a {@link DefaultTypeMapping} and a default {@link ColumnBinderRegistry}
	 */
	public DefaultDialect() {
		this(new DefaultTypeMapping());
	}
	
	/**
	 * Creates a default dialect, with a default {@link ColumnBinderRegistry}
	 */
	public DefaultDialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this(javaTypeToSqlTypeMapping, new ParameterBinderRegistry());
	}
	
	/**
	 * Creates a dialect with given {@link JavaTypeToSqlTypeMapping} and {@link ColumnBinderRegistry}
	 */
	public DefaultDialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, ParameterBinderRegistry parameterBinderRegistry) {
		this.sqlTypeRegistry = new SqlTypeRegistry(javaTypeToSqlTypeMapping);
		this.columnBinderRegistry = new ColumnBinderRegistry(parameterBinderRegistry);
		this.dmlNameProviderFactory = newDMLNameProviderFactory();
		this.dmlGenerator = newDmlGenerator();
		this.ddlTableGenerator = newDdlTableGenerator();
		this.ddlSequenceGenerator = newDdlSequenceGenerator();
		this.writeOperationFactory = newWriteOperationFactory();
		this.readOperationFactory = newReadOperationFactory();
		this.querySQLBuilderFactory = new QuerySQLBuilderFactoryBuilder(dmlNameProviderFactory, columnBinderRegistry, javaTypeToSqlTypeMapping).build();
	}
	
	public DefaultDialect(SqlTypeRegistry sqlTypeRegistry,
						  ColumnBinderRegistry columnBinderRegistry,
						  DDLTableGenerator ddlTableGenerator,
						  DDLSequenceGenerator ddlSequenceGenerator,
						  DMLGenerator dmlGenerator,
						  WriteOperationFactory writeOperationFactory,
						  ReadOperationFactory readOperationFactory,
						  DMLNameProviderFactory dmlNameProviderFactory) {
		this.sqlTypeRegistry = sqlTypeRegistry;
		this.columnBinderRegistry = columnBinderRegistry;
		this.ddlTableGenerator = ddlTableGenerator;
		this.ddlSequenceGenerator = ddlSequenceGenerator;
		this.dmlGenerator = dmlGenerator;
		this.writeOperationFactory = writeOperationFactory;
		this.readOperationFactory = readOperationFactory;
		this.dmlNameProviderFactory = dmlNameProviderFactory;
	}
	
	protected DMLNameProviderFactory newDMLNameProviderFactory() {
		return DMLNameProvider::new;
	}
	
	protected DMLGenerator newDmlGenerator() {
		return new DMLGenerator(getColumnBinderRegistry(), NoopSorter.INSTANCE, dmlNameProviderFactory);
	}
	
	protected DDLTableGenerator newDdlTableGenerator() {
		return new DDLTableGenerator(getSqlTypeRegistry(), dmlNameProviderFactory);
	}
	
	protected DDLSequenceGenerator newDdlSequenceGenerator() {
		return new DDLSequenceGenerator(dmlNameProviderFactory);
	}
	
	@Override
	public DDLTableGenerator getDdlTableGenerator() {
		return ddlTableGenerator;
	}
	
	@Override
	public DDLSequenceGenerator getDdlSequenceGenerator() {
		return ddlSequenceGenerator;
	}
	
	@Override
	public DMLGenerator getDmlGenerator() {
		return dmlGenerator;
	}
	
	protected WriteOperationFactory newWriteOperationFactory() {
		return new WriteOperationFactory();
	}
	
	@Override
	public WriteOperationFactory getWriteOperationFactory() {
		return writeOperationFactory;
	}
	
	protected ReadOperationFactory newReadOperationFactory() {
		return new ReadOperationFactory();
	}
	
	@Override
	public ReadOperationFactory getReadOperationFactory() {
		return readOperationFactory;
	}
	
	@Override
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
	
	@Override
	public SqlTypeRegistry getSqlTypeRegistry() {
		return sqlTypeRegistry;
	}
	
	@Override
	public ColumnBinderRegistry getColumnBinderRegistry() {
		return columnBinderRegistry;
	}
	
	@Override
	public DMLNameProviderFactory getDmlNameProviderFactory() {
		return dmlNameProviderFactory;
	}
	
	@Override
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
	
	@Override
	public GeneratedKeysReaderFactory getGeneratedKeysReaderFactory() {
		return this::buildGeneratedKeysReader;
	}
	
	@Override
	public DatabaseSequenceSelectBuilder getDatabaseSequenceSelectBuilder() {
		return sequenceName -> "select next value for " + sequenceName;
	}
	
	/**
	 * Indicates if this dialect supports what ANSI-SQL terms "row value constructor" syntax, also called tuple syntax.
	 * Basically, does it support syntax like <pre>"... where (FIRST_NAME, LAST_NAME) = ('John', 'Doe')"</pre>.
	 *
	 * @return true if this SQL dialect supports "row value constructor" syntax, false otherwise.
	 */
	@Override
	public boolean supportsTupleCondition() {
		// returning false by default since most databases don't support it
		return false;
	}
}
