package org.codefilarete.stalactite.sql;

import java.util.Locale;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelectBuilder;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.sql.ddl.DDLGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
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
public interface Dialect {
	
	DDLTableGenerator getDdlTableGenerator();
	
	DDLSequenceGenerator getDdlSequenceGenerator();
	
	DMLGenerator getDmlGenerator();
	
	WriteOperationFactory getWriteOperationFactory();
	
	ReadOperationFactory getReadOperationFactory();
	
	QuerySQLBuilderFactory getQuerySQLBuilderFactory();
	
	SqlTypeRegistry getSqlTypeRegistry();
	
	ColumnBinderRegistry getColumnBinderRegistry();
	
	DMLNameProviderFactory getDmlNameProviderFactory();
	
	int getInOperatorMaxSize();
	
	default <I> GeneratedKeysReader<I> buildGeneratedKeysReader(String keyName, Class<I> columnType) {
		return getGeneratedKeysReaderFactory().build(keyName, columnType);
	}
	
	GeneratedKeysReaderFactory getGeneratedKeysReaderFactory();
	
	DatabaseSequenceSelectBuilder getDatabaseSequenceSelectBuilder();
	
	/**
	 * Indicates if this dialect supports what ANSI-SQL terms "row value constructor" syntax, also called tuple syntax.
	 * Basically, does it support syntax like <pre>"... where (FIRST_NAME, LAST_NAME) = ('John', 'Doe')"</pre>.
	 *
	 * @return true if this SQL dialect supports "row value constructor" syntax, false otherwise.
	 */
	boolean supportsTupleCondition();
	
	class DialectSupport implements Dialect {
		
		private final DDLTableGenerator ddlTableGenerator;
		
		private final DDLSequenceGenerator ddlSequenceGenerator;
		
		private final DMLGenerator dmlGenerator;
		
		private final WriteOperationFactory writeOperationFactory;
		
		private final ReadOperationFactory readOperationFactory;
		
		private final QuerySQLBuilderFactory querySQLBuilderFactory;
		
		private final SqlTypeRegistry sqlTypeRegistry;
		
		private final ColumnBinderRegistry columnBinderRegistry;
		
		private final DMLNameProviderFactory dmlNameProviderFactory;
		
		private final int inOperatorMaxSize;
		
		private final GeneratedKeysReaderFactory generatedKeysReaderFactory;
		
		private final DatabaseSequenceSelectBuilder databaseSequenceSelectBuilder;
		
		private final boolean supportsTupleCondition;
		
		public DialectSupport(DDLTableGenerator ddlTableGenerator,
							  DDLSequenceGenerator ddlSequenceGenerator,
							  DMLGenerator dmlGenerator,
							  WriteOperationFactory writeOperationFactory,
							  ReadOperationFactory readOperationFactory,
							  QuerySQLBuilderFactory querySQLBuilderFactory,
							  SqlTypeRegistry sqlTypeRegistry,
							  ColumnBinderRegistry columnBinderRegistry,
							  DMLNameProviderFactory dmlNameProviderFactory,
							  int inOperatorMaxSize,
							  GeneratedKeysReaderFactory generatedKeysReaderFactory,
							  DatabaseSequenceSelectBuilder databaseSequenceSelectBuilder,
							  boolean supportsTupleCondition) {
			this.ddlTableGenerator = ddlTableGenerator;
			this.ddlSequenceGenerator = ddlSequenceGenerator;
			this.dmlGenerator = dmlGenerator;
			this.writeOperationFactory = writeOperationFactory;
			this.readOperationFactory = readOperationFactory;
			this.querySQLBuilderFactory = querySQLBuilderFactory;
			this.sqlTypeRegistry = sqlTypeRegistry;
			this.columnBinderRegistry = columnBinderRegistry;
			this.dmlNameProviderFactory = dmlNameProviderFactory;
			this.inOperatorMaxSize = inOperatorMaxSize;
			this.generatedKeysReaderFactory = generatedKeysReaderFactory;
			this.databaseSequenceSelectBuilder = databaseSequenceSelectBuilder;
			this.supportsTupleCondition = supportsTupleCondition;
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
		
		@Override
		public WriteOperationFactory getWriteOperationFactory() {
			return writeOperationFactory;
		}
		
		@Override
		public ReadOperationFactory getReadOperationFactory() {
			return readOperationFactory;
		}
		
		@Override
		public QuerySQLBuilderFactory getQuerySQLBuilderFactory() {
			return querySQLBuilderFactory;
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
		
		@Override
		public GeneratedKeysReaderFactory getGeneratedKeysReaderFactory() {
			return generatedKeysReaderFactory;
		}
		
		@Override
		public DatabaseSequenceSelectBuilder getDatabaseSequenceSelectBuilder() {
			return databaseSequenceSelectBuilder;
		}
		
		@Override
		public boolean supportsTupleCondition() {
			return supportsTupleCondition;
		}
	}
}
