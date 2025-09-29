package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.PostgreSQLDialectResolver.PostgreSQLDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.stalactite.sql.statement.binder.PostgreSQLParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.PostgreSQLTypeMapping;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.CaseInsensitiveSet;

/**
 * 
 * @author Guillaume Mary
 */
public class PostgreSQLDatabaseSettings extends DatabaseVendorSettings {
	
	/**
	 * PostgreSQL keywords, took from <a href="https://www.postgresql.org/docs/current/sql-keywords-appendix.html">PostgreSQL documentation</a>
	 * and selecting only "reserved" ones by PostgreSQL (the ones that require to be escaped)
	 */
	public static final String[] KEYWORDS = new String[] {
			"ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ASC", "ASYMMETRIC",
			"BOTH",
			"CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
			"DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO",
			"ELSE", "END",
			"FALSE", "FOREIGN",
			"IN", "INITIALLY",
			"LATERAL", "LEADING", "LOCALTIME", "LOCALTIMESTAMP",
			"NOT", "NULL",
			"ONLY", "OR",
			"PLACING", "PRIMARY",
			"REFERENCES",
			"SELECT", "SESSION_USER", "SOME", "SYMMETRIC", "SYSTEM_USER",
			"TABLE", "THEN", "TRAILING", "TRUE",
			"UNIQUE", "USER", "USING",
			"VARIADIC",
			"WHEN",
	};
	
	public static final PostgreSQLDatabaseSettings POSTGRESQL_9_6 = new PostgreSQLDatabaseSettings();
	
	private PostgreSQLDatabaseSettings() {
		this(new PostgreSQLSQLOperationsFactoriesBuilder(), new PostgreSQLParameterBinderRegistry());
	}
	
	private PostgreSQLDatabaseSettings(PostgreSQLSQLOperationsFactoriesBuilder sqlOperationsFactoriesBuilder, PostgreSQLParameterBinderRegistry parameterBinderRegistry) {
		super(new PostgreSQLDatabaseSignet(9, 6),
				Collections.unmodifiableSet(new CaseInsensitiveSet(KEYWORDS)),
				'`',
				new PostgreSQLTypeMapping(),
				parameterBinderRegistry,
				sqlOperationsFactoriesBuilder,
				new DefaultGeneratedKeysReaderFactory(parameterBinderRegistry),
				1000,
				true);
	}
	
	private static class PostgreSQLSQLOperationsFactoriesBuilder implements SQLOperationsFactoriesBuilder {
		
		private final ReadOperationFactory readOperationFactory;
		private final WriteOperationFactory writeOperationFactory;
		
		private PostgreSQLSQLOperationsFactoriesBuilder() {
			this.readOperationFactory = new ReadOperationFactory();
			this.writeOperationFactory = new WriteOperationFactory();
		}
		
		private ReadOperationFactory getReadOperationFactory() {
			return readOperationFactory;
		}
		
		private WriteOperationFactory getWriteOperationFactory() {
			return writeOperationFactory;
		}
		
		@Override
		public SQLOperationsFactories build(ParameterBinderIndex<Column, ParameterBinder> parameterBinders, DMLNameProviderFactory dmlNameProviderFactory, SqlTypeRegistry sqlTypeRegistry) {
			DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, DMLGenerator.NoopSorter.INSTANCE, dmlNameProviderFactory);
			PostgreSQLDDLTableGenerator ddlTableGenerator = new PostgreSQLDDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
			DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
			return new SQLOperationsFactories(writeOperationFactory, readOperationFactory, dmlGenerator, ddlTableGenerator, ddlSequenceGenerator,
					new PostgreSQLSequenceSelectorFactory(readOperationFactory));
		}
	}
	
	public static class PostgreSQLDDLTableGenerator extends DDLTableGenerator {
		
		public PostgreSQLDDLTableGenerator(SqlTypeRegistry typeMapping, DMLNameProviderFactory dmlNameProviderFactory) {
			super(typeMapping, dmlNameProviderFactory);
		}
		
		@Override
		protected String getSqlType(Column column) {
			String sqlType;
			if (column.isAutoGenerated()) {
				sqlType = "BIGSERIAL";
			} else {
				sqlType = super.getSqlType(column);
			}
			return sqlType;
		}
	}
	
	@VisibleForTesting
	static class PostgreSQLSequenceSelectorFactory implements DatabaseSequenceSelectorFactory {
		
		private final ReadOperationFactory readOperationFactory;
		
		private PostgreSQLSequenceSelectorFactory(ReadOperationFactory readOperationFactory) {
			this.readOperationFactory = readOperationFactory;
		}
		
		@Override
		public DatabaseSequenceSelector create(Sequence databaseSequence, ConnectionProvider connectionProvider) {
			return new DatabaseSequenceSelector(databaseSequence, "select nextval('" + databaseSequence.getAbsoluteName() + "')", readOperationFactory, connectionProvider);
		}
	}
}
