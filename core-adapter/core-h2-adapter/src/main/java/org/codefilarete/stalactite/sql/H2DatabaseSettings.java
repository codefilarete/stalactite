package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.H2DialectResolver.H2DatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.H2DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.H2ParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.H2TypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.CaseInsensitiveSet;

/**
 * @author Guillaume Mary
 */
public class H2DatabaseSettings extends DatabaseVendorSettings {
	
	/**
	 * Derby keywords, took from <a href="https://www.h2database.com/html/advanced.html#keywords">H2 documentation</a> because those of
	 * it JDBC Drivers are not enough / accurate. Moreover it might requires a Database Connection to retrieve them (see {@link org.h2.jdbc.JdbcDatabaseMetaData#getSQLKeywords()})
	 */
	@VisibleForTesting
	static final String[] KEYWORDS = new String[] {
			"ALL", "AND", "ANY", "ARRAY", "AS", "ASYMMETRIC", "AUTHORIZATION",
			"BETWEEN", "BOTH",
			"CASE", "CAST", "CHECK", "CONSTRAINT", "CROSS", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA",
				"CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
			"DAY", "DEFAULT", "DISTINCT",
			"ELSE", "END", "EXCEPT", "EXISTS",
			"FALSE", "FETCH", "FOR", "FOREIGN", "FROM", "FULL",
			"GROUP", "GROUPS", "HAVING", "HOUR",
			"IF", "ILIKE", "IN", "INNER", "INTERSECT", "INTERVAL", "IS",
			"JOIN",
			"KEY",
			"LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP",
			"MINUS", "MINUTE", "MONTH",
			"NATURAL", "NOT", "NULL",
			"OFFSET", "ON", "OR", "ORDER", "OVER",
			"PARTITION", "PRIMARY",
			"QUALIFY",
			"RANGE", "REGEXP", "RIGHT", "ROW", "ROWNUM", "ROWS",
			"SECOND", "SELECT", "SESSION_USER", "SET", "SOME", "SYMMETRIC", "SYSTEM_USER",
			"TABLE", "TO", "TOP", "TRAILING", "TRUE",
			"UESCAPE", "UNION", "UNIQUE", "UNKNOWN", "USER", "USING",
			"VALUE", "VALUES",
			"WHEN", "WHERE", "WINDOW", "WITH",
			"YEAR",
			"_ROWID_"
	};
	
	public static final H2DatabaseSettings H2_1_4 = new H2DatabaseSettings();
	
	private H2DatabaseSettings() {
		this(new H2SQLOperationsFactoriesBuilder(), new H2ParameterBinderRegistry());
	}
	
	private H2DatabaseSettings(H2SQLOperationsFactoriesBuilder h2SQLOperationsFactoriesBuilder, H2ParameterBinderRegistry parameterBinderRegistry) {
		super(new H2DatabaseSignet(1, 4),
				Collections.unmodifiableSet(new CaseInsensitiveSet(KEYWORDS)),
				'"',
				new H2TypeMapping(),
				parameterBinderRegistry,
				h2SQLOperationsFactoriesBuilder,
				new DefaultGeneratedKeysReaderFactory(parameterBinderRegistry),
				1000,
				true);
	}
	
	private static class H2SQLOperationsFactoriesBuilder implements SQLOperationsFactoriesBuilder {
		
		private final ReadOperationFactory readOperationFactory;
		private final WriteOperationFactory writeOperationFactory;
		
		private H2SQLOperationsFactoriesBuilder() {
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
			H2DDLTableGenerator ddlTableGenerator = new H2DDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
			DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
			return new SQLOperationsFactories(writeOperationFactory, readOperationFactory, dmlGenerator, ddlTableGenerator, ddlSequenceGenerator, new H2SequenceSelectorFactory(readOperationFactory));
		}
	}
	
	private static class H2SequenceSelectorFactory implements DatabaseSequenceSelectorFactory {
		
		private final ReadOperationFactory readOperationFactory;
		
		private H2SequenceSelectorFactory(ReadOperationFactory readOperationFactory) {
			this.readOperationFactory = readOperationFactory;
		}
		
		@Override
		public DatabaseSequenceSelector create(org.codefilarete.stalactite.sql.ddl.structure.Sequence databaseSequence, ConnectionProvider connectionProvider) {
			return new DatabaseSequenceSelector(databaseSequence, "select next value for " + databaseSequence.getAbsoluteName(), readOperationFactory, connectionProvider);
		}
	}
}
