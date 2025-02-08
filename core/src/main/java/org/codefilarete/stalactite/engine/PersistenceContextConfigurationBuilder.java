package org.codefilarete.stalactite.engine;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.query.builder.QuotingDMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.Dialect.DialectSupport;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.QuerySQLBuilderFactoryBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;

/**
 * 
 * @author Guillaume Mary
 */
public class PersistenceContextConfigurationBuilder {
	
	protected final DatabaseVendorSettings vendorSettings;
	protected final ConnectionSettings connectionSettings;
	protected boolean quoteAllSQLIdentifiers = false;
	
	public PersistenceContextConfigurationBuilder(DatabaseVendorSettings vendorSettings, ConnectionSettings connectionSettings) {
		this.vendorSettings = vendorSettings;
		this.connectionSettings = connectionSettings;
	}
	
	public void quoteAllSQLIdentifiers() {
		setQuoteAllSQLIdentifiers(true);
	}
	
	public void setQuoteAllSQLIdentifiers(boolean quoteAllSQLIdentifiers) {
		this.quoteAllSQLIdentifiers = quoteAllSQLIdentifiers;
	}
	
	public PersistenceContextConfiguration build() {
		SqlTypeRegistry sqlTypeRegistry = buildSqlTypeRegistry();
		
		ColumnBinderRegistry columnBinderRegistry = buildColumnBinderRegistry();
		
		DMLNameProviderFactory dmlNameProviderFactory = buildDmlNameProviderFactory();
		
		SQLOperationsFactories sqlOperationsFactories = vendorSettings.getSqlOperationsFactoriesBuilder().build(columnBinderRegistry, dmlNameProviderFactory, sqlTypeRegistry);
		
		DDLTableGenerator ddlTableGenerator = sqlOperationsFactories.getDdlTableGenerator();
		DMLGenerator dmlGenerator = sqlOperationsFactories.getDmlGenerator();
		WriteOperationFactory writeOperationFactory = sqlOperationsFactories.getWriteOperationFactory();
		ReadOperationFactory readOperationFactory = sqlOperationsFactories.getReadOperationFactory();
		
		QuerySQLBuilderFactory querySQLBuilderFactory = new QuerySQLBuilderFactoryBuilder(
				dmlNameProviderFactory,
				columnBinderRegistry,
				vendorSettings.getJavaTypeToSqlTypes())
				.build();
		
		GeneratedKeysReaderFactory generatedKeysReaderFactory = vendorSettings.getGeneratedKeysReaderFactory();
		
		Dialect dialect = new DialectSupport(
				ddlTableGenerator,
				dmlGenerator,
				writeOperationFactory,
				readOperationFactory,
				querySQLBuilderFactory,
				sqlTypeRegistry,
				columnBinderRegistry,
				dmlNameProviderFactory,
				Objects.preventNull(connectionSettings.getInOperatorMaxSize(), vendorSettings.getInOperatorMaxSize()),
				generatedKeysReaderFactory,
				vendorSettings.supportsTupleCondition()
		);
		
		ConnectionConfiguration connectionConfiguration = buildConnectionConfiguration();
		
		return new PersistenceContextConfiguration(connectionConfiguration, dialect);
	}
	
	protected ColumnBinderRegistry buildColumnBinderRegistry() {
		return new ColumnBinderRegistry(vendorSettings.getParameterBinderRegistry());
	}
	
	protected SqlTypeRegistry buildSqlTypeRegistry() {
		return new SqlTypeRegistry(vendorSettings.getJavaTypeToSqlTypes());
	}
	
	protected DMLNameProviderFactory buildDmlNameProviderFactory() {
		DMLNameProviderFactory dmlNameProviderFactory;
		if (quoteAllSQLIdentifiers) {
			dmlNameProviderFactory = tableAliaser -> new QuotingDMLNameProvider(tableAliaser, vendorSettings.getQuotingCharacter());
		} else {
			dmlNameProviderFactory = new DMLNameProviderFactory() {
				@Override
				public DMLNameProvider build(Function<Fromable, String> tableAliaser) {
					return new DMLNameProvider(tableAliaser) {
						
						private final char quotingCharacter = vendorSettings.getQuotingCharacter();
						private final Set<String> keyWords = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER, vendorSettings.getKeyWords()));
						
						@Override
						public String getSimpleName(Selectable<?> column) {
							String name = super.getSimpleName(column);
							if (keyWords.contains(name)) {
								return quotingCharacter + name + quotingCharacter;
							} else {
								return name;
							}
						}
						
						@Override
						public String getName(Fromable table) {
							String name = super.getName(table);
							if (keyWords.contains(name)) {
								return quotingCharacter + name + quotingCharacter;
							} else {
								return name;
							}
						}
					};
				}
			};
		}
		return dmlNameProviderFactory;
	}
	
	protected ConnectionConfiguration buildConnectionConfiguration() {
		return new ConnectionConfigurationSupport(
				new CurrentThreadTransactionalConnectionProvider(connectionSettings.getDataSource(), connectionSettings.getConnectionOpeningRetryMaxCount()),
				connectionSettings.getBatchSize());
	}
	
	/**
	 * Small class to store result of {@link PersistenceContextConfigurationBuilder#build()}.
	 * @author Guillaume Mary
	 */
	public static class PersistenceContextConfiguration {
		
		private final ConnectionConfiguration connectionConfiguration;
		private final Dialect dialect;
		
		public PersistenceContextConfiguration(ConnectionConfiguration connectionConfiguration, Dialect dialect) {
			this.connectionConfiguration = connectionConfiguration;
			this.dialect = dialect;
		}
		
		public Dialect getDialect() {
			return dialect;
		}
		
		public ConnectionConfiguration getConnectionConfiguration() {
			return connectionConfiguration;
		}
	}
}
