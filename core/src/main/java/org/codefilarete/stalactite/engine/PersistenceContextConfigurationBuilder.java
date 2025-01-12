package org.codefilarete.stalactite.engine;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.PersistenceContext.PersistenceContextConfiguration;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.query.builder.QuotingDMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
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
	
	private final DatabaseVendorSettings vendorSettings;
	private final ConnectionSettings connectionSettings;
	private boolean quoteAllSQLIdentifiers = false;
	
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
		ConnectionConfiguration connectionConfiguration = new ConnectionConfigurationSupport(
				new CurrentThreadConnectionProvider(connectionSettings.getDataSource(), connectionSettings.getConnectionOpeningRetryMaxCount()),
				connectionSettings.getBatchSize());
		
		SqlTypeRegistry sqlTypeRegistry = new SqlTypeRegistry(vendorSettings.getJavaTypeToSqlTypes());
		
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry(vendorSettings.getParameterBinderRegistry());
		
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
		
		int inOperatorMaxSize = Objects.preventNull(connectionSettings.getInOperatorMaxSize(), vendorSettings.getInOperatorMaxSize());
		
		GeneratedKeysReaderFactory generatedKeysReaderFactory = vendorSettings.getGeneratedKeysReaderFactory();
		
		boolean supportsTupleCondition = vendorSettings.supportsTupleCondition();
		
		Dialect dialect = new DialectSupport(
				ddlTableGenerator,
				dmlGenerator,
				writeOperationFactory,
				readOperationFactory,
				querySQLBuilderFactory,
				sqlTypeRegistry,
				columnBinderRegistry,
				dmlNameProviderFactory,
				inOperatorMaxSize,
				generatedKeysReaderFactory,
				supportsTupleCondition
		);
		return new PersistenceContextConfiguration(connectionConfiguration, dialect);
	}
}
