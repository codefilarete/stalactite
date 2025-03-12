package org.codefilarete.stalactite.engine;

import java.util.function.Function;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.Dialect.DialectSupport;
import org.codefilarete.stalactite.sql.DialectOptions;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.QuerySQLBuilderFactoryBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.collection.CaseInsensitiveSet;

public class DialectBuilder {
	
	private final DatabaseVendorSettings vendorSettings;
	private final DialectOptions dialectOptions;
	
	public DialectBuilder(DatabaseVendorSettings vendorSettings) {
		this(vendorSettings, DialectOptions.noOptions());
	}
	
	public DialectBuilder(DatabaseVendorSettings vendorSettings, DialectOptions dialectOptions) {
		this.vendorSettings = vendorSettings;
		this.dialectOptions = dialectOptions;
	}
	
	public Dialect build() {
		SqlTypeRegistry sqlTypeRegistry = buildSqlTypeRegistry();
		
		ColumnBinderRegistry columnBinderRegistry = buildColumnBinderRegistry();
		
		DMLNameProviderFactory dmlNameProviderFactory = buildDmlNameProviderFactory();
		
		SQLOperationsFactories sqlOperationsFactories = vendorSettings.getSqlOperationsFactoriesBuilder().build(columnBinderRegistry, dmlNameProviderFactory, sqlTypeRegistry);
		
		DDLTableGenerator ddlTableGenerator = sqlOperationsFactories.getDdlTableGenerator();
		DDLSequenceGenerator ddlSequenceGenerator = sqlOperationsFactories.getDdlSequenceGenerator();
		DMLGenerator dmlGenerator = sqlOperationsFactories.getDmlGenerator();
		WriteOperationFactory writeOperationFactory = sqlOperationsFactories.getWriteOperationFactory();
		ReadOperationFactory readOperationFactory = sqlOperationsFactories.getReadOperationFactory();
		
		QuerySQLBuilderFactory querySQLBuilderFactory = new QuerySQLBuilderFactoryBuilder(
				dmlNameProviderFactory,
				columnBinderRegistry,
				vendorSettings.getJavaTypeToSqlTypes())
				.build();
		
		GeneratedKeysReaderFactory generatedKeysReaderFactory = vendorSettings.getGeneratedKeysReaderFactory();
		
		return new DialectSupport(
				vendorSettings.getCompatibility(),
				ddlTableGenerator,
				ddlSequenceGenerator,
				dmlGenerator,
				writeOperationFactory,
				readOperationFactory,
				querySQLBuilderFactory,
				sqlTypeRegistry,
				columnBinderRegistry,
				dmlNameProviderFactory,
				dialectOptions.getInOperatorMaxSize().getOrDefault(vendorSettings.getInOperatorMaxSize()),
				generatedKeysReaderFactory,
				vendorSettings.getDatabaseSequenceSelectorFactory(),
				vendorSettings.supportsTupleCondition()
		);
	}
	
	protected ColumnBinderRegistry buildColumnBinderRegistry() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry(vendorSettings.getParameterBinderRegistry());
		if (dialectOptions.getJavaTypeBinders().isTouched()) {
			dialectOptions.getJavaTypeBinders().consumeIfTouched(typeBindings -> {
				typeBindings.forEach(typeBinding -> {
					columnBinderRegistry.register((Class) typeBinding.getJavaType(), typeBinding.getParameterBinder());
				});
			});
		}
		return columnBinderRegistry;
	}
	
	protected SqlTypeRegistry buildSqlTypeRegistry() {
		SqlTypeRegistry sqlTypeRegistry = new SqlTypeRegistry(vendorSettings.getJavaTypeToSqlTypes());
		if (dialectOptions.getJavaTypeToSqlTypeMappings().isTouched()) {
			dialectOptions.getJavaTypeToSqlTypeMappings().consumeIfTouched(typeBindings -> {
				typeBindings.forEach(typeBinding -> {
					sqlTypeRegistry.put(typeBinding.getJavaType(), typeBinding.getSqlType());
				});
			});
		}
		return sqlTypeRegistry;
	}
	
	protected DMLNameProviderFactory buildDmlNameProviderFactory() {
		return dialectOptions.getQuoteSQLIdentifiers().getOrDefault(false)
				? QuotingDMLNameProvider::new
				: QuotingKeywordsDMLNameProvider::new;
	}
	
	protected class QuotingKeywordsDMLNameProvider extends QuotingDMLNameProvider {
		
		private final CaseInsensitiveSet keywords;
		
		protected QuotingKeywordsDMLNameProvider(Function<Fromable, String> tableAliaser) {
			super(tableAliaser);
			this.keywords = new CaseInsensitiveSet(vendorSettings.getKeyWords());
		}
		
		@Override
		protected String quote(String name) {
			if (keywords.contains(name)) {
				return super.quote(name);
			} else {
				return name;
			}
		}
	}
	
	protected class QuotingDMLNameProvider extends DMLNameProvider {
		
		private final char quoteCharacter;
		
		protected QuotingDMLNameProvider(Function<Fromable, String> tableAliaser) {
			super(tableAliaser);
			this.quoteCharacter = dialectOptions.getQuoteCharacter().getOrDefault(vendorSettings.getQuoteCharacter());
		}
		
		@Override
		public String getSimpleName(Selectable<?> column) {
			return quote(super.getSimpleName(column));
		}
		
		@Override
		public String getName(Fromable table) {
			return quote(super.getName(table));
		}
		
		@Override
		public String getAlias(Fromable table) {
			String alias = super.getAlias(table);
			return alias == null ? null : quote(alias);
		}
		
		protected String quote(String name) {
			return quoteCharacter + name + quoteCharacter;
		}
	}
}
