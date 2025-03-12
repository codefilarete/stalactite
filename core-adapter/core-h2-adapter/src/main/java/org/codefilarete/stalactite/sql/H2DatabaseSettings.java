package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.sql.H2DialectResolver.H2DatabaseSignet;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.H2ParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.H2TypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.collection.CaseInsensitiveSet;
import org.codefilarete.tool.function.Sequence;

/**
 * 
 * @author Guillaume Mary
 */
public class H2DatabaseSettings extends DatabaseVendorSettings {
	
	public static final H2DatabaseSettings H2_1_4 = new H2DatabaseSettings();
	
	private H2DatabaseSettings() {
		this(new ReadOperationFactory(), new H2ParameterBinderRegistry());
	}
	
	private H2DatabaseSettings(ReadOperationFactory readOperationFactory, H2ParameterBinderRegistry parameterBinderRegistry) {
		super(new H2DatabaseSignet(1, 4),
				Collections.unmodifiableSet(new CaseInsensitiveSet()),
				'"',
				new H2TypeMapping(),
				parameterBinderRegistry,
				new SQLOperationsFactoriesBuilder() {
					@Override
					public SQLOperationsFactories build(ParameterBinderIndex<Column, ParameterBinder> columnBinderRegistry, DMLNameProviderFactory dmlNameProviderFactory, SqlTypeRegistry sqlTypeRegistry) {
						return null;
					}
				},
				new DefaultGeneratedKeysReaderFactory(parameterBinderRegistry),
				new DatabaseSequenceSelectorFactory() {
					@Override
					public Sequence<Long> create(org.codefilarete.stalactite.sql.ddl.structure.Sequence databaseSequence, ConnectionProvider connectionProvider) {
						return null;
					}
				}, 
				100,
				true);
	}
}
