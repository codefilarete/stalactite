package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.OracleDialectResolver.OracleDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.OracleParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.OracleTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.collection.CaseInsensitiveSet;
import org.codefilarete.tool.function.Sequence;

/**
 * 
 * @author Guillaume Mary
 */
public class OracleDatabaseSettings extends DatabaseVendorSettings {
	
	public static final OracleDatabaseSettings ORACLE_23_0 = new OracleDatabaseSettings();
	
	private OracleDatabaseSettings() {
		this(new ReadOperationFactory(), new OracleParameterBinderRegistry());
	}
	
	private OracleDatabaseSettings(ReadOperationFactory readOperationFactory, OracleParameterBinderRegistry parameterBinderRegistry) {
		super(new OracleDatabaseSignet(23, 0),
				Collections.unmodifiableSet(new CaseInsensitiveSet()),
				'"',
				new OracleTypeMapping(),
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
