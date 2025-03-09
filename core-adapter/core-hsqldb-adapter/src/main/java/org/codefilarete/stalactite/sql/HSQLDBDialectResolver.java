package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.HSQLDBDialect.HSQLDBWriteOperationFactory;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.HSQLDBDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.HSQLDBParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.HSQLDBTypeMapping;
import org.codefilarete.tool.collection.Arrays;

/**
 * @author Guillaume Mary
 */
public class HSQLDBDialectResolver {
	
	private static final ReadOperationFactory READ_OPERATION_FACTORY = new ReadOperationFactory();
	
	public static class HSQLDB_2_7_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final HSQLDBDialect HSQLDB_DIALECT = new HSQLDBDialect();
		
		private static final DatabaseSignet HSQL_2_7_SIGNET = new DatabaseSignet("HSQL Database Engine", 2, 7);
		
		private static final HSQLDBParameterBinderRegistry PARAMETER_BINDER_REGISTRY = new HSQLDBParameterBinderRegistry();
		
		private static final DatabaseVendorSettings HSQLDB_VENDOR_SETTINGS = new DatabaseVendorSettings(
				Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER)),
				'"',
				new HSQLDBTypeMapping(),
				PARAMETER_BINDER_REGISTRY,
				(parameterBinders, dmlNameProviderFactory, sqlTypeRegistry) -> {
					DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, NoopSorter.INSTANCE, dmlNameProviderFactory);
					HSQLDBDDLTableGenerator ddlTableGenerator = new HSQLDBDDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
					DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
					return new SQLOperationsFactories(new HSQLDBWriteOperationFactory(), READ_OPERATION_FACTORY, dmlGenerator, ddlTableGenerator, ddlSequenceGenerator);
				},
				new DefaultGeneratedKeysReaderFactory(PARAMETER_BINDER_REGISTRY),
				new HSQLDBDatabaseSequenceSelectorFactory(),
				100,
				false
		);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return HSQL_2_7_SIGNET;
		}
		
		@Override
		public Dialect getDialect() {
			return HSQLDB_DIALECT;
		}
		
		@Override
		public DatabaseVendorSettings getVendorSettings() {
			return HSQLDB_VENDOR_SETTINGS;
		}
	}
	
	private static class HSQLDBDatabaseSequenceSelectorFactory implements DatabaseSequenceSelectorFactory {
		
		@Override
		public DatabaseSequenceSelector create(Sequence databaseSequence, ConnectionProvider connectionProvider) {
			return new DatabaseSequenceSelector(databaseSequence, "CALL NEXT VALUE FOR " + databaseSequence.getName(), READ_OPERATION_FACTORY, connectionProvider);
		}
	}
}
