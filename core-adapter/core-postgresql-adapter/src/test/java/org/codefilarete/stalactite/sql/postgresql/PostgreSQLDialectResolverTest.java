package org.codefilarete.stalactite.sql.postgresql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver;
import org.codefilarete.stalactite.sql.postgresql.PostgreSQLDatabaseSettings.PostgreSQLDDLTableGenerator;
import org.codefilarete.stalactite.sql.postgresql.PostgreSQLDatabaseSettings.PostgreSQLSequenceSelectorFactory;
import org.codefilarete.stalactite.sql.postgresql.PostgreSQLDialectResolver.PostgreSQLDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.postgresql.statement.binder.PostgreSQLParameterBinderRegistry;
import org.codefilarete.stalactite.sql.postgresql.statement.binder.PostgreSQLTypeMapping;
import org.codefilarete.stalactite.sql.postgresql.test.PostgreSQLTestDataSourceSelector;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.sql.postgresql.PostgreSQLDatabaseSettings.KEYWORDS;

/**
 * @author Guillaume Mary
 */
class PostgreSQLDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle PostgreSQL 9.6
	 */
	@Test
	void resolve_9_6() throws SQLException {
		DataSource postgresqlDataSource = new PostgreSQLTestDataSourceSelector().giveDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(postgresqlDataSource.getConnection());
		assertThat(dialect.getCompatibility()).usingRecursiveComparison().isEqualTo(new PostgreSQLDatabaseSignet(9, 6));
		
		DatabaseVendorSettings vendorSettings = dialectResolver.determineVendorSettings(postgresqlDataSource.getConnection());
		assertThat(vendorSettings.getKeywords()).containsExactlyInAnyOrder(KEYWORDS);
		assertThat(vendorSettings.getQuoteCharacter()).isEqualTo('`');
		assertThat(vendorSettings.getJavaTypeToSqlTypes()).isExactlyInstanceOf(PostgreSQLTypeMapping.class);
		assertThat(vendorSettings.getParameterBinderRegistry()).isExactlyInstanceOf(PostgreSQLParameterBinderRegistry.class);
		assertThat(vendorSettings.getInOperatorMaxSize()).isEqualTo(1000);
		
		SQLOperationsFactories sqlOperationsFactories = vendorSettings.getSqlOperationsFactoriesBuilder().build(new ColumnBinderRegistry(), DMLNameProvider::new, new SqlTypeRegistry());
		assertThat(sqlOperationsFactories.getReadOperationFactory()).isExactlyInstanceOf(ReadOperationFactory.class);
		assertThat(sqlOperationsFactories.getWriteOperationFactory()).isExactlyInstanceOf(WriteOperationFactory.class);
		assertThat(sqlOperationsFactories.getDdlTableGenerator()).isExactlyInstanceOf(PostgreSQLDDLTableGenerator.class);
		assertThat(sqlOperationsFactories.getDmlGenerator()).isExactlyInstanceOf(DMLGenerator.class);
		assertThat(sqlOperationsFactories.getSequenceSelectorFactory()).isExactlyInstanceOf(PostgreSQLSequenceSelectorFactory.class);
		assertThat(vendorSettings.getGeneratedKeysReaderFactory()).isExactlyInstanceOf(DefaultGeneratedKeysReaderFactory.class);
	}
}
