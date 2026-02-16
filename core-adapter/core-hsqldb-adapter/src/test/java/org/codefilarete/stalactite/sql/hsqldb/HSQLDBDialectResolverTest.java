package org.codefilarete.stalactite.sql.hsqldb;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectResolver.HSQLDBDatabaseSignet;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDatabaseSettings.HSQLDBWriteOperationFactory;
import org.codefilarete.stalactite.sql.hsqldb.ddl.HSQLDBDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.hsqldb.statement.binder.HSQLDBParameterBinderRegistry;
import org.codefilarete.stalactite.sql.hsqldb.statement.binder.HSQLDBTypeMapping;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.sql.hsqldb.HSQLDBDatabaseSettings.KEYWORDS;

/**
 * @author Guillaume Mary
 */
class HSQLDBDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle HSQLDB 2.7
	 */
	@Test
	void resolve_2_7() throws SQLException {
		DataSource hsqldbDataSource = new HSQLDBInMemoryDataSource();

		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(hsqldbDataSource.getConnection());
		assertThat(dialect.getCompatibility()).usingRecursiveComparison().isEqualTo(new HSQLDBDatabaseSignet(2, 7));
		
		DatabaseVendorSettings vendorSettings = dialectResolver.determineVendorSettings(hsqldbDataSource.getConnection());
		assertThat(vendorSettings.getKeywords()).containsExactlyInAnyOrder(KEYWORDS);
		assertThat(vendorSettings.getQuoteCharacter()).isEqualTo('"');
		assertThat(vendorSettings.getJavaTypeToSqlTypes()).isExactlyInstanceOf(HSQLDBTypeMapping.class);
		assertThat(vendorSettings.getParameterBinderRegistry()).isExactlyInstanceOf(HSQLDBParameterBinderRegistry.class);
		assertThat(vendorSettings.getInOperatorMaxSize()).isEqualTo(1000);
		
		SQLOperationsFactories sqlOperationsFactories = vendorSettings.getSqlOperationsFactoriesBuilder().build(new ColumnBinderRegistry(), DMLNameProvider::new, new SqlTypeRegistry());
		assertThat(sqlOperationsFactories.getReadOperationFactory()).isExactlyInstanceOf(ReadOperationFactory.class);
		assertThat(sqlOperationsFactories.getWriteOperationFactory()).isExactlyInstanceOf(HSQLDBWriteOperationFactory.class);
		assertThat(sqlOperationsFactories.getDdlTableGenerator()).isExactlyInstanceOf(HSQLDBDDLTableGenerator.class);
		assertThat(sqlOperationsFactories.getDmlGenerator()).isExactlyInstanceOf(DMLGenerator.class);
		assertThat(vendorSettings.getGeneratedKeysReaderFactory()).isExactlyInstanceOf(DefaultGeneratedKeysReaderFactory.class);
	}
}
