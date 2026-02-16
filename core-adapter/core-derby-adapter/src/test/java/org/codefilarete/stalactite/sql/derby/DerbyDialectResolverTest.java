package org.codefilarete.stalactite.sql.derby;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver;
import org.codefilarete.stalactite.sql.derby.DerbyDatabaseSettings.DerbyGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.derby.DerbyDatabaseSettings.DerbyReadOperationFactory;
import org.codefilarete.stalactite.sql.derby.DerbyDatabaseSettings.DerbyWriteOperationFactory;
import org.codefilarete.stalactite.sql.derby.DerbyDialectResolver.DerbyDatabaseSignet;
import org.codefilarete.stalactite.sql.derby.ddl.DerbyDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.derby.statement.binder.DerbyParameterBinderRegistry;
import org.codefilarete.stalactite.sql.derby.statement.binder.DerbyTypeMapping;
import org.codefilarete.stalactite.sql.derby.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.sql.derby.DerbyDatabaseSettings.KEYWORDS;

/**
 * @author Guillaume Mary
 */
class DerbyDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle Apache Derby 10.14
	 */
	@Test
	void resolve_10_14() throws SQLException {
		DataSource derbyDataSource = new DerbyInMemoryDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(derbyDataSource.getConnection());
		assertThat(dialect.getCompatibility()).usingRecursiveComparison().isEqualTo(new DerbyDatabaseSignet(10, 14));
		
		DatabaseVendorSettings vendorSettings = dialectResolver.determineVendorSettings(derbyDataSource.getConnection());
		assertThat(vendorSettings.getKeywords()).containsExactlyInAnyOrder(KEYWORDS);
		assertThat(vendorSettings.getQuoteCharacter()).isEqualTo('"');
		assertThat(vendorSettings.getJavaTypeToSqlTypes()).isExactlyInstanceOf(DerbyTypeMapping.class);
		assertThat(vendorSettings.getParameterBinderRegistry()).isExactlyInstanceOf(DerbyParameterBinderRegistry.class);
		assertThat(vendorSettings.getInOperatorMaxSize()).isEqualTo(1000);
		
		SQLOperationsFactories sqlOperationsFactories = vendorSettings.getSqlOperationsFactoriesBuilder().build(new ColumnBinderRegistry(), DMLNameProvider::new, new SqlTypeRegistry());
		assertThat(sqlOperationsFactories.getReadOperationFactory()).isExactlyInstanceOf(DerbyReadOperationFactory.class);
		assertThat(sqlOperationsFactories.getWriteOperationFactory()).isExactlyInstanceOf(DerbyWriteOperationFactory.class);
		assertThat(sqlOperationsFactories.getDdlTableGenerator()).isExactlyInstanceOf(DerbyDDLTableGenerator.class);
		assertThat(sqlOperationsFactories.getDmlGenerator()).isExactlyInstanceOf(DMLGenerator.class);
		assertThat(vendorSettings.getGeneratedKeysReaderFactory()).isExactlyInstanceOf(DerbyGeneratedKeysReaderFactory.class);
	}
}
