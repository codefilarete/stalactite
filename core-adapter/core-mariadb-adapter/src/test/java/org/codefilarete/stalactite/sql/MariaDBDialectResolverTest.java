package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.MariaDBDatabaseSettings.MariaDBGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.MariaDBDialectResolver.MariaDBDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.MariaDBDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.MariaDBParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.MariaDBTypeMapping;
import org.codefilarete.stalactite.sql.test.MariaDBTestDataSourceSelector;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.sql.MariaDBDatabaseSettings.KEYWORDS;

/**
 * @author Guillaume Mary
 */
class MariaDBDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle MariaDB 10.0
	 */
	@Test
	void resolve_10_0() throws SQLException {
		DataSource mariaDBDataSource = new MariaDBTestDataSourceSelector().giveDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(mariaDBDataSource.getConnection());
		assertThat(dialect.getCompatibility()).usingRecursiveComparison().isEqualTo(new MariaDBDatabaseSignet(10, 0));
		
		DatabaseVendorSettings vendorSettings = dialectResolver.determineVendorSettings(mariaDBDataSource.getConnection());
		assertThat(vendorSettings.getKeyWords()).containsExactlyInAnyOrder(KEYWORDS);
		assertThat(vendorSettings.getQuoteCharacter()).isEqualTo('`');
		assertThat(vendorSettings.getJavaTypeToSqlTypes()).isExactlyInstanceOf(MariaDBTypeMapping.class);
		assertThat(vendorSettings.getParameterBinderRegistry()).isExactlyInstanceOf(MariaDBParameterBinderRegistry.class);
		assertThat(vendorSettings.getInOperatorMaxSize()).isEqualTo(1000);
		
		SQLOperationsFactories sqlOperationsFactories = vendorSettings.getSqlOperationsFactoriesBuilder().build(new ColumnBinderRegistry(), DMLNameProvider::new, new SqlTypeRegistry());
		assertThat(sqlOperationsFactories.getReadOperationFactory()).isExactlyInstanceOf(ReadOperationFactory.class);
		assertThat(sqlOperationsFactories.getWriteOperationFactory()).isExactlyInstanceOf(MariaDBWriteOperationFactory.class);
		assertThat(sqlOperationsFactories.getDdlTableGenerator()).isExactlyInstanceOf(MariaDBDDLTableGenerator.class);
		assertThat(sqlOperationsFactories.getDmlGenerator()).isExactlyInstanceOf(DMLGenerator.class);
		assertThat(vendorSettings.getGeneratedKeysReaderFactory()).isExactlyInstanceOf(MariaDBGeneratedKeysReaderFactory.class);
	}
	
}