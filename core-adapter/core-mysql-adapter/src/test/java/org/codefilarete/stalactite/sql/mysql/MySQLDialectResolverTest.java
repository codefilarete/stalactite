package org.codefilarete.stalactite.sql.mysql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver;
import org.codefilarete.stalactite.sql.mysql.MySQLDatabaseSettings.MySQLGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.mysql.MySQLDatabaseSettings.MySQLSequenceSelectorFactory;
import org.codefilarete.stalactite.sql.mysql.MySQLDialectResolver.MySQLDatabaseSignet;
import org.codefilarete.stalactite.sql.mysql.ddl.MySQLDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.mysql.statement.binder.MySQLParameterBinderRegistry;
import org.codefilarete.stalactite.sql.mysql.statement.binder.MySQLTypeMapping;
import org.codefilarete.stalactite.sql.mysql.test.MySQLTestDataSourceSelector;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.sql.mysql.MySQLDatabaseSettings.KEYWORDS;

/**
 * @author Guillaume Mary
 */
class MySQLDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle MySQL 5.6
	 */
	@Test
	void resolve_5_6() throws SQLException {
		DataSource mySQLDataSource = new MySQLTestDataSourceSelector().giveDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(mySQLDataSource.getConnection());
		assertThat(dialect.getCompatibility()).usingRecursiveComparison().isEqualTo(new MySQLDatabaseSignet(5, 6));
		
		DatabaseVendorSettings vendorSettings = dialectResolver.determineVendorSettings(mySQLDataSource.getConnection());
		assertThat(vendorSettings.getKeywords()).containsExactlyInAnyOrder(KEYWORDS);
		assertThat(vendorSettings.getQuoteCharacter()).isEqualTo('`');
		assertThat(vendorSettings.getJavaTypeToSqlTypes()).isExactlyInstanceOf(MySQLTypeMapping.class);
		assertThat(vendorSettings.getParameterBinderRegistry()).isExactlyInstanceOf(MySQLParameterBinderRegistry.class);
		assertThat(vendorSettings.getInOperatorMaxSize()).isEqualTo(1000);
		
		SQLOperationsFactories sqlOperationsFactories = vendorSettings.getSqlOperationsFactoriesBuilder().build(new ColumnBinderRegistry(), DMLNameProvider::new, new SqlTypeRegistry());
		assertThat(sqlOperationsFactories.getReadOperationFactory()).isExactlyInstanceOf(ReadOperationFactory.class);
		assertThat(sqlOperationsFactories.getWriteOperationFactory()).isExactlyInstanceOf(MySQLWriteOperationFactory.class);
		assertThat(sqlOperationsFactories.getDdlTableGenerator()).isExactlyInstanceOf(MySQLDDLTableGenerator.class);
		assertThat(sqlOperationsFactories.getDmlGenerator()).isExactlyInstanceOf(DMLGenerator.class);
		assertThat(sqlOperationsFactories.getSequenceSelectorFactory()).isExactlyInstanceOf(MySQLSequenceSelectorFactory.class);
		assertThat(vendorSettings.getGeneratedKeysReaderFactory()).isExactlyInstanceOf(MySQLGeneratedKeysReaderFactory.class);
	}
}
