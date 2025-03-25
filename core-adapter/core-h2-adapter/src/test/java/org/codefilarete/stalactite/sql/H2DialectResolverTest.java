package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.H2DialectResolver.H2DatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.H2DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.H2ParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.H2TypeMapping;
import org.codefilarete.stalactite.sql.test.H2InMemoryDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.sql.H2DatabaseSettings.KEYWORDS;

/**
 * @author Guillaume Mary
 */
class H2DialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle H2 1.4
	 */
	@Test
	void resolve_1_4() throws SQLException {
		DataSource h2DataSource = new H2InMemoryDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(h2DataSource.getConnection());
		assertThat(dialect.getCompatibility()).usingRecursiveComparison().isEqualTo(new H2DatabaseSignet(1, 4));
		
		DatabaseVendorSettings vendorSettings = dialectResolver.determineVendorSettings(h2DataSource.getConnection());
		assertThat(vendorSettings.getKeywords()).containsExactlyInAnyOrder(KEYWORDS);
		assertThat(vendorSettings.getQuoteCharacter()).isEqualTo('"');
		assertThat(vendorSettings.getJavaTypeToSqlTypes()).isExactlyInstanceOf(H2TypeMapping.class);
		assertThat(vendorSettings.getParameterBinderRegistry()).isExactlyInstanceOf(H2ParameterBinderRegistry.class);
		assertThat(vendorSettings.getInOperatorMaxSize()).isEqualTo(1000);
		
		SQLOperationsFactories sqlOperationsFactories = vendorSettings.getSqlOperationsFactoriesBuilder().build(new ColumnBinderRegistry(), DMLNameProvider::new, new SqlTypeRegistry());
		assertThat(sqlOperationsFactories.getReadOperationFactory()).isExactlyInstanceOf(ReadOperationFactory.class);
		assertThat(sqlOperationsFactories.getWriteOperationFactory()).isExactlyInstanceOf(WriteOperationFactory.class);
		assertThat(sqlOperationsFactories.getDdlTableGenerator()).isExactlyInstanceOf(H2DDLTableGenerator.class);
		assertThat(sqlOperationsFactories.getDmlGenerator()).isExactlyInstanceOf(DMLGenerator.class);
		assertThat(vendorSettings.getGeneratedKeysReaderFactory()).isExactlyInstanceOf(DefaultGeneratedKeysReaderFactory.class);
	}
}