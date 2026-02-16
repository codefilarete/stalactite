package org.codefilarete.stalactite.sql.oracle;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver;
import org.codefilarete.stalactite.sql.oracle.OracleDatabaseSettings.OracleGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.oracle.OracleDatabaseSettings.OracleWriteOperationFactory;
import org.codefilarete.stalactite.sql.oracle.OracleDialectResolver.OracleDatabaseSignet;
import org.codefilarete.stalactite.sql.oracle.ddl.OracleDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.oracle.statement.binder.OracleParameterBinderRegistry;
import org.codefilarete.stalactite.sql.oracle.statement.binder.OracleTypeMapping;
import org.codefilarete.stalactite.sql.oracle.test.OracleEmbeddableDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.sql.oracle.OracleDatabaseSettings.KEYWORDS;

/**
 * @author Guillaume Mary
 */
class OracleDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle Oracle 23
	 */
	@Test
	void resolve_23_0() throws SQLException {
		DataSource oracleDataSource = new OracleEmbeddableDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(oracleDataSource.getConnection());
		assertThat(dialect.getCompatibility()).usingRecursiveComparison().isEqualTo(new OracleDatabaseSignet(23, 0));
		
		DatabaseVendorSettings vendorSettings = dialectResolver.determineVendorSettings(oracleDataSource.getConnection());
		assertThat(vendorSettings.getKeywords()).containsExactlyInAnyOrder(KEYWORDS);
		assertThat(vendorSettings.getQuoteCharacter()).isEqualTo('"');
		assertThat(vendorSettings.getJavaTypeToSqlTypes()).isExactlyInstanceOf(OracleTypeMapping.class);
		assertThat(vendorSettings.getParameterBinderRegistry()).isExactlyInstanceOf(OracleParameterBinderRegistry.class);
		assertThat(vendorSettings.getInOperatorMaxSize()).isEqualTo(1000);
		
		SQLOperationsFactories sqlOperationsFactories = vendorSettings.getSqlOperationsFactoriesBuilder().build(new ColumnBinderRegistry(), DMLNameProvider::new, new SqlTypeRegistry());
		assertThat(sqlOperationsFactories.getReadOperationFactory()).isExactlyInstanceOf(ReadOperationFactory.class);
		assertThat(sqlOperationsFactories.getWriteOperationFactory()).isExactlyInstanceOf(OracleWriteOperationFactory.class);
		assertThat(sqlOperationsFactories.getDdlTableGenerator()).isExactlyInstanceOf(OracleDDLTableGenerator.class);
		assertThat(sqlOperationsFactories.getDmlGenerator()).isExactlyInstanceOf(DMLGenerator.class);
		assertThat(vendorSettings.getGeneratedKeysReaderFactory()).isExactlyInstanceOf(OracleGeneratedKeysReaderFactory.class);
	}
}
