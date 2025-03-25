package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.SQLiteDatabaseSettings.SQLiteGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.SQLiteDatabaseSettings.SQLiteSequenceSelectorFactory;
import org.codefilarete.stalactite.sql.SQLiteDatabaseSettings.SQLiteWriteOperationFactory;
import org.codefilarete.stalactite.sql.SQLiteDialectResolver.SQLiteDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.SQLiteDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.SQLiteParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.SQLiteTypeMapping;
import org.codefilarete.stalactite.sql.test.SQLiteInMemoryDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.sql.SQLiteDatabaseSettings.KEYWORDS;

/**
 * @author Guillaume Mary
 */
class SQLiteDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle SQLite 3.45
	 */
	@Test
	void resolve_3_45() throws SQLException {
		DataSource SQLiteDataSource = new SQLiteInMemoryDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(SQLiteDataSource.getConnection());
		assertThat(dialect.getCompatibility()).usingRecursiveComparison().isEqualTo(new SQLiteDatabaseSignet(3, 45));
		
		DatabaseVendorSettings vendorSettings = dialectResolver.determineVendorSettings(SQLiteDataSource.getConnection());
		assertThat(vendorSettings.getKeywords()).containsExactlyInAnyOrder(KEYWORDS);
		assertThat(vendorSettings.getQuoteCharacter()).isEqualTo('"');
		assertThat(vendorSettings.getJavaTypeToSqlTypes()).isExactlyInstanceOf(SQLiteTypeMapping.class);
		assertThat(vendorSettings.getParameterBinderRegistry()).isExactlyInstanceOf(SQLiteParameterBinderRegistry.class);
		assertThat(vendorSettings.getInOperatorMaxSize()).isEqualTo(1000);
		
		SQLOperationsFactories sqlOperationsFactories = vendorSettings.getSqlOperationsFactoriesBuilder().build(new ColumnBinderRegistry(), DMLNameProvider::new, new SqlTypeRegistry());
		assertThat(sqlOperationsFactories.getReadOperationFactory()).isExactlyInstanceOf(ReadOperationFactory.class);
		assertThat(sqlOperationsFactories.getWriteOperationFactory()).isExactlyInstanceOf(SQLiteWriteOperationFactory.class);
		assertThat(sqlOperationsFactories.getDdlTableGenerator()).isExactlyInstanceOf(SQLiteDDLTableGenerator.class);
		assertThat(sqlOperationsFactories.getDmlGenerator()).isExactlyInstanceOf(SQLiteDMLGenerator.class);
		assertThat(sqlOperationsFactories.getSequenceSelectorFactory()).isExactlyInstanceOf(SQLiteSequenceSelectorFactory.class);
		assertThat(vendorSettings.getGeneratedKeysReaderFactory()).isExactlyInstanceOf(SQLiteGeneratedKeysReaderFactory.class);
	}
}