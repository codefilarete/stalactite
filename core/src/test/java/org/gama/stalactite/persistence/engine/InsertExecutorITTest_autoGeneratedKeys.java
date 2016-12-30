package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.Lambdas;
import org.gama.lang.Retryer;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.trace.IncrementableInt;
import org.gama.sql.dml.GeneratedKeysReader;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.sql.test.MariaDBEmbeddableDataSource;
import org.gama.stalactite.persistence.id.manager.JDBCGeneratedKeysIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.gama.stalactite.persistence.id.manager.JDBCGeneratedKeysIdentifierManager.keyMapper;
import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class InsertExecutorITTest_autoGeneratedKeys extends AbstractDMLExecutorTest {
	
	protected static class AutoGeneratedKeysDataSet extends DataSet {
		
		protected IncrementableInt generatedKeysGetCallCount;
		
		protected AutoGeneratedKeysDataSet() throws SQLException {
			super();
			generatedKeysGetCallCount = new IncrementableInt();
		}
		
		@Override
		protected PersistenceConfigurationBuilder newPersistenceConfigurationBuilder() {
			return new PersistenceConfigurationBuilder<Toto, Integer>()
					.withTableAndClass("Toto", Toto.class, (tableAndClass, primaryKeyField) -> {
						tableAndClass.targetTable.getPrimaryKey().setAutoGenerated(true);
						
						String primaryKeyColumnName = tableAndClass.targetTable.getPrimaryKey().getName();
						return new ClassMappingStrategy<>(
								tableAndClass.mappedClass,
								tableAndClass.targetTable,
								tableAndClass.persistentFieldHarverster.getFieldToColumn(),
								primaryKeyField,
								new JDBCGeneratedKeysIdentifierManager<>(
										IdMappingStrategy.toIdAccessor(primaryKeyField),
										new GeneratedKeysReaderAsInt(primaryKeyColumnName),
										Lambdas.before(keyMapper(primaryKeyColumnName), () -> generatedKeysGetCallCount.increment()),
										Integer.class)
						);
					})
					.withPrimaryKeyFieldName("a");
		}
	}
	
	
	@DataProvider
	public static Object[][] dataSources() throws SQLException {
		AutoGeneratedKeysDataSet dataSet1 = new AutoGeneratedKeysDataSet();
		AutoGeneratedKeysDataSet dataSet2 = new AutoGeneratedKeysDataSet();
		return new Object[][] {
				{ dataSet1, new HSQLDBInMemoryDataSource(), new DDLTableGenerator(dataSet1.dialect.getJavaTypeToSqlTypeMapping()) {
					
					@Override
					protected String getSqlType(Table.Column column) {
						String sqlType = super.getSqlType(column);
						if (column.isAutoGenerated()) {
							sqlType += " GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)";
						}
						return sqlType;
					}
					
					/** Overriden to do nothing because HSQLDB does not support "primary key" and "identity" */
					@Override
					protected void generateCreatePrimaryKey(Table table, StringAppender sqlCreateTable) {
					}
				}},
				{ dataSet2, new MariaDBEmbeddableDataSource(3406), new DDLTableGenerator(dataSet2.dialect.getJavaTypeToSqlTypeMapping()) {
					
					@Override
					protected String getSqlType(Table.Column column) {
						String sqlType = super.getSqlType(column);
						if (column.isAutoGenerated()) {
							sqlType += " auto_increment";
						}
						return sqlType;
					}
				}},
		};
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testInsert_generated_pk_real_life(AutoGeneratedKeysDataSet dataSet, DataSource dataSource, DDLTableGenerator ddlTableGenerator) throws SQLException {
		dataSet.transactionManager.setDataSource(dataSource);
		
		dataSet.dialect.getDdlSchemaGenerator().setDdlTableGenerator(ddlTableGenerator);
		
		DDLDeployer ddlDeployer = new DDLDeployer(dataSet.dialect.getDdlSchemaGenerator(), dataSet.transactionManager) {
			@Override
			protected Connection getCurrentConnection() throws SQLException {
				return dataSource.getConnection();
			}
		};
		ddlDeployer.getDdlSchemaGenerator().setTables(Arrays.asList(dataSet.persistenceConfiguration.targetTable));
		ddlDeployer.deployDDL();
		
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		InsertExecutor<Toto, Integer> testInstance = new InsertExecutor<>(dataSet.persistenceConfiguration.classMappingStrategy, dataSet.transactionManager, dmlGenerator, Retryer.NO_RETRY, 3, 3);
		List<Toto> totoList = Arrays.asList(new Toto(17, 23), new Toto(29, 31), new Toto(37, 41), new Toto(43, 53));
		testInstance.insert(totoList);
		
		assertEquals(4, dataSet.generatedKeysGetCallCount.getValue());	// if it fails with 0 it means that statement.getGeneratedKeys() returned an empty ResultSet
		// Verfy that database generated keys were set to Java instances
		int i = 1;
		for (Toto toto : totoList) {
			assertEquals(i++, (int) toto.a);
		}
	}
	
	public static class GeneratedKeysReaderAsInt extends GeneratedKeysReader {
		public GeneratedKeysReaderAsInt(String keyName) {
			super(keyName);
		}
		
		/** Overriden to use ResultSet.getInt instead of getLong because Toto.a is of type int (avoid exception (wrong type) during field set) */
		@Override
		protected Object readKey(ResultSet rs) throws SQLException {
			return rs.getInt(1);
		}
	}
	
}