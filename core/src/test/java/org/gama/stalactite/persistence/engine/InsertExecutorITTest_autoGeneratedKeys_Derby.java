package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.Lambdas;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.PropertyAccessor;
import org.gama.sql.dml.WriteOperation;
import org.gama.sql.result.Row;
import org.gama.sql.test.DerbyInMemoryDataSource;
import org.gama.stalactite.persistence.engine.AbstractDMLExecutorTest.PersistenceConfigurationBuilder.TableAndClass;
import org.gama.stalactite.persistence.id.manager.JDBCGeneratedKeysIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.Test;

import static org.gama.stalactite.persistence.id.manager.JDBCGeneratedKeysIdentifierManager.keyMapper;

/**
 * Same as {@link InsertExecutorITTest_autoGeneratedKeys} but dedicated to Derby that requires an override that is not compatible with
 * {@link DataProvider}: {@link AutoGeneratedKeysDerbyDataSet#newPersistenceConfigurationBuilder()}
 *
 * @author Guillaume Mary
 */
public class InsertExecutorITTest_autoGeneratedKeys_Derby extends InsertExecutorITTest_autoGeneratedKeys {
	
	protected static class AutoGeneratedKeysDerbyDataSet extends AutoGeneratedKeysDataSet {
		
		protected AutoGeneratedKeysDerbyDataSet() throws SQLException {
			super();
		}
		
		@Override
		protected PersistenceConfigurationBuilder newPersistenceConfigurationBuilder() {
			return new PersistenceConfigurationBuilder<Toto, Integer>()
					.withTableAndClass("Toto", Toto.class, new BiFunction<TableAndClass<Toto>, PropertyAccessor<Toto, Integer>,
							ClassMappingStrategy<Toto, Integer>>() {
						@Override
						public ClassMappingStrategy<Toto, Integer> apply(final TableAndClass<Toto> tableAndClass, PropertyAccessor<Toto, Integer>
								primaryKeyField) {
							tableAndClass.targetTable.getPrimaryKey().setAutoGenerated(true);
							
							String primaryKeyColumnName = tableAndClass.targetTable.getPrimaryKey().getName();
							GeneratedKeysReaderAsInt derbyGeneratedKeysReader = new GeneratedKeysReaderAsInt(primaryKeyColumnName) {
								/** Overriden to simulate generated keys for Derby because it only returns the highest generated key */
								@Override
								public List<Row> read(WriteOperation writeOperation) throws SQLException {
									List<Row> rows = super.read(writeOperation);
									// Derby only returns one row: the highest generated key
									Row first = Iterables.first(rows);
									int returnedKey = (int) first.get(getKeyName());
									// we append the missing values in incrementing order, assuming that's a one by one increment
									for (int i = 0; i < writeOperation.getUpdatedRowCount(); i++) {
										Row row = new Row();
										row.put(getKeyName(), returnedKey - i);
										rows.add(0, row);
									}
									return rows;
								}
							};
							
							return new ClassMappingStrategy<>(
									tableAndClass.mappedClass,
									tableAndClass.targetTable,
									tableAndClass.persistentFieldHarverster.getFieldToColumn(),
									primaryKeyField,
									new JDBCGeneratedKeysIdentifierManager<>(
											IdMappingStrategy.toIdAccessor(primaryKeyField),
											derbyGeneratedKeysReader,
											Lambdas.before(keyMapper(primaryKeyColumnName), () -> generatedKeysGetCallCount.increment()),
											Integer.class));
						}
						
					})
					.withPrimaryKeyFieldName("a");
		}
	}
	
	@DataProvider
	public static Object[][] dataSources() throws SQLException {
		AutoGeneratedKeysDerbyDataSet dataSet = new AutoGeneratedKeysDerbyDataSet();
		return new Object[][] {
				{ dataSet, new DerbyInMemoryDataSource(), new DDLTableGenerator(dataSet.dialect.getJavaTypeToSqlTypeMapping()) {
					
					@Override
					protected String getSqlType(Table.Column column) {
						String sqlType = super.getSqlType(column);
						if (column.isAutoGenerated()) {
							sqlType += " GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)";
						}
						return sqlType;
					}
				} },
		};
	}
	
	/**
	 * Overriden to force use of the declared data provider in this class, otherwise this test class is exactly the same as the parent one because
	 * the static data provider method is ignored (method hiding)
	 */
	@Override
	@Test
	@UseDataProvider("dataSources")
	public void testInsert_generated_pk_real_life(AutoGeneratedKeysDataSet dataSet, DataSource dataSource, DDLTableGenerator ddlTableGenerator)
			throws SQLException {
		super.testInsert_generated_pk_real_life(dataSet, dataSource, ddlTableGenerator);
	}
}