package org.gama.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.id.manager.JDBCGeneratedKeysIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.sql.dml.WriteOperationFactory;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.sql.dml.SQLExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
abstract class InsertExecutorAutoGeneratedKeysITTest extends AbstractDMLExecutorTest {

	protected DataSource dataSource;
	
	protected Dialect dialect;

	@BeforeEach
	void createDialect() {
		dialect = new Dialect(new JavaTypeToSqlTypeMapping()
			.with(Integer.class, "int"));
	}
	
	@BeforeEach
	abstract void createDataSource();

	protected PersistenceConfiguration<Toto, Integer, Table> giveAutoGeneratedKeysPersistenceConfiguration() {
		PersistenceConfiguration<Toto, Integer, Table> toReturn = new PersistenceConfiguration<>();

		Table targetTable = new Table("Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor<Toto, Object>, Column<Table, Object>> mappedFileds = persistentFieldHarverster.mapFields(Toto.class, targetTable);
		PropertyAccessor<Toto, Integer> primaryKeyAccessor = Accessors.propertyAccessor(persistentFieldHarverster.getField("a"));
		Column primaryKeyColumn = persistentFieldHarverster.getColumn(primaryKeyAccessor);
		primaryKeyColumn.primaryKey();
		primaryKeyColumn.setAutoGenerated(true);

		// changing mapping strategy to add JDBCGeneratedKeysIdentifierManager and GeneratedKeysReader
		IdentifierInsertionManager<Toto, Integer> identifierGenerator = new JDBCGeneratedKeysIdentifierManager<>(
			new SinglePropertyIdAccessor<>(primaryKeyAccessor),
			dialect.buildGeneratedKeysReader(primaryKeyColumn.getName(), primaryKeyColumn.getJavaType()),
			Integer.class);

		toReturn.classMappingStrategy = new ClassMappingStrategy<>(
			Toto.class,
			targetTable,
			mappedFileds,
			primaryKeyAccessor,
			identifierGenerator);
		toReturn.targetTable = targetTable;

		return toReturn;
	}
	
	@Test
	void insert_generated_pk_real_life() {
		jdbcMock.transactionManager.setDataSource(dataSource);

		PersistenceConfiguration<Toto, Integer, Table> persistenceConfiguration = giveAutoGeneratedKeysPersistenceConfiguration();

		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), jdbcMock.transactionManager) {
			@Override
			protected Connection getCurrentConnection() {
				try {
					return dataSource.getConnection();
				} catch (SQLException e) {
					throw new SQLExecutionException(e);
				}
			}
		};
		
		ddlDeployer.getDdlGenerator().setDdlTableGenerator(dialect.getDdlTableGenerator());
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(persistenceConfiguration.classMappingStrategy.getTargetTable()));
		ddlDeployer.getCreationScripts().forEach(System.out::println);
		ddlDeployer.deployDDL();
		
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		InsertExecutor<Toto, Integer, Table> testInstance = new InsertExecutor<>(persistenceConfiguration.classMappingStrategy,
				new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, new WriteOperationFactory(), 3);
		List<Toto> totoList = Arrays.asList(new Toto(17, 23), new Toto(29, 31), new Toto(37, 41), new Toto(43, 53));
		testInstance.insert(totoList);
		
		// Verfy that database generated keys were set to Java instances
		assertThat(Iterables.collectToList(totoList, toto -> toto.a)).isEqualTo(Arrays.asList(1, 2, 3, 4));
	}
}