package org.codefilarete.stalactite.persistence.engine.runtime;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.codefilarete.stalactite.persistence.engine.DDLDeployer;
import org.codefilarete.stalactite.persistence.engine.runtime.DMLExecutorTest.ComposedId;
import org.codefilarete.stalactite.persistence.engine.runtime.DMLExecutorTest.PersistenceConfiguration;
import org.codefilarete.stalactite.persistence.engine.runtime.DMLExecutorTest.Tata;
import org.codefilarete.stalactite.persistence.engine.runtime.DMLExecutorTest.Toto;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.test.DatabaseIntegrationTest;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
abstract class SelectExecutorITTest extends DatabaseIntegrationTest {
	
	private final Dialect dialect = new Dialect(new JavaTypeToSqlTypeMapping()
													.with(Integer.class, "int"));
	
	private final DMLExecutorTest dmlExecutorTest = new DMLExecutorTest();
	
	@Test
	void select() throws SQLException {
		PersistenceConfiguration<Toto, Integer, Table> persistenceConfiguration = dmlExecutorTest.giveDefaultPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		
		Connection connection = connectionProvider.giveConnection();
		ConnectionProvider connectionProvider = new SimpleConnectionProvider(connection);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), connectionProvider);
		ddlDeployer.getDdlGenerator().addTables(persistenceConfiguration.classMappingStrategy.getTargetTable());
		ddlDeployer.deployDDL();
		connection.prepareStatement("insert into Toto(a, b, c) values (1, 10, 100)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (2, 20, 200)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (3, 30, 300)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (4, 40, 400)").execute();
		
		SelectExecutor<Toto, Integer, Table> testInstance = new SelectExecutor<>(persistenceConfiguration.classMappingStrategy, connectionProvider, dmlGenerator, 3);
		
		// test with 1 id
		List<Toto> totos = testInstance.select(Arrays.asList(1));
		Toto t = Iterables.first(totos);
		assertThat((Object) t.a).isEqualTo(1);
		assertThat((Object) t.b).isEqualTo(10);
		assertThat((Object) t.c).isEqualTo(100);
		
		// test with 3 ids
		totos = testInstance.select(Arrays.asList(2, 3, 4));
		List<Toto> expectedResult = Arrays.asList(
			new Toto(2, 20, 200),
			new Toto(3, 30, 300),
			new Toto(4, 40, 400));
		assertThat(totos.toString()).isEqualTo(expectedResult.toString());
	}
	
	@Test
	void select_composedId_idIsItSelf() throws SQLException {
		PersistenceConfiguration<Toto, Toto, Table> persistenceConfiguration = dmlExecutorTest.giveIdAsItselfPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		
		Connection connection = connectionProvider.giveConnection();
		ConnectionProvider connectionProvider = new SimpleConnectionProvider(connection);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), connectionProvider);
		ddlDeployer.getDdlGenerator().addTables(persistenceConfiguration.classMappingStrategy.getTargetTable());
		ddlDeployer.deployDDL();
		connection.prepareStatement("insert into Toto(a, b, c) values (1, 10, 100)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (2, 20, 200)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (3, 30, 300)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (4, 40, 400)").execute();
		
		SelectExecutor<Toto, Toto, Table> testInstance = new SelectExecutor<>(persistenceConfiguration.classMappingStrategy, connectionProvider, dmlGenerator, 3);
		List<Toto> result = testInstance.select(Arrays.asList(new Toto(1, 10, null), new Toto(2, 20, null), new Toto(3, 30, null), new Toto(4, 40, null)));
		List<Toto> expectedResult = Arrays.asList(
			new Toto(1, 10, 100),
			new Toto(2, 20, 200),
			new Toto(3, 30, 300),
			new Toto(4, 40, 400));
		assertThat(result.toString()).isEqualTo(expectedResult.toString());
	}
	
	@Test
	void select_composedId_idIsABean() throws SQLException {
		PersistenceConfiguration<Tata, ComposedId, Table> persistenceConfiguration = dmlExecutorTest.giveComposedIdPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		
		Connection connection = connectionProvider.giveConnection();
		ConnectionProvider connectionProvider = new SimpleConnectionProvider(connection);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), connectionProvider);
		ddlDeployer.getDdlGenerator().addTables(persistenceConfiguration.classMappingStrategy.getTargetTable());
		ddlDeployer.deployDDL();
		connection.prepareStatement("insert into Tata(a, b, c) values (1, 10, 100)").execute();
		connection.prepareStatement("insert into Tata(a, b, c) values (2, 20, 200)").execute();
		connection.prepareStatement("insert into Tata(a, b, c) values (3, 30, 300)").execute();
		connection.prepareStatement("insert into Tata(a, b, c) values (4, 40, 400)").execute();
		
		SelectExecutor<Tata, ComposedId, Table> testInstance = new SelectExecutor<>(persistenceConfiguration.classMappingStrategy, connectionProvider, dmlGenerator, 3);
		List<Tata> result = testInstance.select(Arrays.asList(new ComposedId(1, 10), new ComposedId(2, 20), new ComposedId(3, 30), new ComposedId(4, 40)));
		Set<Tata> expectedResult = Arrays.asHashSet(
			new Tata(1, 10, 100),
			new Tata(2, 20, 200),
			new Tata(3, 30, 300),
			new Tata(4, 40, 400));
		assertThat(new HashSet<>(result)).isEqualTo(expectedResult);
	}
}