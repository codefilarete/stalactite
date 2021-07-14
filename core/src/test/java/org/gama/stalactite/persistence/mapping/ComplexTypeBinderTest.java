package org.gama.stalactite.persistence.mapping;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.gama.lang.Strings;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.reflection.Accessors;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.DataSourceConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.binder.ParameterBinderIndex;
import org.gama.stalactite.sql.result.Row;
import org.gama.stalactite.sql.result.RowIterator;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.runtime.Persister;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class ComplexTypeBinderTest {
	
	@Test
	public void testReadingAndWriting() throws SQLException {
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Table targetTable = new Table<>("Toto");
		Column colId = targetTable.addColumn("id", Integer.class).primaryKey();
		Column colList = targetTable.addColumn("myList", (Class<List<String>>) (Class) List.class);
		
		// Our mapping : Toto.myList is mapped but not yet define with a binder
		ClassMappingStrategy<Toto, Integer, Table> totoMappingStrategy = new ClassMappingStrategy<>(
				Toto.class,
				targetTable,
				persistentFieldHarverster.mapFields(Toto.class, targetTable),
				Accessors.propertyAccessor(persistentFieldHarverster.getField("id")),
				// Basic mapping to prevent NullPointerException, even if it's not the goal of our test
				new AlreadyAssignedIdentifierManager<>(Integer.class, c -> {}, c -> false));
		
		// Creating our test instance : will persist List<String> as a String (stupid case)
		ComplexTypeBinder<List<String>> testInstance = new ComplexTypeBinder<>(DefaultParameterBinders.STRING_BINDER,
				s -> Arrays.asList(Strings.cutTail(Strings.cutHead(s, 1), 1).toString().split(",\\s")),
				Object::toString);
		
		// declaring it to our dialect
		HSQLDBDialect dialect = new HSQLDBDialect();
		dialect.getColumnBinderRegistry().register(colList, testInstance);
		dialect.getJavaTypeToSqlTypeMapping().put(colList, "VARCHAR(255)");
		
		// deploying schema to our database
		ConnectionProvider connectionProvider = new DataSourceConnectionProvider(new HSQLDBInMemoryDataSource());
		PersistenceContext persistenceContext = new PersistenceContext(connectionProvider, dialect);
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlGenerator().addTables(targetTable);
		ddlDeployer.deployDDL();
		
		// writing
		Persister<Toto, Integer, ?> totoPersister = new Persister<>(totoMappingStrategy, persistenceContext);
		Toto toto = new Toto(1, Arrays.asList("a", "b"));
		totoPersister.insert(Arrays.asList(toto));
		
		// reading
		Toto result = totoPersister.select(1);
		assertThat(result.myList).isEqualTo(Arrays.asList("a", "b"));
		
		// a more low level reading to see if it's correctly persisted or can be used directly
		ResultSet resultSet = connectionProvider.getCurrentConnection().prepareStatement("select * from Toto").executeQuery();
		RowIterator rowIterator = new RowIterator(resultSet, ParameterBinderIndex.fromMap(Maps.asMap("myList", testInstance)));
		
		// Result must be decoded as a List
		assertThat(rowIterator.hasNext()).isTrue();
		Row singleLine = rowIterator.next();
		assertThat(singleLine.get("myList")).isEqualTo(Arrays.asList("a", "b"));
	}
	
	private static class Toto {
		private Integer id;
		
		private List<String> myList;
		
		public Toto() {
		}
		
		public Toto(Integer id) {
			this(id, null);
		}
		
		public Toto(Integer id, List<String> myList) {
			this.id = id;
			this.myList = myList;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("a", (Object) id).add("myList", myList)
					+ "]";
		}
	}
}