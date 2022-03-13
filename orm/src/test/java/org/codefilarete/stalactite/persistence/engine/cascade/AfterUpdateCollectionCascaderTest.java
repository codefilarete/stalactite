package org.codefilarete.stalactite.persistence.engine.cascade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.persistence.engine.runtime.Persister;
import org.codefilarete.stalactite.persistence.engine.runtime.UpdateExecutor;
import org.codefilarete.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.persistence.mapping.ClassMappingStrategy;
import org.codefilarete.stalactite.persistence.mapping.EntityMappingStrategy;
import org.codefilarete.stalactite.persistence.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.persistence.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
public class AfterUpdateCollectionCascaderTest extends AbstractCascaderTest {
	
	@Test
	public <T extends Table<T>> void testAfterUpdate() {
		T tataTable = (T) new Table("Tata");
		Column<T, Long> primaryKey = tataTable.addColumn("id", Long.class).primaryKey();
		Column<T, String> nameColumn = tataTable.addColumn("name", String.class);
		ReversibleAccessor<Tata, Long> identifier = Accessors.accessorByField(Tata.class, "id");
		ReversibleAccessor<Tata, Long> propName = Accessors.accessorByField(Tata.class, "name");
		// we must add a property to let us set some differences between 2 instances and have them detected by the system
		Map<? extends ReversibleAccessor<Tata, Object>, Column<T, Object>> mapping = (Map) Maps.asMap(identifier, (Column) primaryKey).add(propName, nameColumn);
		ClassMappingStrategy<Tata, Long, T> mappingStrategyMock = new ClassMappingStrategy<>(Tata.class, tataTable,
				mapping, identifier,
				new AlreadyAssignedIdentifierManager<>(Long.class, c -> {}, c -> false));
		
		Persister<Tata, Long, T> persisterStub = new Persister<Tata, Long, T>(mappingStrategyMock, mock(Dialect.class), mock(ConnectionConfiguration.class)) {
			
			@Override
			protected UpdateExecutor<Tata, Long, T> newUpdateExecutor(EntityMappingStrategy<Tata, Long, T> mappingStrategy, ConnectionConfiguration connectionConfiguration, DMLGenerator dmlGenerator, WriteOperationFactory writeOperationFactory, int inOperatorMaxSize) {
				return new UpdateExecutor<Tata, Long, T>(mappingStrategy, connectionConfiguration, dmlGenerator,
						new WriteOperationFactory(), inOperatorMaxSize) {
					
					@Override
					public void update(Iterable<? extends Duo<Tata, Tata>> differencesIterable, boolean allColumnsStatement) {
						// Overriden to do no action, because default super action is complex to mock
					}
				};
			}
		};
		
		List<String> actions = new ArrayList<>();
		List<Duo<? extends Tata, ? extends Tata>> triggeredTarget = new ArrayList<>();
		// Instance to test: overriden methods allow later checking
		AfterUpdateCollectionCascader<Toto, Tata> testInstance = new AfterUpdateCollectionCascader<Toto, Tata>(persisterStub) {
			
			@Override
			protected void postTargetUpdate(Iterable<? extends Duo<? extends Tata, ? extends Tata>> entities) {
				actions.add("postTargetUpdate");
				triggeredTarget.addAll(Iterables.collectToList(entities, Function.identity()));
			}
			
			@Override
			protected Collection<Duo<Tata, Tata>> getTargets(Toto modifiedTrigger, Toto unmodifiedTrigger) {
				actions.add("getTargets");
				return Arrays.asList((Duo<Tata, Tata>) new Duo<>(modifiedTrigger.tata, unmodifiedTrigger.tata));
			}
		};
		
		int tataId = 42;
		Toto triggeringInstance1 = new Toto(new Tata().setId(tataId));
		Toto triggeringInstance1_modified = new Toto(new Tata().setId(tataId));
		Toto triggeringInstance2 = new Toto(new Tata().setName("x").setId(tataId));
		Toto triggeringInstance2_modified = new Toto(new Tata().setName("y").setId(tataId));
		// we give some instance with modifications (on name), so they'll be detected has modified (see Tata's strategy). Thus postTargetUpdate will be triggered
		testInstance.afterUpdate(Arrays.asList(
				new Duo<>(triggeringInstance1_modified, triggeringInstance1),
				new Duo<>(triggeringInstance2_modified, triggeringInstance2)), true);
		
		// check actions are done in good order
		assertThat(actions).isEqualTo(Arrays.asList("getTargets", "getTargets", "postTargetUpdate"));
		// check triggered targets are those expected
		assertThat(triggeredTarget).isEqualTo(Arrays.asList(new Duo<>(triggeringInstance1_modified.tata, triggeringInstance1.tata),
				new Duo<>(triggeringInstance2_modified.tata, triggeringInstance2.tata)));
	}
}