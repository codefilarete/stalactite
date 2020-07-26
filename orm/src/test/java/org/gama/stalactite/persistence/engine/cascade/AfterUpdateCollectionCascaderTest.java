package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.Retryer;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.runtime.Persister;
import org.gama.stalactite.persistence.engine.runtime.UpdateExecutor;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
		IReversibleAccessor<Tata, Long> identifier = Accessors.accessorByField(Tata.class, "id");
		IReversibleAccessor<Tata, Long> propName = Accessors.accessorByField(Tata.class, "name");
		// we must add a property to let us set some differences between 2 instances and have them detected by the system
		Map<? extends IReversibleAccessor<Tata, Object>, Column<T, Object>> mapping = (Map) Maps.asMap(identifier, (Column) primaryKey).add(propName, nameColumn);
		ClassMappingStrategy<Tata, Long, T> mappingStrategyMock = new ClassMappingStrategy<>(Tata.class, tataTable,
				mapping, identifier,
				new AlreadyAssignedIdentifierManager<>(Long.class, c -> {}, c -> false));
		
		Persister<Tata, Long, T> persisterStub = new Persister<Tata, Long, T>(mappingStrategyMock, mock(Dialect.class), mock(IConnectionConfiguration.class)) {
			
			@Override
			protected UpdateExecutor<Tata, Long, T> newUpdateExecutor(IEntityMappingStrategy<Tata, Long, T> mappingStrategy, IConnectionConfiguration connectionConfiguration, DMLGenerator dmlGenerator, Retryer writeOperationRetryer, int inOperatorMaxSize) {
				return new UpdateExecutor<Tata, Long, T>(mappingStrategy, connectionConfiguration, dmlGenerator,
						writeOperationRetryer, inOperatorMaxSize) {
					
					@Override
					public int update(Iterable<? extends Duo<? extends Tata, ? extends Tata>> differencesIterable, boolean allColumnsStatement) {
						// Overriden to do no action, because default super action is complex to mock
						return 0;
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
		assertEquals(Arrays.asList("getTargets", "getTargets", "postTargetUpdate"), actions);
		// check triggered targets are those expected
		assertEquals(Arrays.asList(new Duo<>(triggeringInstance1_modified.tata, triggeringInstance1.tata),
				new Duo<>(triggeringInstance2_modified.tata, triggeringInstance2.tata)), triggeredTarget);
	}
}