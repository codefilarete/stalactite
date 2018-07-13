package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayList;
import java.util.List;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class AfterDeleteCascaderTest extends AbstractCascaderTest {
	
	@Test
	public void testAfterDelete() {
		ClassMappingStrategy mappingStrategyMock = mock(ClassMappingStrategy.class);
		// IdMappingStrategy is called by InsertExecutor to retrieve IdentifierInsertionManager but this will not be called, so we can mock it
		when(mappingStrategyMock.getIdMappingStrategy()).thenReturn(mock(IdMappingStrategy.class));
		Persister<Tata, Long, Table> persisterMock = new Persister<Tata, Long, Table>(mappingStrategyMock, mock(Dialect.class), null, 10) {
			@Override
			protected int doDelete(Iterable<Tata> entities) {
				// Overriden to do no action, because default super action is complex to mock
				return 0;
			}
		};
		
		List<String> actions = new ArrayList<>();
		List<Tata> triggeredTarget = new ArrayList<>();
		// Instance to test: overriden methods allow later checking
		AfterDeleteCascader<Toto, Tata> testInstance = new AfterDeleteCascader<Toto, Tata>(persisterMock) {
			@Override
			protected void postTargetDelete(Iterable<Tata> entities) {
				actions.add("postTargetDelete");
				triggeredTarget.addAll(Iterables.copy(entities));
			}
			
			@Override
			protected Tata getTarget(Toto toto) {
				actions.add("getTargets");
				return toto.tata;
			}
		};
		
		// 
		Toto triggeringInstance1 = new Toto(new Tata());
		Toto triggeringInstance2 = new Toto(new Tata());
		testInstance.afterDelete(Arrays.asList(triggeringInstance1, triggeringInstance2));
		
		// check actions are done in good order
		assertEquals(Arrays.asList("getTargets", "getTargets", "postTargetDelete"), actions);
		// check triggered targets are those expected
		assertEquals(Arrays.asList(triggeringInstance1.tata, triggeringInstance2.tata), triggeredTarget);
	}
}