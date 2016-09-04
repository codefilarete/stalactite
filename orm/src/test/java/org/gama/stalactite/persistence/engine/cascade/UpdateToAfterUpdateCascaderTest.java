package org.gama.stalactite.persistence.engine.cascade;

import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class UpdateToAfterUpdateCascaderTest extends AbstractCascaderTest {
	
	@Test
	public void testAfterUpdate() throws SQLException {
		ClassMappingStrategy mappingStrategyMock = mock(ClassMappingStrategy.class);
		// IdMappingStrategy is called by InsertExecutor to retrieve IdentifierInsertionManager but this will not be called, so we can mock it
		when(mappingStrategyMock.getIdMappingStrategy()).thenReturn(mock(IdMappingStrategy.class));
		Persister<Tata, Object> persisterMock = new Persister<Tata, Object>(mappingStrategyMock, mock(Dialect.class), null, 10) {
			@Override
			protected int doUpdate(Iterable<Entry<Tata, Tata>> differencesIterable, boolean allColumnsStatement) {
				// Overriden to do no action, because default super action is complex to mock
				return 0;
			}
		};
		
		final List<String> actions = new ArrayList<>();
		final List<Entry<Tata, Tata>> triggeredTarget = new ArrayList<>();
		// Instance to test: overriden methods allow later checking
		UpdateToAfterUpdateCascader<Toto, Tata> testInstance = new UpdateToAfterUpdateCascader<Toto, Tata>(persisterMock) {
			
			@Override
			protected void postTargetUpdate(Iterable<Entry<Tata, Tata>> iterables) {
				actions.add("postTargetUpdate");
				triggeredTarget.addAll(Iterables.copy(iterables));
			}
			
			@Override
			protected Collection<Entry<Tata, Tata>> getTargets(Toto modifiedTrigger, Toto unmodifiedTrigger) {
				actions.add("getTargets");
				return Arrays.asList((Entry<Tata, Tata>) new SimpleEntry<>(modifiedTrigger.tata, unmodifiedTrigger.tata));
			}
		};
		
		// 
		Toto triggeringInstance1 = new Toto(new Tata());
		Toto triggeringInstance1_modfied = new Toto(new Tata());
		Toto triggeringInstance2 = new Toto(new Tata());
		Toto triggeringInstance2_modified = new Toto(new Tata());
		testInstance.afterUpdate(Arrays.asList(new SimpleEntry<>(triggeringInstance1_modfied, triggeringInstance1),
				new SimpleEntry<>(triggeringInstance2_modified, triggeringInstance2)
		), true);
		
		// check actions are done in good order
		assertEquals(Arrays.asList("getTargets", "getTargets", "postTargetUpdate"), actions);
		// check triggered targets are those expected
		assertEquals(Arrays.asList(new SimpleEntry<>(triggeringInstance1_modfied.tata, triggeringInstance1.tata),
				new SimpleEntry<>(triggeringInstance2_modified.tata, triggeringInstance2.tata)), triggeredTarget);
	}
}