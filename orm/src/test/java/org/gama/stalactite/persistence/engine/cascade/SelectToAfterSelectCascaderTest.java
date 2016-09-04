package org.gama.stalactite.persistence.engine.cascade;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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
public class SelectToAfterSelectCascaderTest extends AbstractCascaderTest {
	
	@Test
	public void testAfterSelect() throws SQLException {
		ClassMappingStrategy mappingStrategyMock = mock(ClassMappingStrategy.class);
		// IdMappingStrategy is called by InsertExecutor to retrieve IdentifierInsertionManager but this will not be called, so we can mock it
		when(mappingStrategyMock.getIdMappingStrategy()).thenReturn(mock(IdMappingStrategy.class));
		Persister<Tata, Long> persisterMock = new Persister<Tata, Long>(mappingStrategyMock, mock(Dialect.class), null, 10) {
			@Override
			protected List<Tata> doSelect(Iterable<Long> ids) {
				List<Tata> selectedTarget = new ArrayList<>();
				for (Long id : ids) {
					Tata tata = new Tata();
					tata.id = id;
					selectedTarget.add(tata);
				}
				return selectedTarget;
			}
		};
		
		List<String> actions = new ArrayList<>();
		List<Tata> triggeredTarget = new ArrayList<>();
		
		// Instance to test: overriden methods allow later checking
		SelectToAfterSelectCascader<Toto, Tata, Long> testInstance = new SelectToAfterSelectCascader<Toto, Tata, Long>(persisterMock) {
			
			@Override
			protected void postTargetSelect(Iterable<Tata> iterable) {
				actions.add("postTargetSelect");
				triggeredTarget.addAll(Iterables.copy(iterable));
			}
			
			@Override
			protected Collection<Long> getTargetIds(Toto toto) {
				actions.add("getTargets");
				return Arrays.asList(((long) toto.hashCode()));
			}
		};
		
		Toto triggeringInstance1 = new Toto();
		Toto triggeringInstance2 = new Toto();
		testInstance.afterSelect(Arrays.asList(triggeringInstance1, triggeringInstance2));
		
		// check actions are done in good order
		assertEquals(Arrays.asList("getTargets", "getTargets", "postTargetSelect"), actions);
		// check triggered targets are those expected
		List<Long> tataIds = triggeredTarget.stream().map(tata -> tata.id).collect(Collectors.toList());
		assertEquals(Arrays.asList((long) triggeringInstance1.hashCode(), (long) triggeringInstance2.hashCode()), tataIds);
	}
}