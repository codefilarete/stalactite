package org.gama.stalactite.persistence.engine.cascade;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class InsertToBeforeInsertCascaderTest extends CascaderTest {
	
	@Test
	public void testBeforeInsert() throws SQLException {
		// Necessary Persister to be passed to the InsertToBeforeInsertCascader tested instance
		PersistenceContext persistenceContextMock = mock(PersistenceContext.class);
		when(persistenceContextMock.getDialect()).thenReturn(new Dialect(new JavaTypeToSqlTypeMapping(), new ColumnBinderRegistry()));
		Persister<Tata> persisterMock = new Persister<Tata>(persistenceContextMock, mock(ClassMappingStrategy.class)) {
			@Override
			protected void doInsert(Iterable<Tata> iterable) {
				// Overriden to do no action, because default super action is complex to mock
			}
		};
		
		final List<String> actions = new ArrayList<>();
		final List<Tata> triggeredTarget = new ArrayList<>();
		// Instance to test: overriden methods allow further cheching
		InsertToBeforeInsertCascader<Toto, Tata> testInstance = new InsertToBeforeInsertCascader<Toto, Tata>(persisterMock) {
			@Override
			protected void postTargetInsert(Iterable<Tata> iterables) {
				actions.add("postTargetInsert");
				triggeredTarget.addAll(Iterables.copy(iterables));
			}
			
			@Override
			protected Collection<Tata> getTargets(Toto toto) {
				actions.add("getTargets");
				return Arrays.asList(toto.tata);
			}
		};
		
		// 
		Toto triggeringInstance1 = new Toto(new Tata());
		Toto triggeringInstance2 = new Toto(new Tata());
		testInstance.beforeInsert(Arrays.asList(triggeringInstance1, triggeringInstance2));
		
		// check actions are done in good order
		assertEquals(Arrays.asList("getTargets", "getTargets", "postTargetInsert"), actions);
		// check triggered targets are those expected
		assertEquals(Arrays.asList(triggeringInstance1.tata, triggeringInstance2.tata), triggeredTarget);
	}
}