package org.gama.stalactite.persistence.engine.cascade;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class SelectToAfterSelectCascaderTest extends CascaderTest {
	
	@Test
	public void testAfterSelect() throws SQLException {
		PersistenceContext persistenceContextMock = mock(PersistenceContext.class);
		when(persistenceContextMock.getDialect()).thenReturn(new Dialect(new JavaTypeToSqlTypeMapping(), new ColumnBinderRegistry()));
		Persister<Tata> persisterMock = new Persister<Tata>(mock(ClassMappingStrategy.class),
				persistenceContextMock.getDialect(),
				null, 10) {
			@Override
			protected List<Tata> doSelect(Iterable<Serializable> ids) {
				List<Tata> selectedTarget = new ArrayList<>();
				for (Serializable id : ids) {
					Tata tata = new Tata();
					tata.id = (Long) id;
					selectedTarget.add(tata);
				}
				return selectedTarget;
			}
		};
		
		final List<String> actions = new ArrayList<>();
		final List<Tata> triggeredTarget = new ArrayList<>();
		
		// Instance to test: overriden methods allow further cheching
		SelectToAfterSelectCascader<Toto, Tata> testInstance = new SelectToAfterSelectCascader<Toto, Tata>(persisterMock) {
			
			@Override
			protected void postTargetSelect(Iterable<Tata> iterable) {
				actions.add("postTargetSelect");
				triggeredTarget.addAll(Iterables.copy(iterable));
			}
			
			@Override
			protected Collection<Serializable> getTargetIds(Toto toto) {
				actions.add("getTargets");
				return Arrays.asList((Serializable)  ((long) toto.hashCode()^17));
			}
		};
		
		// 
		Toto triggeringInstance1 = new Toto();
		Toto triggeringInstance2 = new Toto();
		testInstance.afterSelect(Arrays.asList(triggeringInstance1, triggeringInstance2));
		
		// check actions are done in good order
		assertEquals(Arrays.asList("getTargets", "getTargets", "postTargetSelect"), actions);
		// check triggered targets are those expected
		List<Long> tataIds = new ArrayList<>();
		for (Tata tata : triggeredTarget) {
			tataIds.add(tata.id);
		}
		assertEquals(Arrays.asList((long) triggeringInstance1.hashCode()^17, (long) triggeringInstance2.hashCode()^17), tataIds);
	}
}