package org.gama.stalactite.persistence.engine.runtime;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.gama.lang.Duo;
import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.model.AbstractVehicle;
import org.gama.stalactite.persistence.engine.model.Vehicle;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * @author Guillaume Mary
 */
class EntityIsManagedByPersisterAsserterTest {
	
	public static Object[][] assertionIsInvoked() throws NoSuchMethodException {
		Class<IEntityPersister> persisterInterface = IEntityPersister.class;
		return new Object[][] {
				{ persisterInterface.getMethod("persist", Object.class),
						new Object[] { new Vehicle(42L) } },
				{ persisterInterface.getMethod("persist", Iterable.class),
						new Object[] { Arrays.asList(new Vehicle(42L)) } },
				{ persisterInterface.getMethod("insert", Object.class),
						new Object[] { new Vehicle(42L) } },
				{ persisterInterface.getMethod("insert", Iterable.class),
						new Object[] { Arrays.asList(new Vehicle(42L)) } },
				{ persisterInterface.getMethod("update", Object.class),
						new Object[] { new Vehicle(42L) } },
				{ persisterInterface.getMethod("update", Iterable.class),
						new Object[] { Arrays.asList(new Vehicle(42L)) } },
				{ persisterInterface.getMethod("update", Iterable.class, boolean.class),
						new Object[] { Arrays.asList(new Duo<>(new Vehicle(42L), new Vehicle(42L))), true } },
				{ persisterInterface.getMethod("update", Object.class, Object.class, boolean.class),
						new Object[] { new Vehicle(42L), new Vehicle(42L), true } },
				{ persisterInterface.getMethod("updateById", Object.class),
						new Object[] { new Vehicle(42L) } },
				{ persisterInterface.getMethod("updateById", Iterable.class),
						new Object[] { Arrays.asList(new Vehicle(42L)) } },
				{ persisterInterface.getMethod("delete", Object.class),
						new Object[] { new Vehicle(42L) } },
				{ persisterInterface.getMethod("delete", Iterable.class),
						new Object[] { Arrays.asList(new Vehicle(42L)) } },
				{ persisterInterface.getMethod("deleteById", Object.class),
						new Object[] { new Vehicle(42L) } },
				{ persisterInterface.getMethod("deleteById", Iterable.class),
						new Object[] { Arrays.asList(new Vehicle(42L)) } },
		};
	}
	
	@ParameterizedTest
	@MethodSource
	void assertionIsInvoked(Method invokedMethod, Object[] args) throws InvocationTargetException, IllegalAccessException {
		IEntityConfiguredJoinedTablesPersister surrogateMock = mock(IEntityConfiguredJoinedTablesPersister.class);
		when(surrogateMock.getClassToPersist()).thenReturn(Vehicle.class);
		when(surrogateMock.getId(args[0])).thenReturn(42L);
		IEntityMappingStrategy mappingStrategyMock = mock(IEntityMappingStrategy.class);
		when(mappingStrategyMock.getId(any())).thenReturn(42L);
		when(surrogateMock.getMappingStrategy()).thenReturn(mappingStrategyMock);
		
		EntityIsManagedByPersisterAsserter<AbstractVehicle, Integer> testInstance = Mockito.spy(new EntityIsManagedByPersisterAsserter<>(surrogateMock));
		invokedMethod.invoke(testInstance, args);
		if (args[0] instanceof Iterable) {
			Mockito.verify(testInstance, times(1)).assertPersisterManagesEntities(any());
		} else {
			Mockito.verify(testInstance, times(1)).assertPersisterManagesEntity(any());
		}
	}
	
	@ParameterizedTest
	@MethodSource("assertionIsInvoked")
	void assertionIsInvoked_withPolymorphicPersisterAtInit(Method invokedMethod, Object[] args) throws InvocationTargetException, IllegalAccessException {
		IEntityConfiguredJoinedTablesPersister surrogateMock = mock(IEntityConfiguredJoinedTablesPersister.class, withSettings().extraInterfaces(PolymorphicPersister.class));
		when(((PolymorphicPersister) surrogateMock).getSupportedEntityTypes()).thenReturn(Arrays.asSet(Vehicle.class));
		when(surrogateMock.getId(args[0])).thenReturn(42L);
		IEntityMappingStrategy mappingStrategyMock = mock(IEntityMappingStrategy.class);
		when(mappingStrategyMock.getId(any())).thenReturn(42L);
		when(surrogateMock.getMappingStrategy()).thenReturn(mappingStrategyMock);
		
		EntityIsManagedByPersisterAsserter<AbstractVehicle, Integer> testInstance = Mockito.spy(new EntityIsManagedByPersisterAsserter<>(surrogateMock));
		invokedMethod.invoke(testInstance, args);
		if (args[0] instanceof Iterable) {
			Mockito.verify(testInstance, times(1)).assertPersisterManagesEntities(any());
		} else {
			Mockito.verify(testInstance, times(1)).assertPersisterManagesEntity(any());
		}
	}
}