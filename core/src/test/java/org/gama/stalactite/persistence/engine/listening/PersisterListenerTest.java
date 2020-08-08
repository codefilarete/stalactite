package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

import org.gama.lang.Duo;
import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
public class PersisterListenerTest {
	
	@Test
	public void doWithSelectListener() {
		PersisterListener testInstance = new PersisterListener();
		SelectListener listenerMock = mock(SelectListener.class);
		testInstance.addSelectListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		assertEquals(result, testInstance.doWithSelectListener(entities, () -> result));
		
		verify(listenerMock).beforeSelect(eq(entities));
		verify(listenerMock).afterSelect(eq(result));
	}
	
	@Test
	public void doWithSelectListener_onError() {
		PersisterListener testInstance = new PersisterListener();
		SelectListener listenerMock = mock(SelectListener.class);
		testInstance.addSelectListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		RuntimeException thrownException = assertThrows(RuntimeException.class, () -> testInstance.doWithSelectListener(entities, () -> { throw error; }));
		assertSame(error, thrownException);
		
		verify(listenerMock).beforeSelect(eq(entities));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterSelect(anyIterable());
	}
	
	@Test
	public void doWithInsertListener() {
		PersisterListener testInstance = new PersisterListener();
		InsertListener listenerMock = mock(InsertListener.class);
		testInstance.addInsertListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		assertEquals(result, testInstance.doWithInsertListener(entities, () -> result));
		
		verify(listenerMock).beforeInsert(eq(entities));
		verify(listenerMock).afterInsert(eq(result));
	}
	
	@Test
	public void doWithInsertListener_onError() {
		PersisterListener testInstance = new PersisterListener();
		InsertListener listenerMock = mock(InsertListener.class);
		testInstance.addInsertListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		RuntimeException thrownException = assertThrows(RuntimeException.class, () -> testInstance.doWithInsertListener(entities, () -> { throw error; }));
		assertSame(error, thrownException);
		
		verify(listenerMock).beforeInsert(eq(entities));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterInsert(anyIterable());
	}
	
	@Test
	public void doWithUpdateListener() {
		PersisterListener testInstance = new PersisterListener();
		UpdateListener listenerMock = mock(UpdateListener.class);
		testInstance.addUpdateListener(listenerMock);
		
		ArrayList<UpdatePayload> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		
		assertEquals(result, testInstance.doWithUpdateListener(entities, true, (p, b) -> result));
		
		verify(listenerMock).beforeUpdate(eq(entities), eq(true));
		verify(listenerMock).afterUpdate(eq(result), eq(true));
	}
	
	@Test
	public void doWithUpdateListener_onError() {
		PersisterListener testInstance = new PersisterListener();
		UpdateListener listenerMock = mock(UpdateListener.class);
		testInstance.addUpdateListener(listenerMock);
		
		ArrayList<Duo> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		RuntimeException thrownException = assertThrows(RuntimeException.class, () -> testInstance.doWithUpdateListener(entities, true, (p, b) -> { throw error; }));
		assertSame(error, thrownException);
		
		verify(listenerMock).beforeUpdate(eq(entities), eq(true));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterUpdate(anyIterable(), eq(true));
	}
	
	@Test
	public void doWithUpdateByIdListener() {
		PersisterListener testInstance = new PersisterListener();
		UpdateByIdListener listenerMock = mock(UpdateByIdListener.class);
		testInstance.addUpdateByIdListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		assertEquals(result, testInstance.doWithUpdateByIdListener(entities, () -> result));
		
		verify(listenerMock).beforeUpdateById(eq(entities));
		verify(listenerMock).afterUpdateById(eq(result));
	}
	
	@Test
	public void doWithUpdateByIdListener_onError() {
		PersisterListener testInstance = new PersisterListener();
		UpdateByIdListener listenerMock = mock(UpdateByIdListener.class);
		testInstance.addUpdateByIdListener(listenerMock);
		
		ArrayList<Duo> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		RuntimeException thrownException = assertThrows(RuntimeException.class, () -> testInstance.doWithUpdateByIdListener(entities, () -> { throw error; }));
		assertSame(error, thrownException);
		
		verify(listenerMock).beforeUpdateById(eq(entities));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterUpdateById(anyIterable());
	}
	
	@Test
	public void doWithDeleteListener() {
		PersisterListener testInstance = new PersisterListener();
		DeleteListener listenerMock = mock(DeleteListener.class);
		testInstance.addDeleteListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		assertEquals(result, testInstance.doWithDeleteListener(entities, () -> result));
		
		verify(listenerMock).beforeDelete(eq(entities));
		verify(listenerMock).afterDelete(eq(result));
	}
	
	@Test
	public void doWithDeleteListener_onError() {
		PersisterListener testInstance = new PersisterListener();
		DeleteListener listenerMock = mock(DeleteListener.class);
		testInstance.addDeleteListener(listenerMock);
		
		ArrayList<Duo> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		RuntimeException thrownException = assertThrows(RuntimeException.class, () -> testInstance.doWithDeleteListener(entities, () -> { throw error; }));
		assertSame(error, thrownException);
		
		verify(listenerMock).beforeDelete(eq(entities));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterDelete(anyIterable());
	}
	
	@Test
	public void doWithDeleteByIdListener() {
		PersisterListener testInstance = new PersisterListener();
		DeleteByIdListener listenerMock = mock(DeleteByIdListener.class);
		testInstance.addDeleteByIdListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		assertEquals(result, testInstance.doWithDeleteByIdListener(entities, () -> result));
		
		verify(listenerMock).beforeDeleteById(eq(entities));
		verify(listenerMock).afterDeleteById(eq(result));
	}
	
	@Test
	public void doWithDeleteByIdListener_onError() {
		PersisterListener testInstance = new PersisterListener();
		DeleteByIdListener listenerMock = mock(DeleteByIdListener.class);
		testInstance.addDeleteByIdListener(listenerMock);
		
		ArrayList<Duo> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		RuntimeException thrownException = assertThrows(RuntimeException.class, () -> testInstance.doWithDeleteByIdListener(entities, () -> { throw error; }));
		assertSame(error, thrownException);
		
		verify(listenerMock).beforeDeleteById(eq(entities));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterDeleteById(anyIterable());
	}
	
	@Test
	public void moveTo() {
		PersisterListener testInstance = new PersisterListener();
		PersisterListener targetInstance = new PersisterListener();
		
		InsertListener expectedInsertListener = Mockito.mock(InsertListener.class);
		UpdateListener expectedUpdateListener = Mockito.mock(UpdateListener.class);
		UpdateByIdListener expectedUpdateByIdListener = Mockito.mock(UpdateByIdListener.class);
		DeleteListener expectedDeleteListener = Mockito.mock(DeleteListener.class);
		DeleteByIdListener expectedDeleteByIdListener = Mockito.mock(DeleteByIdListener.class);
		SelectListener expectedSelectListener = Mockito.mock(SelectListener.class);
		testInstance.addInsertListener(expectedInsertListener);
		testInstance.addUpdateListener(expectedUpdateListener);
		testInstance.addUpdateByIdListener(expectedUpdateByIdListener);
		testInstance.addDeleteListener(expectedDeleteListener);
		testInstance.addDeleteByIdListener(expectedDeleteByIdListener);
		testInstance.addSelectListener(expectedSelectListener);
		
		testInstance.moveTo(targetInstance);
		
		// event listeners must not be invoked when original instance trigger events
		List<Object> entities = Arrays.asList(new Object());
		testInstance.doWithInsertListener(entities, () -> 1);
		Mockito.verify(expectedInsertListener, times(0)).beforeInsert(eq(entities));
		Mockito.verify(expectedInsertListener, times(0)).afterInsert(eq(entities));
		List<Duo<Object, Object>> updatableEntities = Arrays.asList(new Duo<>(new Object(), new Object()));
		testInstance.doWithUpdateListener(updatableEntities, true, (d, b) -> Void.class);
		Mockito.verify(expectedUpdateListener, times(0)).beforeUpdate(eq(updatableEntities), eq(true));
		Mockito.verify(expectedUpdateListener, times(0)).afterUpdate(eq(updatableEntities), eq(true));
		testInstance.doWithUpdateByIdListener(entities, () -> 1);
		Mockito.verify(expectedUpdateByIdListener, times(0)).beforeUpdateById(eq(entities));
		Mockito.verify(expectedUpdateByIdListener, times(0)).afterUpdateById(eq(entities));
		testInstance.doWithDeleteListener(entities, () -> 1);
		Mockito.verify(expectedDeleteListener, times(0)).beforeDelete(eq(entities));
		Mockito.verify(expectedDeleteListener, times(0)).afterDelete(eq(entities));
		testInstance.doWithDeleteByIdListener(entities, () -> 1);
		Mockito.verify(expectedDeleteByIdListener, times(0)).beforeDeleteById(eq(entities));
		Mockito.verify(expectedDeleteByIdListener, times(0)).afterDeleteById(eq(entities));
		List<Object> entitiesIds = Arrays.asList(new Object());
		List loadedEntities = new ArrayList();
		testInstance.doWithSelectListener(entitiesIds, () -> loadedEntities);
		Mockito.verify(expectedSelectListener, times(0)).beforeSelect(eq(entitiesIds));
		Mockito.verify(expectedSelectListener, times(0)).afterSelect(eq(loadedEntities));
		
		// event listeners must be invoked when target instance trigger events
		targetInstance.doWithInsertListener(entities, () -> 1);
		Mockito.verify(expectedInsertListener).beforeInsert(eq(entities));
		Mockito.verify(expectedInsertListener).afterInsert(eq(entities));
		targetInstance.doWithUpdateListener(updatableEntities, true, (d, b) -> Void.class);
		Mockito.verify(expectedUpdateListener).beforeUpdate(eq(updatableEntities), eq(true));
		Mockito.verify(expectedUpdateListener).afterUpdate(eq(updatableEntities), eq(true));
		targetInstance.doWithUpdateByIdListener(entities, () -> 1);
		Mockito.verify(expectedUpdateByIdListener).beforeUpdateById(eq(entities));
		Mockito.verify(expectedUpdateByIdListener).afterUpdateById(eq(entities));
		targetInstance.doWithDeleteListener(entities, () -> 1);
		Mockito.verify(expectedDeleteListener).beforeDelete(eq(entities));
		Mockito.verify(expectedDeleteListener).afterDelete(eq(entities));
		targetInstance.doWithDeleteByIdListener(entities, () -> 1);
		Mockito.verify(expectedDeleteByIdListener).beforeDeleteById(eq(entities));
		Mockito.verify(expectedDeleteByIdListener).afterDeleteById(eq(entities));
		targetInstance.doWithSelectListener(entitiesIds, () -> loadedEntities);
		Mockito.verify(expectedSelectListener).beforeSelect(eq(entitiesIds));
		Mockito.verify(expectedSelectListener).afterSelect(eq(loadedEntities));
		

	}
}