package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
public class PersisterListenerTest {
	
	@Test
	public void doWithSelectListener() {
		PersisterListener testInstance = new PersisterListener();
		ISelectListener listenerMock = mock(ISelectListener.class);
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
		ISelectListener listenerMock = mock(ISelectListener.class);
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
		IInsertListener listenerMock = mock(IInsertListener.class);
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
		IInsertListener listenerMock = mock(IInsertListener.class);
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
		IUpdateListener listenerMock = mock(IUpdateListener.class);
		testInstance.addUpdateListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		assertEquals(result, testInstance.doWithUpdateListener(entities, true, () -> result));
		
		verify(listenerMock).beforeUpdate(eq(entities), eq(true));
		verify(listenerMock).afterUpdate(eq(result), eq(true));
	}
	
	@Test
	public void doWithUpdateListener_onError() {
		PersisterListener testInstance = new PersisterListener();
		IUpdateListener listenerMock = mock(IUpdateListener.class);
		testInstance.addUpdateListener(listenerMock);
		
		ArrayList<Entry> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		RuntimeException thrownException = assertThrows(RuntimeException.class, () -> testInstance.doWithUpdateListener(entities, true, () -> { throw error; }));
		assertSame(error, thrownException);
		
		verify(listenerMock).beforeUpdate(eq(entities), eq(true));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterUpdate(anyIterable(), anyBoolean());
	}
	
	@Test
	public void doWithUpdateByIdListener() {
		PersisterListener testInstance = new PersisterListener();
		IUpdateByIdListener listenerMock = mock(IUpdateByIdListener.class);
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
		IUpdateByIdListener listenerMock = mock(IUpdateByIdListener.class);
		testInstance.addUpdateByIdListener(listenerMock);
		
		ArrayList<Entry> entities = new ArrayList<>();
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
		IDeleteListener listenerMock = mock(IDeleteListener.class);
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
		IDeleteListener listenerMock = mock(IDeleteListener.class);
		testInstance.addDeleteListener(listenerMock);
		
		ArrayList<Entry> entities = new ArrayList<>();
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
		IDeleteByIdListener listenerMock = mock(IDeleteByIdListener.class);
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
		IDeleteByIdListener listenerMock = mock(IDeleteByIdListener.class);
		testInstance.addDeleteByIdListener(listenerMock);
		
		ArrayList<Entry> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		RuntimeException thrownException = assertThrows(RuntimeException.class, () -> testInstance.doWithDeleteByIdListener(entities, () -> { throw error; }));
		assertSame(error, thrownException);
		
		verify(listenerMock).beforeDeleteById(eq(entities));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterDeleteById(anyIterable());
	}
	
}