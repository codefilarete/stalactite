package org.codefilarete.stalactite.persistence.engine.listener;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.stalactite.persistence.engine.listener.UpdateListener.UpdatePayload;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
public class PersisterListenerCollectionTest {
	
	@Test
	public void doWithSelectListener() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		SelectListener listenerMock = mock(SelectListener.class);
		testInstance.addSelectListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		assertThat(testInstance.doWithSelectListener(entities, () -> result)).isEqualTo(result);
		
		verify(listenerMock).beforeSelect(eq(entities));
		verify(listenerMock).afterSelect(eq(result));
	}
	
	@Test
	public void doWithSelectListener_onError() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		SelectListener listenerMock = mock(SelectListener.class);
		testInstance.addSelectListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		assertThatThrownBy(() -> testInstance.doWithSelectListener(entities, () -> {
					throw error;
				})).isSameAs(error);
		
		verify(listenerMock).beforeSelect(eq(entities));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterSelect(anyIterable());
	}
	
	@Test
	public void doWithInsertListener() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		InsertListener listenerMock = mock(InsertListener.class);
		testInstance.addInsertListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		assertThat(testInstance.doWithInsertListener(entities, () -> result)).isEqualTo(result);
		
		verify(listenerMock).beforeInsert(eq(entities));
		verify(listenerMock).afterInsert(eq(result));
	}
	
	@Test
	public void doWithInsertListener_onError() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		InsertListener listenerMock = mock(InsertListener.class);
		testInstance.addInsertListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		assertThatThrownBy(() -> testInstance.doWithInsertListener(entities, () -> {
			throw error;
		})).isSameAs(error);
		
		verify(listenerMock).beforeInsert(eq(entities));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterInsert(anyIterable());
	}
	
	@Test
	public void doWithUpdateListener() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		UpdateListener listenerMock = mock(UpdateListener.class);
		testInstance.addUpdateListener(listenerMock);
		
		ArrayList<UpdatePayload> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		
		assertThat(testInstance.doWithUpdateListener(entities, true, (p, b) -> result)).isEqualTo(result);
		
		verify(listenerMock).beforeUpdate(eq(entities), eq(true));
		verify(listenerMock).afterUpdate(eq(result), eq(true));
	}
	
	@Test
	public void doWithUpdateListener_onError() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		UpdateListener listenerMock = mock(UpdateListener.class);
		testInstance.addUpdateListener(listenerMock);
		
		ArrayList<Duo> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		assertThatThrownBy(() -> testInstance.doWithUpdateListener(entities, true, (BiConsumer<Iterable<? extends Duo>, Boolean>) (p, b) -> {
			throw error;
		})).isSameAs(error);
		
		verify(listenerMock).beforeUpdate(eq(entities), eq(true));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterUpdate(anyIterable(), eq(true));
	}
	
	@Test
	public void doWithUpdateByIdListener() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		UpdateByIdListener listenerMock = mock(UpdateByIdListener.class);
		testInstance.addUpdateByIdListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		assertThat(testInstance.doWithUpdateByIdListener(entities, () -> result)).isEqualTo(result);
		
		verify(listenerMock).beforeUpdateById(eq(entities));
		verify(listenerMock).afterUpdateById(eq(result));
	}
	
	@Test
	public void doWithUpdateByIdListener_onError() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		UpdateByIdListener listenerMock = mock(UpdateByIdListener.class);
		testInstance.addUpdateByIdListener(listenerMock);
		
		ArrayList<Duo> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		assertThatThrownBy(() -> testInstance.doWithUpdateByIdListener(entities, () -> {
			throw error;
		})).isSameAs(error);
		
		verify(listenerMock).beforeUpdateById(eq(entities));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterUpdateById(anyIterable());
	}
	
	@Test
	public void doWithDeleteListener() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		DeleteListener listenerMock = mock(DeleteListener.class);
		testInstance.addDeleteListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		assertThat(testInstance.doWithDeleteListener(entities, () -> result)).isEqualTo(result);
		
		verify(listenerMock).beforeDelete(eq(entities));
		verify(listenerMock).afterDelete(eq(result));
	}
	
	@Test
	public void doWithDeleteListener_onError() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		DeleteListener listenerMock = mock(DeleteListener.class);
		testInstance.addDeleteListener(listenerMock);
		
		ArrayList<Duo> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		assertThatThrownBy(() -> testInstance.doWithDeleteListener(entities, () -> {
			throw error;
		})).isSameAs(error);
		
		verify(listenerMock).beforeDelete(eq(entities));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterDelete(anyIterable());
	}
	
	@Test
	public void doWithDeleteByIdListener() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		DeleteByIdListener listenerMock = mock(DeleteByIdListener.class);
		testInstance.addDeleteByIdListener(listenerMock);
		
		ArrayList<Object> entities = new ArrayList<>();
		ArrayList<Object> result = new ArrayList<>();
		assertThat(testInstance.doWithDeleteByIdListener(entities, () -> result)).isEqualTo(result);
		
		verify(listenerMock).beforeDeleteById(eq(entities));
		verify(listenerMock).afterDeleteById(eq(result));
	}
	
	@Test
	public void doWithDeleteByIdListener_onError() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		DeleteByIdListener listenerMock = mock(DeleteByIdListener.class);
		testInstance.addDeleteByIdListener(listenerMock);
		
		ArrayList<Duo> entities = new ArrayList<>();
		RuntimeException error = new RuntimeException("This is the expected exception to be thrown");
		assertThatThrownBy(() -> testInstance.doWithDeleteByIdListener(entities, () -> {
			throw error;
		})).isSameAs(error);
		
		verify(listenerMock).beforeDeleteById(eq(entities));
		verify(listenerMock).onError(eq(entities), eq(error));
		verify(listenerMock, never()).afterDeleteById(anyIterable());
	}
	
	@Test
	public void moveTo() {
		PersisterListenerCollection testInstance = new PersisterListenerCollection();
		PersisterListenerCollection targetInstance = new PersisterListenerCollection();
		
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