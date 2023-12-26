package org.codefilarete.stalactite.engine.runtime.cycle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractCycleLoaderTest {
	
	/**
	 * Test made to ensure that {@link AbstractCycleLoader} doesn't fall into infinite loop (ending with {@link StackOverflowError})
	 * when Persister is the same as the one that triggered {@link AbstractCycleLoader#afterSelect(Set)}, because it also calls persister.select(..)
	 * which will trigger afterSelect(..) again (through SelectListener), etc.
	 */
	static Object[][] afterSelect_withSamePersisterAsTrigger_databaseIsATree_doesntFallIntoInfiniteLoop() {
		return new Object[][]{
				{
						// Database is a tree
						Arrays.asList(
								Maps.forHashMap(String.class, Object.class)
										.add("id", 0)
										.add("name", "Root")
										.add("childId", 1),
								Maps.forHashMap(String.class, Object.class)
										.add("id", 1)
										.add("name", "Root.1")
										.add("childId", 2),
								Maps.forHashMap(String.class, Object.class)
										.add("id", 2)
										.add("name", "Root.1.1")
										.add("childId", 3))
				},
				{
						// Database is a cycle
						Arrays.asList(
								Maps.forHashMap(String.class, Object.class)
										.add("id", 0)
										.add("name", "Root")
										.add("childId", 1),
								Maps.forHashMap(String.class, Object.class)
										.add("id", 1)
										.add("name", "Root.1")
										.add("childId", 2),
								Maps.forHashMap(String.class, Object.class)
										.add("id", 2)
										.add("name", "Root.1.1")
										.add("childId", 3),
								// we add a cycle between a root child (a deep one) and root 
								Maps.forHashMap(String.class, Object.class)
										.add("id", 2)
										.add("name", "Root.1.1")
										.add("childId", 0)
						)
				}
		};
	}
	
	@ParameterizedTest
	@MethodSource
	void afterSelect_withSamePersisterAsTrigger_databaseIsATree_doesntFallIntoInfiniteLoop(List<Map<String, Object>> pseudoDatabase) {
		EntityPersister<String, Integer> persister = mock(EntityPersister.class);
		when(persister.getId(any())).thenAnswer((Answer<Integer>) invocation -> {
			String entity = invocation.getArgument(0);
			Optional<Map<String, Object>> foundRow = pseudoDatabase.stream().filter(row -> row.get("name").equals(entity)).findFirst();
			if (foundRow.isPresent()) {
				return (Integer) foundRow.get().get("id");
			} else {
				// should not happen
				throw new NoSuchElementException("No identifier found for entity " + entity);
			}
		});
		
		List<Set<Integer>> executionStack = new ArrayList<>();
		
		EntityPersister<String, Integer> persisterWrapperWithBeforeAfterSelect = mock(EntityPersister.class);
		
		AbstractCycleLoader<String, String, Integer> testInstance = new AbstractCycleLoader<String, String, Integer>(persisterWrapperWithBeforeAfterSelect) {
			@Override
			protected void applyRelationToSource(EntityRelationStorage<String, Integer> targetIdsPerSource,
												 BeanRelationFixer<String, String> beanRelationFixer,
												 Map<Integer, String> targetPerId) {
			}
		};
		
		when(persisterWrapperWithBeforeAfterSelect.getId(any())).thenAnswer(invocation -> persister.getId(invocation.getArgument(0)));
		
		when(persisterWrapperWithBeforeAfterSelect.select(anyIterable())).thenAnswer(invocation -> {
			// we trigger beforeSelect(..) as runtime does through selectListener and doWithSelectListener that wraps persister.select(..)
			// just to mimic complete mechanism
			testInstance.beforeSelect(invocation.getArgument(0));
			
			Collection<Integer> idsToSelect = invocation.getArgument(0);
			Set<String> result = persister.select(idsToSelect);
			// we trigger afterSelect(..) as runtime does through selectListener and doWithSelectListener that wraps persister.select(..)
			// just to mimic complete mechanism
			testInstance.afterSelect(result);
			return result;
		});
		
		when(persister.select(anySet())).thenAnswer((Answer<Set<String>>) invocation -> {
			Set<Integer> idsToSelect = invocation.getArgument(0);
			// we store elements for test assertion
			executionStack.add(idsToSelect);
			
			// All following coded mimic a database read (data are took on pseudoDatabase)
			// as well as what runtime should do : invoke addRelationToInitialize(..° 
			CycleLoadRuntimeContext<String, Integer> cycleLoadRuntimeContext = testInstance.currentRuntimeContext.get();
			Set<Map<String, Object>> matchingRows = pseudoDatabase.stream()
					.filter(row -> idsToSelect.contains(row.get("id"))).collect(Collectors.toSet());
			Set<String> result = new HashSet<>();
			matchingRows.forEach(row -> {
				String name = (String) row.get("name");
				cycleLoadRuntimeContext.addRelationToInitialize("relation", name, (Integer) row.get("childId"));
				result.add(name);
			});
			
			return result;
		});
		
		// mimicking very first load (what is upstream AbstractCycleLoader) 
		testInstance.currentRuntimeContext.get().addRelationToInitialize("relation", "Root", 1);
		// triggering cycle (and test though)
		persisterWrapperWithBeforeAfterSelect.select(Arrays.asSet(0));
		
		// read ids must be in exact order
		assertThat(executionStack).containsExactly(Arrays.asSet(0), Arrays.asSet(1), Arrays.asSet(2), Arrays.asSet(3));
		
		// checking that memory context is clear
		assertThat(testInstance.currentlyLoadedEntityIdsInCycle.isPresent()).isFalse();
		assertThat(testInstance.currentRuntimeContext.isPresent()).isFalse();
		assertThat(testInstance.currentCycleCount.isPresent()).isFalse();
	}
}