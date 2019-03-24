package org.gama.stalactite.persistence.id.diff;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Functions;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.gama.lang.collection.Arrays.asHashSet;
import static org.gama.lang.collection.Arrays.asList;
import static org.gama.lang.collection.Arrays.asSet;
import static org.gama.stalactite.persistence.id.diff.State.ADDED;
import static org.gama.stalactite.persistence.id.diff.State.HELD;
import static org.gama.stalactite.persistence.id.diff.State.REMOVED;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class CollectionDifferTest {
	
	
	private static String toString(Iterable<? extends AbstractDiff> diffs) {
		return Iterables.stream(diffs).map(CollectionDifferTest::toString).collect(Collectors.joining(", "));
	}
	
	private static String toString(AbstractDiff diff) {
		return "" + diff.getState() + " " + diff.getSourceInstance() + " vs " + diff.getReplacingInstance()
				+ (diff instanceof IndexedDiff ? " [ " + ((IndexedDiff) diff).getSourceIndexes() + " vs " + ((IndexedDiff) diff).getReplacerIndexes() + " ]" : "");
	}
	
	private static final Function<AbstractDiff<? extends Identified>, ? extends Identified> GET_SOURCE_INSTANCE = AbstractDiff::getSourceInstance;
	private static final Function<AbstractDiff<? extends Identified>, ? extends Identified> GET_REPLACING_INSTANCE = AbstractDiff::getReplacingInstance;
	private static final Function<AbstractDiff<? extends Identified>, State> GET_STATE = AbstractDiff::getState;
	
	private static final Function<AbstractDiff<? extends Identified>, Comparable> SOURCE_ID_GETTER = Functions.link(GET_SOURCE_INSTANCE, Identified::getId)
			.andThen(statefullIdentifier -> (Comparable) statefullIdentifier.getSurrogate());
	private static final Function<AbstractDiff<? extends Identified>, Comparable> REPLACING_ID_GETTER = Functions.link(GET_REPLACING_INSTANCE, Identified::getId)
			.andThen(statefullIdentifier -> (Comparable) statefullIdentifier.getSurrogate());
	private static final Comparator<AbstractDiff<? extends Identified>> COMPARING = Comparator.comparing(GET_STATE);
	private static final Comparator<AbstractDiff<? extends Identified>> STATE_THEN_INSTANCES_COMPARATOR = COMPARING
			.thenComparing(SOURCE_ID_GETTER, Comparator.nullsFirst(Comparator.naturalOrder()))
			.thenComparing(REPLACING_ID_GETTER, Comparator.nullsFirst(Comparator.naturalOrder()));
	
	private static class TestData {
		
		private final LongProvider longProvider = new LongProvider();
		private final Country country1 = new Country(longProvider.giveNewIdentifier());
		private final Country country2 = new Country(longProvider.giveNewIdentifier());
		private final Country country3 = new Country(longProvider.giveNewIdentifier());
		private final Country country3Clone = new Country(country3.getId());
		private final Country country4 = new Country(longProvider.giveNewIdentifier());
		private final Country country5 = new Country(longProvider.giveNewIdentifier());
		
		private TestData() {
			country1.setName("France");
			country2.setName("Spain");
			country3.setName("Italy");
			country3Clone.setName(country3.getName() + " changed");
			country4.setName("England");
			country5.setName("Germany");
		}
	}
	
	public static Object[][] testDiffSet() {
		TestData testData = new TestData();
		return new Object[][] {
				{
						asHashSet(testData.country1, testData.country2, testData.country3),
						asHashSet(testData.country3Clone, testData.country4, testData.country5),
						Arrays.asHashSet(new Diff<>(ADDED, null, testData.country4),
										new Diff<>(ADDED, null, testData.country5),
										new Diff<>(REMOVED, testData.country1, null),
										new Diff<>(REMOVED, testData.country2, null),
										new Diff<>(HELD, testData.country3, testData.country3Clone))
				},
				// corner cases with empty sets
				{
						asHashSet(),
						asHashSet(testData.country1),
						asHashSet(new Diff<>(ADDED, null, testData.country1))
				},
				{
						asHashSet(testData.country1),
						asHashSet(),
						asHashSet(new Diff<>(REMOVED, testData.country1, null))
				},
				{
						asHashSet(),
						asHashSet(),
						asHashSet()
				}
		};
	}
	
	@ParameterizedTest
	@MethodSource("testDiffSet")
	public void testDiffSet(Set<Country> set1, Set<Country> set2, Set<Diff<Country>> expectedResult) {
		CollectionDiffer<Country> testInstance = new CollectionDiffer<>(Country::getId);
		
		Set<Diff<Country>> diffs = testInstance.diffSet(set1, set2);
		
		// we must use a comparator to ensure same order then use a ToString, because the default solution of using assertEquals(..) needs
		// an implementation of equals(..) and hashCode() which would have been made only for testing purpose
		TreeSet<Diff<Country>> sortedExpectation = Arrays.asTreeSet(STATE_THEN_INSTANCES_COMPARATOR, expectedResult);
		TreeSet<Diff<Country>> sortedResult = Arrays.asTreeSet(STATE_THEN_INSTANCES_COMPARATOR, diffs);
		
		assertEquals(toString(sortedExpectation), toString(sortedResult));
	}
	
	public static Object[][] testDiffList() {
		TestData testData = new TestData();
		return new Object[][] {
				{
						asList(testData.country1, testData.country2, testData.country3),
						asList(testData.country2, testData.country1, testData.country3),
						asHashSet(new IndexedDiff(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addReplacerIndex(1),
								new IndexedDiff(HELD, testData.country2, testData.country2)
										.addSourceIndex(1).addReplacerIndex(0),
								new IndexedDiff(HELD, testData.country3, testData.country3)
										.addSourceIndex(2).addReplacerIndex(2))
				},
				{
						asList(testData.country1),
						asList(testData.country1, testData.country2),
						asHashSet(new IndexedDiff(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addReplacerIndex(0),
								new IndexedDiff(ADDED, null, testData.country2)
										.addReplacerIndex(1))
				},
				{
						asList(testData.country1, testData.country2),
						asList(testData.country1),
						asHashSet(new IndexedDiff(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addReplacerIndex(0),
								new IndexedDiff(REMOVED, testData.country2, null)
										.addSourceIndex(1))
				},
				// with duplicates ...
				// ... one removed
				{
						asList(testData.country1, testData.country2, testData.country1),
						asList(testData.country1),
						asHashSet(new IndexedDiff(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addReplacerIndex(0),
								new IndexedDiff(REMOVED, testData.country2, null)
										.addSourceIndex(1),
								new IndexedDiff(REMOVED, testData.country1, null)
										.addSourceIndex(2))
				},
				// ... none removed
				{
						asList(testData.country1, testData.country2, testData.country1),
						asList(testData.country1, testData.country1),
						asHashSet(new IndexedDiff(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addSourceIndex(2).addReplacerIndex(0).addReplacerIndex(1),
								new IndexedDiff(REMOVED, testData.country2, null)
										.addSourceIndex(1))
				},
				// ... all removed
				{
						asList(testData.country1, testData.country2, testData.country1),
						asList(testData.country2),
						asHashSet(new IndexedDiff(REMOVED, testData.country1, null)
										.addSourceIndex(0).addSourceIndex(2),
								new IndexedDiff(HELD, testData.country2, testData.country2)
										.addSourceIndex(1).addReplacerIndex(0))
				},
				// ... none removed but some added
				{
						asList(testData.country1, testData.country2, testData.country1),
						asList(testData.country1, testData.country1, testData.country1, testData.country1),
						asHashSet(new IndexedDiff(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addSourceIndex(2).addReplacerIndex(0).addReplacerIndex(1),
								new IndexedDiff(ADDED, null, testData.country1)
										.addReplacerIndex(2).addReplacerIndex(3),
								new IndexedDiff(REMOVED, testData.country2, null)
										.addSourceIndex(1))
				},
				// ... none removed but some added
				{
						asList(testData.country1, testData.country2, testData.country1),
						asList(testData.country1, testData.country1, testData.country1, testData.country1, testData.country2, testData.country2),
						asHashSet(new IndexedDiff(HELD, testData.country1, testData.country1)
										.addSourceIndex(0).addSourceIndex(2).addReplacerIndex(0).addReplacerIndex(1),
								new IndexedDiff(ADDED, null, testData.country1)
										.addReplacerIndex(2).addReplacerIndex(3),
								new IndexedDiff(HELD, testData.country2, testData.country2)
										.addSourceIndex(1).addReplacerIndex(4),
								new IndexedDiff(ADDED, null, testData.country2)
										.addReplacerIndex(5))
				},
				// corner cases with empty sets
				{
						asList(),
						asList(testData.country1),
						asHashSet(new IndexedDiff(ADDED, null, testData.country1)
										.addReplacerIndex(0))
				},
				{
						asList(testData.country1),
						asList(),
						asHashSet(new IndexedDiff(REMOVED, testData.country1, null)
										.addSourceIndex(0))
				},
				{
						asList(),
						asList(),
						asHashSet()
				}
		};
	}
	
	@ParameterizedTest
	@MethodSource("testDiffList")
	public void testDiffList(List<Country> set1, List<Country> set2, Set<IndexedDiff<Country>> expectedResult) {
		CollectionDiffer<Country> testInstance = new CollectionDiffer<>(Country::getId);
		
		Set<IndexedDiff<Country>> diffs = testInstance.diffList(set1, set2);
		
		// we must use a comparator to ensure same order then use a ToString, because the default solution of using assertEquals(..) needs
		// an implementation of equals(..) and hashCode() which would have been made only for testing purpose
		TreeSet<IndexedDiff<Country>> treeSet1 = Arrays.asTreeSet(STATE_THEN_INSTANCES_COMPARATOR, diffs);
		TreeSet<IndexedDiff<Country>> treeSet2 = Arrays.asTreeSet(STATE_THEN_INSTANCES_COMPARATOR, expectedResult);
		assertEquals(toString(treeSet2), toString(treeSet1));
	}
	
	public static Object[][] testLookupIndexes() {
		City city1 = new City(new PersistedIdentifier<>(1L));
		City city2 = new City(new PersistedIdentifier<>(2L));
		return new Object[][] {
				{ asList(city1, city2), city1, asSet(0) },
				{ asList(city1, city2), city2, asSet(1) },
				{ asList(city1, city2, city2), city2, asSet(1, 2) },
				{ asList(city1, city2, city1, city2), city2, asSet(1, 3) },
				{ asList(city1, city2, city1, city2), city1, asSet(0, 2) },
				{ asList(), city1, asSet() },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testLookupIndexes")
	public void testLookupIndexes(List<City> input, City lookupElement, Set<Integer> expected) {
		CollectionDiffer<Country> testInstance = new CollectionDiffer<>(Country::getId);
		Set<Integer> result = testInstance.lookupIndexes(input, lookupElement);
		assertEquals(expected, result);
	}
}
