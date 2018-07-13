package org.gama.stalactite.persistence.id;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Functions;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.Diff;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.IndexedDiff;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.gama.lang.collection.Arrays.asHashSet;
import static org.gama.lang.collection.Arrays.asList;
import static org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.State.ADDED;
import static org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.State.HELD;
import static org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.State.REMOVED;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class IdentifiedCollectionDifferTest {
	
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
						Arrays.asHashSet(new Diff(ADDED, null, testData.country4),
										new Diff(ADDED, null, testData.country5),
										new Diff(REMOVED, testData.country1, null),
										new Diff(REMOVED, testData.country2, null),
										new Diff(HELD, testData.country3, testData.country3Clone))
				},
				// corner cases with empty sets
				{
						asHashSet(),
						asHashSet(testData.country1),
						asHashSet(new Diff(ADDED, null, testData.country1))
				},
				{
						asHashSet(testData.country1),
						asHashSet(),
						asHashSet(new Diff(REMOVED, testData.country1, null))
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
	public void testDiffSet(Set<Country> set1, Set<Country> set2, Set<Diff> expectedResult) {
		IdentifiedCollectionDiffer testInstance = new IdentifiedCollectionDiffer();
		
		Set<Diff> diffs = testInstance.diffSet(set1, set2);
		
		assertEquals(expectedResult, diffs);
	}
	
	public static Object[][] testDiffList() {
		TestData testData = new TestData();
		return new Object[][] {
				{
						asList(testData.country1, testData.country2),
						asList(testData.country2, testData.country1),
						asHashSet(new Diff(HELD, testData.country1, testData.country1),
								new Diff(HELD, testData.country2, testData.country2))
				},
				{
						asList(testData.country1),
						asList(testData.country1, testData.country2),
						asHashSet(new IndexedDiff(HELD, testData.country1, testData.country1),
								new IndexedDiff(ADDED, null, testData.country2))
				},
				{
						asList(testData.country1, testData.country2),
						asList(testData.country1),
						asHashSet(new IndexedDiff(HELD, testData.country1, testData.country1),
								new IndexedDiff(REMOVED, testData.country2, null))
				},
				// corner cases with empty sets
				{
						asList(),
						asList(testData.country1),
						asHashSet(new IndexedDiff(ADDED, null, testData.country1))
				},
				{
						asList(testData.country1),
						asList(),
						asHashSet(new IndexedDiff(REMOVED, testData.country1, null))
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
	public void testDiffList(List<Country> set1, List<Country> set2, Set<Diff> expectedResult) {
		IdentifiedCollectionDiffer testInstance = new IdentifiedCollectionDiffer();
		
		Set<IndexedDiff> diffs = testInstance.diffList(set1, set2);
		
		TreeSet<Diff> treeSet1 = new TreeSet<>(STATE_THEN_SOURCE_COMPARATOR);
		treeSet1.addAll(diffs);
		TreeSet<Diff> treeSet2 = new TreeSet<>(STATE_THEN_SOURCE_COMPARATOR);
		treeSet2.addAll(expectedResult);
		assertEquals(toString(treeSet2), toString(treeSet1));
	}
	
	private static String toString(Iterable<Diff> diffs) {
		return Iterables.stream(diffs).map(IdentifiedCollectionDifferTest::toString).collect(Collectors.joining(", "));
	}
	
	private static String toString(Diff diff) {
		return "" + diff.getState() + " " + diff.getSourceInstance() + " " + diff.getReplacingInstance();
	}
	
	private static Function<Diff, Comparable> SOURCE_IDENTIFIER_GETTER = Functions.link(Diff::getSourceInstance, Identified::getId)
					.andThen(statefullIdentifier -> (Comparable) statefullIdentifier.getSurrogate());
	private static Comparator<Diff> STATE_THEN_SOURCE_COMPARATOR = Comparator.comparing(Diff::getState).thenComparing(SOURCE_IDENTIFIER_GETTER, Comparator.nullsFirst(Comparator.naturalOrder()));
}
