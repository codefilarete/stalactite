package org.gama.stalactite.persistence.id;

import java.util.HashSet;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.Diff;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.State;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
		Set<Diff> expectedResult = new HashSet<>();
		expectedResult.add(new Diff(State.ADDED, null, testData.country4));
		expectedResult.add(new Diff(State.ADDED, null, testData.country5));
		expectedResult.add(new Diff(State.REMOVED, testData.country1, null));
		expectedResult.add(new Diff(State.REMOVED, testData.country2, null));
		expectedResult.add(new Diff(State.HELD, testData.country3, testData.country3Clone));
		
		return new Object[][] {
				{
						Arrays.asHashSet(testData.country1, testData.country2, testData.country3),
						Arrays.asHashSet(testData.country3Clone, testData.country4, testData.country5),
						expectedResult
				},
				// corner cases with empty sets
				{
						Arrays.asHashSet(),
						Arrays.asHashSet(testData.country1),
						Arrays.asHashSet(new Diff(State.ADDED, null, testData.country1))
				},
				{
						Arrays.asHashSet(testData.country1),
						Arrays.asHashSet(),
						Arrays.asHashSet(new Diff(State.REMOVED, testData.country1, null))
				},
				{
						Arrays.asHashSet(),
						Arrays.asHashSet(),
						Arrays.asHashSet()
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
}
