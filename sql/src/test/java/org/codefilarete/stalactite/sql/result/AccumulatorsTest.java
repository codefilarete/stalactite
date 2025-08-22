package org.codefilarete.stalactite.sql.result;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;

import org.codefilarete.stalactite.sql.result.ResultSetRowTransformerTest.Person;
import org.codefilarete.stalactite.sql.result.ResultSetRowTransformerTest.Vehicle;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class AccumulatorsTest {
	
	@Test
	void toCollection() {
		Accumulator<Person, Queue<Person>, Queue<Person>> testInstance = Accumulators.toCollection(ArrayDeque::new);
		Person titi = new Person("titi");
		Person tata = new Person("tata");
		Person toto = new Person("toto");
		Queue<Person> collect = testInstance.collect(Arrays.asHashSet(titi, tata, toto));
		assertThat(collect).containsExactlyInAnyOrder(titi, tata, toto);
	}
	
	@Test
	void toList() {
		Accumulator<Person, ? extends List<Person>, List<Person>> testInstance = Accumulators.toList();
		Person titi = new Person("titi");
		Person tata = new Person("tata");
		Person toto = new Person("toto");
		List<Person> collect = testInstance.collect(Arrays.asHashSet(titi, tata, toto));
		assertThat(collect).containsExactlyInAnyOrder(titi, tata, toto);
	}
	
	@Test
	void toUnmodifiableList() {
		Accumulator<Person, ? extends List<Person>, List<Person>> testInstance = Accumulators.toUnmodifiableList();
		Person titi = new Person("titi");
		Person tata = new Person("tata");
		Person toto = new Person("toto");
		List<Person> collect = testInstance.collect(Arrays.asHashSet(titi, tata, toto));
		assertThat(collect).containsExactlyInAnyOrder(titi, tata, toto);
		assertThat(collect).isUnmodifiable();
	}
	
	@Test
	void toSet() {
		Accumulator<Person, ? extends Set<Person>, Set<Person>> testInstance = Accumulators.toSet();
		Person titi = new Person("titi");
		Person tata = new Person("tata");
		Person toto = new Person("toto");
		Set<Person> collect = testInstance.collect(Arrays.asHashSet(titi, tata, toto));
		assertThat(collect).containsExactlyInAnyOrder(toto, titi, tata);
	}
	
	@Test
	void toUnmodifiableSet() {
		Accumulator<Person, ? extends Set<Person>, Set<Person>> testInstance = Accumulators.toUnmodifiableSet();
		Person titi = new Person("titi");
		Person tata = new Person("tata");
		Person toto = new Person("toto");
		Set<Person> collect = testInstance.collect(Arrays.asHashSet(titi, tata, toto));
		assertThat(collect).containsExactlyInAnyOrder(toto, titi, tata);
		assertThat(collect).isUnmodifiable();
	}
	
	@Test
	void toKeepingOrderSet() {
		Accumulator<Person, Set<Person>, Set<Person>> testInstance = Accumulators.toKeepingOrderSet();
		Person titi = new Person("titi");
		Person tata = new Person("tata");
		Person toto = new Person("toto");
		Set<Person> collect = testInstance.collect(Arrays.asList(titi, tata, toto));
		assertThat(collect).containsExactly(titi, tata, toto);
	}
	
	@Test
	void toSortedSet() {
		Accumulator<Person, NavigableSet<Person>, NavigableSet<Person>> testInstance = Accumulators.toSortedSet(Comparator.comparing(Person::getName));
		Person titi = new Person("titi");
		Person tata = new Person("tata");
		Person toto = new Person("toto");
		NavigableSet<Person> collect = testInstance.collect(Arrays.asHashSet(titi, tata, toto));
		assertThat(collect).containsExactly(tata, titi, toto);
	}
	
	@Test
	void groupingBy() {
		Accumulator<Person, ?, Map<String, Person>> testInstance = Accumulators.groupingBy(Person::getName);
		Person titi = new Person("titi");
		Person tata = new Person("tata");
		Person toto = new Person("toto");
		Map<String, Person> collect = testInstance.collect(Arrays.asHashSet(titi, tata, toto));
		assertThat(collect).containsExactlyInAnyOrderEntriesOf(Maps.forHashMap(String.class, Person.class)
				.add("titi", titi)
				.add("tata", tata)
				.add("toto", toto));
	}
	
	@Test
	void groupingBy_withMapping() {
		Accumulator<Vehicle, ?, Map<String, List<String>>> testInstance = Accumulators.groupingBy(Vehicle::getName, Accumulators.mapping(Vehicle::getColor, Accumulators.toList()));
		Vehicle titi1 = new Vehicle("titi");
		titi1.setColor("yellow");
		Vehicle titi2 = new Vehicle("titi");
		titi2.setColor("blue");
		Vehicle toto = new Vehicle("toto");
		toto.setColor("brown");
		Map<String, List<String>> collect = testInstance.collect(Arrays.asSet(titi1, titi2, toto));
		assertThat(collect).containsExactlyInAnyOrderEntriesOf(Maps.forHashMap(String.class, (Class<List<String>>) null)
				.add("titi", Arrays.asList("yellow", "blue"))
				.add("toto", Arrays.asList("brown")));
	}
	
	@Test
	void mapping() {
		Accumulator<Vehicle, ?, List<String>> testInstance = Accumulators.mapping(Vehicle::getColor, Accumulators.toList());
		Vehicle titi1 = new Vehicle("titi");
		titi1.setColor("yellow");
		Vehicle titi2 = new Vehicle("titi");
		titi2.setColor("blue");
		Vehicle toto = new Vehicle("toto");
		toto.setColor("brown");
		List<String> collect = testInstance.collect(Arrays.asSet(titi1, titi2, toto));
		assertThat(collect).containsExactlyInAnyOrder("yellow", "blue", "brown");
	}
	
	@Test
	void getFirst() {
		Accumulator<String, ?, String> testInstance = Accumulators.getFirst();
		String collect = testInstance.collect(Arrays.asList("tata", "titi", "toto"));
		assertThat(collect).isEqualTo("tata");
		collect = testInstance.collect(Arrays.asList(null, "titi", "toto"));
		assertThat(collect).isEqualTo("titi");
		collect = testInstance.collect(Arrays.asList(null, null, null));
		assertThat(collect).isNull();
	}
	
	@Test
	void getFirstUnique() {
		Accumulator<String, ?, String> testInstance = Accumulators.getFirstUnique();
		assertThatCode(() -> testInstance.collect(Arrays.asList("tata", "titi", "toto")))
				.hasMessage("Object was expected to be a lonely one but another object is present");
	}
}