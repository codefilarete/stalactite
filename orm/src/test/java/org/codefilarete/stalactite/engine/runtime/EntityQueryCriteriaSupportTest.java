package org.codefilarete.stalactite.engine.runtime;

import java.util.Comparator;
import java.util.TreeSet;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.runtime.EntityQueryCriteriaSupport.EntityQueryPageSupport.OrderByItem;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EntityQueryCriteriaSupportTest {
	
	@Test
	void buildComparator_singleDirectProperty() {
		KeepOrderSet<OrderByItem> orderBy = new KeepOrderSet<>(
				new OrderByItem(Arrays.asList(new AccessorByMethodReference<>(Country::getName)), Order.DESC, false)
		);
		Comparator<Country> objectComparator = EntityQueryCriteriaSupport.buildComparator(orderBy);
		Country country1 = new Country(1);
		country1.setName("A");
		Country country2 = new Country(2);
		country2.setName("B");
		Country country3 = new Country(3);
		country3.setName("C");
		TreeSet<Country> objects = new TreeSet<>(objectComparator);
		objects.addAll(Arrays.asHashSet(country2, country3, country1));
		
		assertThat(objects).containsExactly(country3, country2, country1);
	}
	
	@Test
	void buildComparator_severalDirectProperty() {
		KeepOrderSet<OrderByItem> orderBy = new KeepOrderSet<>(
				new OrderByItem(Arrays.asList(new AccessorByMethodReference<>(Country::getName)), Order.DESC, false),
				new OrderByItem(Arrays.asList(new AccessorByMethodReference<>(Country::getDescription)), Order.ASC, false)
		);
		Comparator<Country> objectComparator = EntityQueryCriteriaSupport.buildComparator(orderBy);
		Country country1 = new Country(1);
		country1.setName("A");
		country1.setDescription("A");
		Country country2 = new Country(2);
		country2.setName("B");
		country2.setDescription("B");
		Country country3 = new Country(3);
		country3.setName("C");
		country3.setDescription("C");
		Country country4 = new Country(4);
		country4.setName("A");
		country4.setDescription("B");
		TreeSet<Country> objects = new TreeSet<>(objectComparator);
		objects.addAll(Arrays.asHashSet(country2, country3, country1, country4));
		
		assertThat(objects).containsExactly(country3, country2, country1, country4);
	}
	
	@Test
	void buildComparator_severalCombinedProperty() {
		KeepOrderSet<OrderByItem> orderBy = new KeepOrderSet<>(
				new OrderByItem(Arrays.asList(new AccessorByMethodReference<>(Country::getName)), Order.DESC, false),
				new OrderByItem(Arrays.asList(new AccessorByMethodReference<>(Country::getId), new AccessorByMethodReference<Identifier, Object>(Identifier::getSurrogate)), Order.ASC, false)
		);
		Comparator<Country> objectComparator = EntityQueryCriteriaSupport.buildComparator(orderBy);
		Country country1 = new Country(1);
		country1.setName("A");
		Country country2 = new Country(2);
		country2.setName("B");
		Country country3 = new Country(3);
		country3.setName("C");
		Country country4 = new Country(4);
		country4.setName("A");
		TreeSet<Country> objects = new TreeSet<>(objectComparator);
		objects.addAll(Arrays.asHashSet(country2, country3, country1, country4));
		
		assertThat(objects).containsExactly(country3, country2, country1, country4);
	}
	
	@Test
	void buildComparator_severalCombinedSetter() {
		KeepOrderSet<OrderByItem> orderBy = new KeepOrderSet<>(
				new OrderByItem(Arrays.asList(new MutatorByMethodReference<>(Country::setName)), Order.DESC, false),
				new OrderByItem(Arrays.asList(new AccessorByMethodReference<>(Country::getCapital), new MutatorByMethodReference<>(City::setName)), Order.ASC, false)
		);
		Comparator<Country> objectComparator = EntityQueryCriteriaSupport.buildComparator(orderBy);
		Country country1 = new Country(1);
		country1.setName("A");
		City cityA = new City(1);
		cityA.setName("A");
		country1.setCapital(cityA);
		Country country2 = new Country(2);
		country2.setName("B");
		City cityB = new City(2);
		cityB.setName("B");
		country2.setCapital(cityB);
		Country country3 = new Country(3);
		country3.setName("C");
		City cityC = new City(3);
		cityC.setName("C");
		country3.setCapital(cityC);
		Country country4 = new Country(4);
		country4.setName("A");
		City cityD = new City(4);
		cityD.setName("D");
		country4.setCapital(cityD);
		TreeSet<Country> objects = new TreeSet<>(objectComparator);
		objects.addAll(Arrays.asHashSet(country2, country3, country1, country4));
		
		assertThat(objects).containsExactly(country3, country2, country1, country4);
	}
	
	@Test
	void buildComparator_severalCombinedSetter_withNullValue() {
		KeepOrderSet<OrderByItem> orderBy = new KeepOrderSet<>(
				new OrderByItem(Arrays.asList(new MutatorByMethodReference<>(Country::setName)), Order.DESC, false),
				new OrderByItem(Arrays.asList(new AccessorByMethodReference<>(Country::getCapital), new MutatorByMethodReference<>(City::setName)), Order.ASC, false)
		);
		Comparator<Country> objectComparator = EntityQueryCriteriaSupport.buildComparator(orderBy);
		Country country1 = new Country(1);
		country1.setName("A");
		City cityA = new City(1);
		cityA.setName("A");
		country1.setCapital(cityA);
		Country country2 = new Country(2);
		// Country2 has no name
		City cityB = new City(2);
		cityB.setName("B");
		country2.setCapital(cityB);
		Country country3 = new Country(3);
		country3.setName("C");
		City cityC = new City(3);
		cityC.setName("C");
		country3.setCapital(cityC);
		// Country 4 has no city
		Country country4 = new Country(4);
		country4.setName("A");
		TreeSet<Country> objects = new TreeSet<>(objectComparator);
		objects.addAll(Arrays.asHashSet(country2, country3, country1, country4));
		
		// country2 appears first because it has null name and nulls are put last while ordering but direction is DESC
		// country4 appears last because it has same name as country1, and it has no city, which is considered null, hence put last with ASC direction
		assertThat(objects).containsExactly(country2, country3, country1, country4);
	}
}