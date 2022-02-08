package org.gama.stalactite.sql.result;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class BeanRelationFixerTest {
	
	@Test
	public void of_setter() {
		BeanRelationFixer<DummyTarget, String> testInstance = BeanRelationFixer.of(DummyTarget::setProp1);
		DummyTarget target = new DummyTarget();
		String input = "toto";
		testInstance.apply(target, input);
		assertThat(target.getProp1()).isEqualTo(input);
	}
	
	@Test
	public void of_setter_getter() {
		BeanRelationFixer<Country, President> testInstance = BeanRelationFixer.of(Country::setPresident, President::setCountry);
		Country target = new Country();
		President president = new President();
		testInstance.apply(target, president);
		assertThat(target.getPresident()).isEqualTo(president);
		assertThat(president.getCountry()).isEqualTo(target);
	}
	
	@Test
	public void of_setter_getter_collection() {
		BeanRelationFixer<DummyTarget, Integer> testInstance = BeanRelationFixer.of(DummyTarget::setProp2, DummyTarget::getProp2, ArrayList::new);
		DummyTarget target = new DummyTarget();
		testInstance.apply(target, 2);
		testInstance.apply(target, 5);
		assertThat(target.getProp2()).isEqualTo(Arrays.asList(2, 5));
	}
	
	@Test
	public void of_setter_getter_collection_bidirectional() {
		BeanRelationFixer<Country, City> testInstance = BeanRelationFixer.of(Country::setCities, Country::getCities, ArrayList::new, City::setCountry);
		Country target = new Country();
		City city = new City();
		testInstance.apply(target, city);
		assertThat(Iterables.first(target.getCities())).isEqualTo(city);
		assertThat(city.getCountry()).isEqualTo(target);
	}
	
	@Test
	public void giveCollectionFactory() {
		assertThat(BeanRelationFixer.giveCollectionFactory(List.class).get().getClass()).isEqualTo(ArrayList.class);
		assertThat(BeanRelationFixer.giveCollectionFactory(Set.class).get().getClass()).isEqualTo(HashSet.class);
		assertThat(BeanRelationFixer.giveCollectionFactory(LinkedHashSet.class).get().getClass()).isEqualTo(LinkedHashSet.class);
	}
	
	
	private static class DummyTarget {
		private String prop1;
		private List<Integer> prop2;
		
		public String getProp1() {
			return prop1;
		}
		
		public void setProp1(String prop1) {
			this.prop1 = prop1;
		}
		
		public List<Integer> getProp2() {
			return prop2;
		}
		
		public void setProp2(List<Integer> prop2) {
			this.prop2 = prop2;
		}
	}
	
	private static class Country {
		
		private President president;
		
		private List<City> cities;
		
		public President getPresident() {
			return president;
		}
		
		public void setPresident(President president) {
			this.president = president;
		}
		
		public List<City> getCities() {
			return cities;
		}
		
		public void setCities(List<City> cities) {
			this.cities = cities;
		}
	}
	
	private static class President {
		
		private Country country;
		
		public Country getCountry() {
			return country;
		}
		
		public void setCountry(Country country) {
			this.country = country;
		}
	}
	
	private static class City {
		private Country country;
		
		public Country getCountry() {
			return country;
		}
		
		public void setCountry(Country country) {
			this.country = country;
		}
	}
	
}