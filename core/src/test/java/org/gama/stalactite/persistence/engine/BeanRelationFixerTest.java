package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.List;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class BeanRelationFixerTest {
	
	@Test
	public void testOf_oneToOne() {
		BeanRelationFixer<DummyTarget, String> testInstance = BeanRelationFixer.of(DummyTarget::setProp1);
		DummyTarget target = new DummyTarget();
		String input = "toto";
		testInstance.apply(target, input);
		assertEquals(input, target.getProp1());
	}
	
	@Test
	public void testOf_oneToMany() {
		BeanRelationFixer<DummyTarget, Integer> testInstance = BeanRelationFixer.of(DummyTarget::setProp2, DummyTarget::getProp2, ArrayList::new);
		DummyTarget target = new DummyTarget();
		testInstance.apply(target, 2);
		testInstance.apply(target, 5);
		assertEquals(Arrays.asList(2, 5), target.getProp2());
	}
	
	@Test
	public void testOf_oneToOne_bidirectionnal() {
		BeanRelationFixer<Country, President> testInstance = BeanRelationFixer.of(Country::setPresident, President::setCountry);
		Country target = new Country();
		President president = new President();
		testInstance.apply(target, president);
		assertEquals(president, target.getPresident());
		assertEquals(target, president.getCountry());
	}
	
	@Test
	public void testOf_oneToMany_bidirectionnal() {
		BeanRelationFixer<Country, City> testInstance = BeanRelationFixer.of(Country::setCities, Country::getCities, ArrayList::new, City::setCountry);
		Country target = new Country();
		City city = new City();
		testInstance.apply(target, city);
		assertEquals(city, Iterables.first(target.getCities()));
		assertEquals(target, city.getCountry());
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