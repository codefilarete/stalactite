package org.codefilarete.stalactite.engine.model.compositekey;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Person {
	
	private PersonId id;
	
	private int age;
	
	private House house;
	
	private Set<Pet> pets = new HashSet<>();
	
	public Person() {
	}
	
	public Person(PersonId id) {
		this.id = id;
	}
	
	public PersonId getId() {
		return id;
	}
	
	public void setId(PersonId id) {
		this.id = id;
	}
	
	public int getAge() {
		return age;
	}
	
	public void setAge(int age) {
		this.age = age;
	}
	
	public House getHouse() {
		return house;
	}
	
	public void setHouse(House house) {
		this.house = house;
	}
	
	public Set<Pet> getPets() {
		return pets;
	}
	
	public void addPet(Pet pet) {
		this.pets.add(pet);
	}
	
	public void removePet(Pet.PetId petId) {
		this.pets.removeIf(pet -> pet.getId().equals(petId));
	}
	
	@Override
	public String toString() {
		return "Person{" +
				"id=" + id +
				", age=" + age +
				", house=" + house +
				", pets=" + pets +
				'}';
	}
	
	public static class PersonId {
		
		public PersonId() {
		}
		
		public PersonId(String firstName, String lastName, String address) {
			this.firstName = firstName;
			this.lastName = lastName;
			this.address = address;
		}
		
		private String firstName;
		
		private String lastName;
		
		private String address;
		
		public String getFirstName() {
			return firstName;
		}
		
		public String getLastName() {
			return lastName;
		}
		
		public String getFamilyName() {
			return lastName;
		}
		
		public String getAddress() {
			return address;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			PersonId personId = (PersonId) o;
			return Objects.equals(firstName, personId.firstName)
					&& Objects.equals(lastName, personId.lastName)
					&& Objects.equals(address, personId.address);
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(firstName, lastName, address);
		}
		
		@Override
		public String toString() {
			return "PersonId{" +
					"firstName='" + firstName + '\'' +
					", lastName='" + lastName + '\'' +
					", address='" + address + '\'' +
					'}';
		}
	}
}
