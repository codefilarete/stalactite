package org.codefilarete.stalactite.engine.model.compositekey;

import java.util.Objects;

public class Pet {
	
	private PetId id;
	
	private Person owner;
	
	public Pet(PetId id) {
		this.id = id;
	}
	
	public PetId getId() {
		return id;
	}
	
	public Person getOwner() {
		return owner;
	}
	
	@Override
	public String toString() {
		return "Pet{" +
				"id=" + id +
				'}';
	}
	
	public static class PetId {
		
		private String name;
		private String race;
		private int age;
		
		public PetId() {
		}
		
		public PetId(String name, String race, int age) {
			this.name = name;
			this.race = race;
			this.age = age;
		}
		
		public String getName() {
			return name;
		}
		
		public String getRace() {
			return race;
		}
		
		public int getAge() {
			return age;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			PetId petId = (PetId) o;
			return age == petId.age && Objects.equals(name, petId.name) && Objects.equals(race, petId.race);
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(name, race, age);
		}
		
		@Override
		public String toString() {
			return "PetId{" +
					"name='" + name + '\'' +
					", race='" + race + '\'' +
					", age=" + age +
					'}';
		}
	}
	
	public static class Dog extends Pet {
		
		private DogBreed dogBreed;
		
		public Dog(PetId id) {
			super(id);
		}
		
		public DogBreed getDogBreed() {
			return dogBreed;
		}
	}
	
	public enum DogBreed {
		Labrador,
		Australian_Shepherd,
		Golden_Retriever,
	}
	
	public static class Cat extends Pet {
		
		private CatBreed catBreed;
		
		public Cat(PetId id) {
			super(id);
		}
		
		public CatBreed getCatBreed() {
			return catBreed;
		}
		
		public Cat setCatBreed(CatBreed catBreed) {
			this.catBreed = catBreed;
			return this;
		}
	}
	
	public enum CatBreed {
		Persian,
		Siamese,
		Maine_Coon
	}
}
