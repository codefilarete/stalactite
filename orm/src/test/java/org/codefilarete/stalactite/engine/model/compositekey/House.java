package org.codefilarete.stalactite.engine.model.compositekey;

import java.util.Objects;

public class House {
	
	private Long id;
	
	private HouseId houseId;
	
	private Long version;
	
	private String surname;
	
	private Person owner;
	
	public House() {
	}
	
	public House(HouseId houseId) {
		this.houseId = houseId;
	}
	
	public Long getId() {
		return id;
	}
	
	public HouseId getHouseId() {
		return houseId;
	}
	
	public Long getVersion() {
		return version;
	}

	public String getSurname() {
		return surname;
	}

	public Person getOwner() {
		return owner;
	}
	
	public void setOwner(Person owner) {
		this.owner = owner;
	}
	
	@Override
	public String toString() {
		return "House{" +
				"id=" + id +
				", houseId=" + houseId +
				'}';
	}
	
	public static class HouseId {
		
		private int number;
		private String street;
		private String zipCode;
		private String city;
		
		public HouseId() {
			
		}
		
		public HouseId(int number, String street, String zipCode, String city) {
			this.number = number;
			this.street = street;
			this.zipCode = zipCode;
			this.city = city;
		}
		
		public int getNumber() {
			return number;
		}
		
		public String getStreet() {
			return street;
		}
		
		public String getZipCode() {
			return zipCode;
		}
		
		public String getCity() {
			return city;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			
			HouseId houseId = (HouseId) o;
			
			if (number != houseId.number) return false;
			if (!Objects.equals(street, houseId.street)) return false;
			if (!Objects.equals(zipCode, houseId.zipCode)) return false;
			return Objects.equals(city, houseId.city);
		}
		
		@Override
		public int hashCode() {
			int result = number;
			result = 31 * result + (street != null ? street.hashCode() : 0);
			result = 31 * result + (zipCode != null ? zipCode.hashCode() : 0);
			result = 31 * result + (city != null ? city.hashCode() : 0);
			return result;
		}
		
		@Override
		public String toString() {
			return "HouseId{" +
					"number=" + number +
					", street='" + street + '\'' +
					", zipCode='" + zipCode + '\'' +
					", city='" + city + '\'' +
					'}';
		}
	}
}
