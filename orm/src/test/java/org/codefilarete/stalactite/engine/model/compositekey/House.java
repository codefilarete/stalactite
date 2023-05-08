package org.codefilarete.stalactite.engine.model.compositekey;

public class House {
	
	private Long id;
	
	private HouseId houseId;
	
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
