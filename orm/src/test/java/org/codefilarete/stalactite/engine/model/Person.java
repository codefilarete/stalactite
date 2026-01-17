package org.codefilarete.stalactite.engine.model;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codefilarete.stalactite.engine.model.Car.Radio;
import org.codefilarete.stalactite.engine.model.compositekey.House;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

/**
 * @author Guillaume Mary
 */
public class Person implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	private int version;
	
	private Timestamp timestamp;
	
	private Country country;
	
	private Vehicle vehicle;
	
	private Bicycle mainBicycle;
	
	private Set<Bicycle> bicycles;
	
	private Set<String> nicknames;
	
	private List<String> middleNames;
	
	private Map<String, String> phoneNumbers;
	
	private Map<AddressBookType, String> addressBook;
	
	private Map<Timestamp, String> addresses;
	
	private Map<String, Timestamp> contracts;
	
	private Map<Timestamp, Radio> mapPropertyMadeOfComplexTypes;
	
	private Map<Timestamp, Timestamp> mapPropertyMadeOfComplexTypesWithColumnDuplicates;
	
	private Map<Country, String> mapPropertyMadeOfEntityAsKey;
	
	private Map<String, Country> mapPropertyMadeOfEntityAsValue;
	
	private Map<City, Country> mapPropertyMadeOfEntityAsKeyAndValue;
	
	private Map<House, String> mapPropertyMadeOfCompositeIdEntityAsKey;
	
	private Map<String, House> mapPropertyMadeOfCompositeIdEntityAsValue;
	
	
	public Person() {
	}
	
	public Person(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Person(Identifier<Long> id) {
		this.id = id;
	}
	
	@Override
	public Identifier<Long> getId() {
		return id;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public int getVersion() {
		return version;
	}
	
	public Timestamp getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}
	
	public Country getCountry() {
		return country;
	}
	
	public void setCountry(Country country) {
		this.country = country;
	}
	
	public Vehicle getVehicle() {
		return vehicle;
	}
	
	public void setVehicle(Vehicle vehicle) {
		this.vehicle = vehicle;
		if (vehicle != null) {
			vehicle.setOwner(this);
		}
	}
	
	public Bicycle getMainBicycle() {
		return mainBicycle;
	}

	public void setMainBicycle(Bicycle mainBicycle) {
		this.mainBicycle = mainBicycle;
	}
	
	public Set<Bicycle> getBicycles() {
		return bicycles;
	}
	
	public void setBicycles(Set<Bicycle> bicycles) {
		this.bicycles = bicycles;
	}
	
	public Set<String> getNicknames() {
		return nicknames;
	}
	
	public void setNicknames(Set<String> nicknames) {
		this.nicknames = nicknames;
	}
	
	public void initNicknames() {
		this.nicknames = new HashSet<>();
	}
	
	public void addNickname(String nickname) {
		this.nicknames.add(nickname);
	}
	
	public List<String> getMiddleNames() {
		return middleNames;
	}
	
	public void setMiddleNames(List<String> middleNames) {
		this.middleNames = middleNames;
	}
	
	public void addMiddleName(String middleName) {
		this.middleNames.add(middleName);
	}
	
	public Map<String, String> getPhoneNumbers() {
		return phoneNumbers;
	}
	
	public void setPhoneNumbers(Map<String, String> phoneNumbers) {
		this.phoneNumbers = phoneNumbers;
	}
	
	public Map<AddressBookType, String> getAddressBook() {
		return addressBook;
	}
	
	public void setAddressBook(Map<AddressBookType, String> addressBook) {
		this.addressBook = addressBook;
	}
	
	public Map<Timestamp, String> getAddresses() {
		return addresses;
	}
	
	public void setAddresses(Map<Timestamp, String> addresses) {
		this.addresses = addresses;
	}
	
	public Map<String, Timestamp> getContracts() {
		return contracts;
	}
	
	public void setContracts(Map<String, Timestamp> contracts) {
		this.contracts = contracts;
	}
	
	public Map<Timestamp, Radio> getMapPropertyMadeOfComplexTypes() {
		return mapPropertyMadeOfComplexTypes;
	}
	
	public void setMapPropertyMadeOfComplexTypes(Map<Timestamp, Radio> mapPropertyMadeOfComplexTypes) {
		this.mapPropertyMadeOfComplexTypes = mapPropertyMadeOfComplexTypes;
	}
	
	public Map<Timestamp, Timestamp> getMapPropertyMadeOfComplexTypesWithColumnDuplicates() {
		return mapPropertyMadeOfComplexTypesWithColumnDuplicates;
	}
	
	public void setMapPropertyMadeOfComplexTypesWithColumnDuplicates(Map<Timestamp, Timestamp> mapPropertyMadeOfComplexTypesWithColumnDuplicates) {
		this.mapPropertyMadeOfComplexTypesWithColumnDuplicates = mapPropertyMadeOfComplexTypesWithColumnDuplicates;
	}
	
	public Map<Country, String> getMapPropertyMadeOfEntityAsKey() {
		return mapPropertyMadeOfEntityAsKey;
	}
	
	public void setMapPropertyMadeOfEntityAsKey(Map<Country, String> mapPropertyMadeOfEntityAsKey) {
		this.mapPropertyMadeOfEntityAsKey = mapPropertyMadeOfEntityAsKey;
	}
	
	public Map<String, Country> getMapPropertyMadeOfEntityAsValue() {
		return mapPropertyMadeOfEntityAsValue;
	}
	
	public void setMapPropertyMadeOfEntityAsValue(Map<String, Country> mapPropertyMadeOfEntityAsValue) {
		this.mapPropertyMadeOfEntityAsValue = mapPropertyMadeOfEntityAsValue;
	}
	
	public Map<City, Country> getMapPropertyMadeOfEntityAsKeyAndValue() {
		return mapPropertyMadeOfEntityAsKeyAndValue;
	}
	
	public void setMapPropertyMadeOfEntityAsKeyAndValue(Map<City, Country> mapPropertyMadeOfEntityAsKeyAndValue) {
		this.mapPropertyMadeOfEntityAsKeyAndValue = mapPropertyMadeOfEntityAsKeyAndValue;
	}
	
	public Map<House, String> getMapPropertyMadeOfCompositeIdEntityAsKey() {
		return mapPropertyMadeOfCompositeIdEntityAsKey;
	}
	
	public void setMapPropertyMadeOfCompositeIdEntityAsKey(Map<House, String> mapPropertyMadeOfCompositeIdEntityAsKey) {
		this.mapPropertyMadeOfCompositeIdEntityAsKey = mapPropertyMadeOfCompositeIdEntityAsKey;
	}
	
	public Map<String, House> getMapPropertyMadeOfCompositeIdEntityAsValue() {
		return mapPropertyMadeOfCompositeIdEntityAsValue;
	}
	
	public void setMapPropertyMadeOfCompositeIdEntityAsValue(Map<String, House> mapPropertyMadeOfCompositeIdEntityAsValue) {
		this.mapPropertyMadeOfCompositeIdEntityAsValue = mapPropertyMadeOfCompositeIdEntityAsValue;
	}
	
	@Override
	public boolean equals(Object o) {
		return EqualsBuilder.reflectionEquals(this, o);
	}
	
	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}
	
	/**
	 * Implemented for easier debug
	 *
	 * @return a simple representation of this
	 */
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
	
	public enum AddressBookType {
		HOME,
		SECONDARY_HOUSE,
		BILLING_ADDRESS,
		OTHER
	}
}
