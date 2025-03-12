package org.codefilarete.stalactite.spring.repository.query;

import java.util.Collection;
import java.util.Set;

import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface NativeQueriesRepository extends StalactiteRepository<Republic, Identifier<Long>> {
	
	@Query(value = "select Republic.modificationDate as Republic_modificationDate, Republic.name as Republic_name,"
			+ " Republic.creationDate as Republic_creationDate, Republic.description as Republic_description, Republic.euMember as Republic_euMember,"
			+ " Republic.id as Republic_id, president.name as president_name, president.id as president_id, president_vehicle.color as president_vehicle_color,"
			+ " president_vehicle.id as president_vehicle_id, president_Person_nicknames.nicknames as president_Person_nicknames_nicknames,"
			+ " president_Person_nicknames.id as president_Person_nicknames_id, Country_states_State.name as Country_states_State_name,"
			+ " Country_states_State.id as Country_states_State_id, Country_languages_Language.code as Country_languages_Language_code,"
			+ " Country_languages_Language.id as Country_languages_Language_id"
			+ " from Republic"
			+ " left outer join Person as president on Republic.presidentId = president.id"
			+ " left outer join Country_states as Country_states on Republic.id = Country_states.republic_id"
			+ " left outer join Country_languages as Country_languages on Republic.id = Country_languages.republic_id"
			+ " left outer join Vehicle as president_vehicle on president.vehicleId = president_vehicle.id"
			+ " left outer join Person_nicknames as president_Person_nicknames on president.id = president_Person_nicknames.id"
			+ " left outer join State as Country_states_State on Country_states.states_id = Country_states_State.id"
			+ " left outer join \"Language\" as Country_languages_Language on Country_languages.languages_id = Country_languages_Language.id"
			+ " where Republic.id in (:ids)")
	Set<Republic> loadByIdIn(@Param("ids") Identifier<Long>... ids);
	
	@Query(value = "select Republic.modificationDate as Republic_modificationDate, Republic.name as Republic_name,"
			+ " Republic.creationDate as Republic_creationDate, Republic.description as Republic_description, Republic.euMember as Republic_euMember,"
			+ " Republic.id as Republic_id, president.name as president_name, president.id as president_id, president_vehicle.color as president_vehicle_color,"
			+ " president_vehicle.id as president_vehicle_id, president_Person_nicknames.nicknames as president_Person_nicknames_nicknames,"
			+ " president_Person_nicknames.id as president_Person_nicknames_id, Country_states_State.name as Country_states_State_name,"
			+ " Country_states_State.id as Country_states_State_id, Country_languages_Language.code as Country_languages_Language_code,"
			+ " Country_languages_Language.id as Country_languages_Language_id"
			+ " from Republic"
			+ " left outer join Person as president on Republic.presidentId = president.id"
			+ " left outer join Country_states as Country_states on Republic.id = Country_states.republic_id"
			+ " left outer join Country_languages as Country_languages on Republic.id = Country_languages.republic_id"
			+ " left outer join Vehicle as president_vehicle on president.vehicleId = president_vehicle.id"
			+ " left outer join Person_nicknames as president_Person_nicknames on president.id = president_Person_nicknames.id"
			+ " left outer join State as Country_states_State on Country_states.states_id = Country_states_State.id"
			+ " left outer join \"Language\" as Country_languages_Language on Country_languages.languages_id = Country_languages_Language.id"
			+ " where Republic.name in (:names)")
	Set<Republic> loadByNameIn(@Param("names") String... name);
	
	@Query(value = "select Republic.modificationDate as Republic_modificationDate, Republic.name as Republic_name,"
			+ " Republic.creationDate as Republic_creationDate, Republic.description as Republic_description, Republic.euMember as Republic_euMember,"
			+ " Republic.id as Republic_id, president.name as president_name, president.id as president_id, president_vehicle.color as president_vehicle_color,"
			+ " president_vehicle.id as president_vehicle_id, president_Person_nicknames.nicknames as president_Person_nicknames_nicknames,"
			+ " president_Person_nicknames.id as president_Person_nicknames_id, Country_states_State.name as Country_states_State_name,"
			+ " Country_states_State.id as Country_states_State_id, Country_languages_Language.code as Country_languages_Language_code,"
			+ " Country_languages_Language.id as Country_languages_Language_id"
			+ " from Republic"
			+ " left outer join Person as president on Republic.presidentId = president.id"
			+ " left outer join Country_states as Country_states on Republic.id = Country_states.republic_id"
			+ " left outer join Country_languages as Country_languages on Republic.id = Country_languages.republic_id"
			+ " left outer join Vehicle as president_vehicle on president.vehicleId = president_vehicle.id"
			+ " left outer join Person_nicknames as president_Person_nicknames on president.id = president_Person_nicknames.id"
			+ " left outer join State as Country_states_State on Country_states.states_id = Country_states_State.id"
			+ " left outer join \"Language\" as Country_languages_Language on Country_languages.languages_id = Country_languages_Language.id"
			+ " where Republic.id in (:ids)")
	Set<Republic> loadByIdIn(@Param("ids") Iterable<Identifier<Long>> ids);
	
	@Query(value = "select Republic.modificationDate as Republic_modificationDate, Republic.name as Republic_name,"
			+ " Republic.creationDate as Republic_creationDate, Republic.description as Republic_description, Republic.euMember as Republic_euMember,"
			+ " Republic.id as Republic_id, president.name as president_name, president.id as president_id, president_vehicle.color as president_vehicle_color,"
			+ " president_vehicle.id as president_vehicle_id, president_Person_nicknames.nicknames as president_Person_nicknames_nicknames,"
			+ " president_Person_nicknames.id as president_Person_nicknames_id, Country_states_State.name as Country_states_State_name,"
			+ " Country_states_State.id as Country_states_State_id, Country_languages_Language.code as Country_languages_Language_code,"
			+ " Country_languages_Language.id as Country_languages_Language_id"
			+ " from Republic"
			+ " left outer join Person as president on Republic.presidentId = president.id"
			+ " left outer join Country_states as Country_states on Republic.id = Country_states.republic_id"
			+ " left outer join Country_languages as Country_languages on Republic.id = Country_languages.republic_id"
			+ " left outer join Vehicle as president_vehicle on president.vehicleId = president_vehicle.id"
			+ " left outer join Person_nicknames as president_Person_nicknames on president.id = president_Person_nicknames.id"
			+ " left outer join State as Country_states_State on Country_states.states_id = Country_states_State.id"
			+ " left outer join \"Language\" as Country_languages_Language on Country_languages.languages_id = Country_languages_Language.id"
			+ " where Republic.name = :name")
	Republic loadByName(@Param("name") String name);
	
	@Query(value = "select Republic.modificationDate as Republic_modificationDate, Republic.name as Republic_name,"
			+ " Republic.creationDate as Republic_creationDate, Republic.description as Republic_description, Republic.euMember as Republic_euMember,"
			+ " Republic.id as Republic_id, president.name as president_name, president.id as president_id, president_vehicle.color as president_vehicle_color,"
			+ " president_vehicle.id as president_vehicle_id, president_Person_nicknames.nicknames as president_Person_nicknames_nicknames,"
			+ " president_Person_nicknames.id as president_Person_nicknames_id, Country_states_State.name as Country_states_State_name,"
			+ " Country_states_State.id as Country_states_State_id, Country_languages_Language.code as Country_languages_Language_code,"
			+ " Country_languages_Language.id as Country_languages_Language_id"
			+ " from Republic"
			+ " left outer join Person as president on Republic.presidentId = president.id"
			+ " left outer join Country_states as Country_states on Republic.id = Country_states.republic_id"
			+ " left outer join Country_languages as Country_languages on Republic.id = Country_languages.republic_id"
			+ " left outer join Vehicle as president_vehicle on president.vehicleId = president_vehicle.id"
			+ " left outer join Person_nicknames as president_Person_nicknames on president.id = president_Person_nicknames.id"
			+ " left outer join State as Country_states_State on Country_states.states_id = Country_states_State.id"
			+ " left outer join \"Language\" as Country_languages_Language on Country_languages.languages_id = Country_languages_Language.id"
			+ " where Republic.euMember = true")
	Republic loadByEuMemberIsTrue();
	
	@Query(value = "select Republic.modificationDate as Republic_modificationDate, Republic.name as Republic_name,"
			+ " Republic.creationDate as Republic_creationDate, Republic.description as Republic_description, Republic.euMember as Republic_euMember,"
			+ " Republic.id as Republic_id, president.name as president_name, president.id as president_id, president_vehicle.color as president_vehicle_color,"
			+ " president_vehicle.id as president_vehicle_id, president_Person_nicknames.nicknames as president_Person_nicknames_nicknames,"
			+ " president_Person_nicknames.id as president_Person_nicknames_id, Country_states_State.name as Country_states_State_name,"
			+ " Country_states_State.id as Country_states_State_id, Country_languages_Language.code as Country_languages_Language_code,"
			+ " Country_languages_Language.id as Country_languages_Language_id"
			+ " from Republic"
			+ " left outer join Person as president on Republic.presidentId = president.id"
			+ " left outer join Country_states as Country_states on Republic.id = Country_states.republic_id"
			+ " left outer join Country_languages as Country_languages on Republic.id = Country_languages.republic_id"
			+ " left outer join Vehicle as president_vehicle on president.vehicleId = president_vehicle.id"
			+ " left outer join Person_nicknames as president_Person_nicknames on president.id = president_Person_nicknames.id"
			+ " left outer join State as Country_states_State on Country_states.states_id = Country_states_State.id"
			+ " left outer join \"Language\" as Country_languages_Language on Country_languages.languages_id = Country_languages_Language.id"
			+ " where Republic.id = :id and Republic.name = :name")
	Republic loadByIdAndName(@Param("id") Identifier<Long> id, @Param("name") String name);
	
	@Query(value = "select Republic.modificationDate as Republic_modificationDate, Republic.name as Republic_name,"
			+ " Republic.creationDate as Republic_creationDate, Republic.description as Republic_description, Republic.euMember as Republic_euMember,"
			+ " Republic.id as Republic_id, president.name as president_name, president.id as president_id, president_vehicle.color as president_vehicle_color,"
			+ " president_vehicle.id as president_vehicle_id, president_Person_nicknames.nicknames as president_Person_nicknames_nicknames,"
			+ " president_Person_nicknames.id as president_Person_nicknames_id, Country_states_State.name as Country_states_State_name,"
			+ " Country_states_State.id as Country_states_State_id, Country_languages_Language.code as Country_languages_Language_code,"
			+ " Country_languages_Language.id as Country_languages_Language_id"
			+ " from Republic"
			+ " left outer join Person as president on Republic.presidentId = president.id"
			+ " left outer join Country_states as Country_states on Republic.id = Country_states.republic_id"
			+ " left outer join Country_languages as Country_languages on Republic.id = Country_languages.republic_id"
			+ " left outer join Vehicle as president_vehicle on president.vehicleId = president_vehicle.id"
			+ " left outer join Person_nicknames as president_Person_nicknames on president.id = president_Person_nicknames.id"
			+ " left outer join State as Country_states_State on Country_states.states_id = Country_states_State.id"
			+ " left outer join \"Language\" as Country_languages_Language on Country_languages.languages_id = Country_languages_Language.id"
			+ " where president_vehicle.color = :color")
	Republic loadByPresidentVehicleColor(@Param("color") Color color);
	
	long deleteByLanguagesCodeIs(String code);
	
	long countByLanguagesCodeIs(String code);
	
	DerivedQueriesRepository.NamesOnly getByName(String name);
	
	<T> Collection<T> getByName(String name, Class<T> type);
	
	boolean existsByName(String name);
	
	interface NamesOnly {
		
		String getName();
		
		@Value("#{target.president.name}")
		String getPresidentName();
		
		DerivedQueriesRepository.NamesOnly.SimplePerson getPresident();
		
		interface SimplePerson {
			
			String getName();
			
		}
	}
	
}
