package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.Collection;
import java.util.Set;

import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.codefilarete.stalactite.spring.repository.query.DerivedQueriesRepository;
import org.codefilarete.stalactite.spring.repository.query.NativeQuery;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface NativeQueriesRepository extends StalactiteRepository<Republic, Identifier<Long>> {
	
	@NativeQuery(value = "select"
			+ " R.name as Republic_name,"
			+ " R.euMember as Republic_euMember,"
			+ " R.id as Republic_id,"
			+ " P.name as president_name,"
			+ " P.id as president_id,"
			+ " V.color as president_vehicle_color,"
			+ " V.id as president_vehicle_id,"
			+ " L.code as Country_languages_Language_code,"
			+ " L.id as Country_languages_Language_id"
			+ " from Republic as R"
			+ " left outer join Person as P on R.presidentId = P.id"
			+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
			+ " left outer join Vehicle as V on P.vehicleId = V.id"
			+ " left outer join Language as L on Country_languages.languages_id = L.id"
			+ " where R.id in (:ids)")
	Set<Republic> loadByIdIn(@Param("ids") Identifier<Long>... ids);
	
	@NativeQuery(value = "select"
			+ " R.name as Republic_name,"
			+ " R.euMember as Republic_euMember,"
			+ " R.id as Republic_id,"
			+ " P.name as president_name,"
			+ " P.id as president_id,"
			+ " V.color as president_vehicle_color,"
			+ " V.id as president_vehicle_id,"
			+ " L.code as Country_languages_Language_code,"
			+ " L.id as Country_languages_Language_id"
			+ " from Republic as R"
			+ " left outer join Person as P on R.presidentId = P.id"
			+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
			+ " left outer join Vehicle as V on P.vehicleId = V.id"
			+ " left outer join Language as L on Country_languages.languages_id = L.id"
			+ " where R.name in (:names)")
	Set<Republic> loadByNameIn(@Param("names") String... name);
	
	@NativeQuery(value = "select"
			+ " R.name as Republic_name,"
			+ " R.euMember as Republic_euMember,"
			+ " R.id as Republic_id,"
			+ " P.name as president_name,"
			+ " P.id as president_id,"
			+ " V.color as president_vehicle_color,"
			+ " V.id as president_vehicle_id,"
			+ " L.code as Country_languages_Language_code,"
			+ " L.id as Country_languages_Language_id"
			+ " from Republic as R"
			+ " left outer join Person as P on R.presidentId = P.id"
			+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
			+ " left outer join Vehicle as V on P.vehicleId = V.id"
			+ " left outer join Language as L on Country_languages.languages_id = L.id"
			+ " where R.id in (:ids)")
	Set<Republic> loadByIdIn(@Param("ids") Iterable<Identifier<Long>> ids);
	
	@NativeQuery(value = "select"
			+ " R.name as Republic_name,"
			+ " R.euMember as Republic_euMember,"
			+ " R.id as Republic_id,"
			+ " P.name as president_name,"
			+ " P.id as president_id,"
			+ " V.color as president_vehicle_color,"
			+ " V.id as president_vehicle_id,"
			+ " L.code as Country_languages_Language_code,"
			+ " L.id as Country_languages_Language_id"
			+ " from Republic as R"
			+ " left outer join Person as P on R.presidentId = P.id"
			+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
			+ " left outer join Vehicle as V on P.vehicleId = V.id"
			+ " left outer join Language as L on Country_languages.languages_id = L.id"
			+ " where R.name = :name")
	Republic loadByName(@Param("name") String name);
	
	@NativeQuery(value = "select"
			+ " R.name as Republic_name,"
			+ " R.euMember as Republic_euMember,"
			+ " R.id as Republic_id,"
			+ " P.name as president_name,"
			+ " P.id as president_id,"
			+ " V.color as president_vehicle_color,"
			+ " V.id as president_vehicle_id,"
			+ " L.code as Country_languages_Language_code,"
			+ " L.id as Country_languages_Language_id"
			+ " from Republic as R"
			+ " left outer join Person as P on R.presidentId = P.id"
			+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
			+ " left outer join Vehicle as V on P.vehicleId = V.id"
			+ " left outer join Language as L on Country_languages.languages_id = L.id"
			+ " where R.euMember = true")
	Republic loadByEuMemberIsTrue();
	
	@NativeQuery(value = "select"
			+ " R.name as Republic_name,"
			+ " R.euMember as Republic_euMember,"
			+ " R.id as Republic_id,"
			+ " P.name as president_name,"
			+ " P.id as president_id,"
			+ " V.color as president_vehicle_color,"
			+ " V.id as president_vehicle_id,"
			+ " L.code as Country_languages_Language_code,"
			+ " L.id as Country_languages_Language_id"
			+ " from Republic as R"
			+ " left outer join Person as P on R.presidentId = P.id"
			+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
			+ " left outer join Vehicle as V on P.vehicleId = V.id"
			+ " left outer join Language as L on Country_languages.languages_id = L.id"
			+ " where R.id = :id and R.name = :name")
	Republic loadByIdAndName(@Param("id") Identifier<Long> id, @Param("name") String name);
	
	@NativeQuery(value = "select"
			+ " R.name as Republic_name,"
			+ " R.euMember as Republic_euMember,"
			+ " R.id as Republic_id,"
			+ " P.name as president_name,"
			+ " P.id as president_id,"
			+ " V.color as president_vehicle_color,"
			+ " V.id as president_vehicle_id,"
			+ " L.code as Country_languages_Language_code,"
			+ " L.id as Country_languages_Language_id"
			+ " from Republic as R"
			+ " left outer join Person as P on R.presidentId = P.id"
			+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
			+ " left outer join Vehicle as V on P.vehicleId = V.id"
			+ " left outer join Language as L on Country_languages.languages_id = L.id"
			+ " where V.color = :color")
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
