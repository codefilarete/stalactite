package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.Collection;
import java.util.Set;

import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.codefilarete.stalactite.spring.repository.query.NativeQuery;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
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
	
	// projection tests
	@NativeQuery(value = "select"
			+ " R.name as Republic_name,"
			+ " P.name as president_name"
			+ " from Republic as R"
			+ " left outer join Person as P on R.presidentId = P.id"
			+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
			+ " left outer join Vehicle as V on P.vehicleId = V.id"
			+ " left outer join Language as L on Country_languages.languages_id = L.id"
			+ " where R.name = :name")
	NamesOnly getByName(@Param("name") String name);
	
	// we create a native query that collects all the columns to build the whole aggregate but the caller will decide of the result
	//- if the result is a NamesOnlyWithValue projection, all column are required because it has a @Value annotation
	//- if the result is a NamesOnly projection, then the collector will only need the 2 columns necessary to build the NamesOnly projection
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
	<T> Collection<T> getByName(@Param("name") String name, Class<T> type);
	
	@NativeQuery(value = "select"
			+ " R.name as Republic_name,"
			+ " P.name as president_name"
			+ " from Republic as R"
			+ " left outer join Person as P on R.presidentId = P.id"
			+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
			+ " left outer join Vehicle as V on P.vehicleId = V.id"
			+ " left outer join Language as L on Country_languages.languages_id = L.id"
			+ " where R.name like concat('%', :name, '%')"
			+ " order by president_name asc")
	Set<NamesOnly> getByNameLikeOrderByPresidentNameAsc(@Param("name") String name);

	@NativeQuery(value = "select"
			+ " R.name as Republic_name,"
			+ " P.name as president_name"
			+ " from Republic as R"
			+ " left outer join Person as P on R.presidentId = P.id"
			+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
			+ " left outer join Vehicle as V on P.vehicleId = V.id"
			+ " left outer join Language as L on Country_languages.languages_id = L.id"
			+ " where R.name like concat('%', :name, '%')"
			+ " order by president_name asc"
	)
	Slice<NamesOnly> getByNameLikeOrderByPresidentNameAsc(@Param("name") String name, Pageable pageable);
	
	@NativeQuery(value = "select"
			+ " R.name as Republic_name,"
			+ " P.name as president_name"
			+ " from Republic as R"
			+ " left outer join Person as P on R.presidentId = P.id"
			+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
			+ " left outer join Vehicle as V on P.vehicleId = V.id"
			+ " left outer join Language as L on Country_languages.languages_id = L.id"
			+ " where R.name like concat('%', :name, '%')"
			+ " order by president_name desc",
			countQuery = "select count(R.id)"
					+ " from Republic as R"
					+ " left outer join Person as P on R.presidentId = P.id"
					+ " left outer join Country_languages as Country_languages on R.id = Country_languages.republic_id"
					+ " left outer join Vehicle as V on P.vehicleId = V.id"
					+ " left outer join Language as L on Country_languages.languages_id = L.id"
					+ " where R.name like concat('%', :name, '%')"
	)
	Page<NamesOnly> getByNameLikeOrderByPresidentNameDesc(@Param("name") String name, Pageable pageable);
	
	boolean existsByName(String name);
	
	interface NamesOnly {
		
		String getName();
		
		SimplePerson getPresident();
		
		interface SimplePerson {
			
			String getName();
			
		}
	}
	
	// This class should create a query that retrieves the whole aggregate because it has a @Value annotation
	// Indeed, values required by the @Value annotations can't be known in advance, so the query must retrieve the whole aggregate
	interface NamesOnlyWithValue extends NamesOnly {
		
		@Value("#{target.president.name + '-' + target.president.id.delegate}")
		String getPresidentName();
	}
}
