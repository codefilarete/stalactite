package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.Set;

import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.codefilarete.stalactite.spring.repository.query.NativeQuery;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface TablePerClassNativeQueriesRepository extends StalactiteRepository<Country, Identifier<Long>> {
	
	// we changed the original query by
	// - a combination of a lighter UNION (less selected columns from sub-tables)
	// - a more complex select with COALESCE and CASEWHEN tp handle inheritance
	@NativeQuery(value = "select"
			+ " coalesce(Realm.name, Republic.name) as Country_name,"
			+ " coalesce(Realm.euMember, Republic.euMember) as Country_euMember,"
			+ " coalesce(Realm.id, Republic.id) as Country_id,"
			+ " coalesce(Realm.presidentId, Republic.presidentId) as Country_presidentId,"
			+ " casewhen(Realm.id is not null, 'Realm', 'Republic') as Country_DISCRIMINATOR,"
			+ " Republic.deputeCount as Republic_deputeCount,"
			+ " Realm_king.name as Realm_king_name,"
			+ " Realm_king.id as Realm_king_id,"
			+ " president_vehicle.color as president_vehicle_color,"
			+ " president_vehicle.id as president_vehicle_id,"
			+ " president.name as president_name,"
			+ " president.id as president_id,"
			+ " Country_languages_Language.code as Country_languages_Language_code,"
			+ " Country_languages_Language.id as Country_languages_Language_id"
			+ " from"
			+ " (select"
				+ " Realm.id as id,"
				+ " Realm.presidentId as presidentId"
				+ " from Realm"
				+ " UNION ALL"
				+ " select"
				+ " Republic.id as id,"
				+ " Republic.presidentId as presidentId"
				+ " from Republic"
			+ ") as Country"
			+ " left outer join Person as president on Country.presidentId = president.id"
			+ " left outer join Country_languages as Country_languages on Country.id = Country_languages.country_id"
			+ " left outer join Realm as Realm on Country.id = Realm.id"
			+ " left outer join Republic as Republic on Country.id = Republic.id"
			+ " left outer join Vehicle as president_vehicle on president.vehicleId = president_vehicle.id"
			+ " left outer join Language as Country_languages_Language on Country_languages.languages_id = Country_languages_Language.id"
			+ " left outer join King as Realm_king on Realm.kingId = Realm_king.id"
			+ " where"
			+ " Country.id in (:ids)")
	Set<Country> loadByIdIn(@Param("ids") Identifier<Long>... ids);
	
}