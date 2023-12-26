package org.codefilarete.stalactite.engine.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JoinTablePolymorphismSelectExecutorTest {
	
	@Test
	void transformCompositeIdentifierColumnValuesToTupleInValues() {
		Table table = new Table("Toto");
		Column firstNameColumn = table.addColumn("firstName", String.class);
		Column lastNameColumn = table.addColumn("lastName", String.class);
		Column ageColumn = table.addColumn("age", int.class);
		
		Map<Column<Table, Object>, List<Object>> columnValues = new LinkedHashMap<>();
		
		Toto bean1 = new Toto("John", "Doe", 40);
		Toto bean2 = new Toto("Jane", "Doe", 41);
		Toto bean3 = new Toto("Paul", "Smith", 35);
		
		List<Toto> beans = Arrays.asList(bean1, bean2, bean3);
		beans.forEach(bean -> {
			columnValues.computeIfAbsent(firstNameColumn, k -> new ArrayList<>()).add(bean.getFirstName());
			columnValues.computeIfAbsent(lastNameColumn, k -> new ArrayList<>()).add(bean.getLastName());
			columnValues.computeIfAbsent(ageColumn, k -> new ArrayList<>()).add(bean.getAge());
		});
		
		JoinTablePolymorphismSelectExecutor testInstance = new JoinTablePolymorphismSelectExecutor<>(null, null, null, null, null);
		TupleIn tupleIn = testInstance.transformCompositeIdentifierColumnValuesToTupleInValues(3, columnValues);
		assertThat(tupleIn.getColumns()).containsExactly(firstNameColumn, lastNameColumn, ageColumn);
		assertThat(tupleIn.getValue().get(0)).containsExactly("John", "Doe", 40);
		assertThat(tupleIn.getValue().get(1)).containsExactly("Jane", "Doe", 41);
		assertThat(tupleIn.getValue().get(2)).containsExactly("Paul", "Smith", 35);
		
	}
	
	private static class Toto {
		
		private final String firstName;
		private final String lastName;
		private final int age;
		
		private Toto(String firstName, String lastName, int age) {
			this.firstName = firstName;
			this.lastName = lastName;
			this.age = age;
		}
		
		public String getFirstName() {
			return firstName;
		}
		
		public String getLastName() {
			return lastName;
		}
		
		public int getAge() {
			return age;
		}
	}
	
}