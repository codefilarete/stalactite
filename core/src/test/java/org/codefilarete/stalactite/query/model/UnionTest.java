package org.codefilarete.stalactite.query.model;

import java.util.Map;

import org.codefilarete.stalactite.query.model.QueryStatement.PseudoColumn;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnionTest {
	
	@Test
	void registerColumn_basicUseCase() {
		Union testInstance = new Union();
		PseudoColumn<Integer> createdColumn = testInstance.registerColumn("count(*)", int.class);
		assertThat(createdColumn.getExpression()).isEqualTo("count(*)");
		assertThat(createdColumn.getJavaType()).isEqualTo(int.class);
	}
	
	@Test
	void registerColumn_columnAlreadyExists_doesntRegisterColumn() {
		Union testInstance = new Union();
		PseudoColumn<Integer> createdColumn = testInstance.registerColumn("count(*)", int.class);
		PseudoColumn<Integer> createdColumn2 = testInstance.registerColumn("count(*)", int.class);
		assertThat(createdColumn2).isSameAs(createdColumn);
		assertThat(testInstance.getColumns()).hasSize(1);
	}
	
	@Test
	void registerColumn_columnAlreadyExistsButHasAlias_doesntRegisterColumn() {
		Union testInstance = new Union();
		PseudoColumn<Integer> createdColumn = testInstance.registerColumn("count(*)", int.class, "count");
		PseudoColumn<Integer> createdColumn2 = testInstance.registerColumn("count(*)", int.class);
		assertThat(createdColumn2).isSameAs(createdColumn);
		assertThat(testInstance.getColumns()).hasSize(1);
	}
	
	@Test
	void registerColumn_columnAlreadyExistsButRegisteredOnHasAlias_doesntRegisterColumn() {
		Union testInstance = new Union();
		PseudoColumn<Integer> createdColumn = testInstance.registerColumn("count(*)", int.class);
		PseudoColumn<Integer> createdColumn2 = testInstance.registerColumn("count(*)", int.class, "count");
		assertThat(createdColumn2).isNotSameAs(createdColumn);
		assertThat(testInstance.getColumns()).hasSize(2);
	}
	
	@Test
	void registerColumn_columnAlreadyExistsWithDifferentType_throwsException() {
		Union testInstance = new Union();
		testInstance.registerColumn("count(*)", int.class);
		assertThatThrownBy(() -> testInstance.registerColumn("count(*)", Integer.class))
				.hasMessage("Trying to add a column 'count(*)' that already exists with a different type : int vs j.l.Integer");
	}
	
	@Test
	void registerColumn_columnAlreadyExistsWithDifferentTypeButWithDifferentAlias_doesNotThrowsException() {
		Union testInstance = new Union();
		PseudoColumn<Integer> createdColumn = testInstance.registerColumn("count(*)", int.class, "a");
		PseudoColumn<Integer> createdColumn2 = testInstance.registerColumn("count(*)", int.class, "b");
		assertThat(createdColumn2).isNotSameAs(createdColumn);
		assertThat(testInstance.getColumns()).hasSize(2);
	}
	
	@Test
	void mapsColumnOnName() {
		Union testInstance = new Union();
		PseudoColumn<Integer> column1 = testInstance.registerColumn("count(*)", int.class);
		PseudoColumn<String> column2 = testInstance.registerColumn("name", String.class);
		PseudoColumn<String> column3 = testInstance.registerColumn("FIRST_NAME", String.class, "firstName");
		Map<String, ? extends Selectable<?>> columnPerName = testInstance.mapColumnsOnName();
		assertThat(columnPerName).isEqualTo(Maps.forHashMap(String.class, Selectable.class)
				.add("count(*)", column1)
				.add("name", column2)
				.add("FIRST_NAME", column3)
		);
	}
	
}