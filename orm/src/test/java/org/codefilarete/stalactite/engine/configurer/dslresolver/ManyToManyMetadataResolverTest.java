package org.codefilarete.stalactite.engine.configurer.dslresolver;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.MappingJoin;
import org.codefilarete.stalactite.engine.model.security.RecoveryQuestion;
import org.codefilarete.stalactite.engine.model.survey.Answer;
import org.codefilarete.stalactite.engine.model.survey.Choice;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy.databaseAutoIncrement;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.tool.collection.Iterables.first;
import static org.mockito.Mockito.mock;

class ManyToManyMetadataResolverTest {
	
	@Test
	void resolve_defaultAssociationTableMapping() {
		FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceBuilder = entityBuilder(Choice.class, Identifier.LONG_TYPE)
				.mapKey(Choice::getId, ALREADY_ASSIGNED)
				.map(Choice::getLabel);
		
		FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, Identifier.LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, choiceBuilder);
		
		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Answer, Identifier<Long>, ?> answerEntity = testInstance.resolve(answerBuilder.getConfiguration());
		
		assertThat(answerEntity.getRelations()).hasSize(1);
		MappingJoin<?, ?, ?> relation = first(answerEntity.getRelations());
		assertThat(relation).isInstanceOf(org.codefilarete.stalactite.engine.configurer.model.ManyToManyRelation.class);
		
		IntermediaryRelationJoin<?, ?, ?, ?, ?> join = (IntermediaryRelationJoin<?, ?, ?, ?, ?>) relation.getJoin();
		assertThat(join.getJoinTable().getName()).isEqualTo("Answer_choices");
		
		Key<?, ?> leftAssociationKey = join.getLeftAssociationKey();
		Key<?, ?> rightAssociationKey = join.getRightAssociationKey();
		assertThat(((Column<?, ?>) first(leftAssociationKey.getColumns())).getName()).isEqualTo("answer_id");
		assertThat(((Column<?, ?>) first(rightAssociationKey.getColumns())).getName()).isEqualTo("choices_id");
	}
	
	@Test
	void resolve_customJoinTableAndJoinColumns() {
		FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceBuilder = entityBuilder(Choice.class, Identifier.LONG_TYPE)
				.mapKey(Choice::getId, ALREADY_ASSIGNED)
				.map(Choice::getLabel);
		
		FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, Identifier.LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, choiceBuilder)
						.joinTable("Toto")
								.sourceJoinColumn("left_side_id")
								.targetJoinColumn("right_side_id");
		
		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Answer, Identifier<Long>, ?> answerEntity = testInstance.resolve(answerBuilder.getConfiguration());
		
		MappingJoin<?, ?, ?> relation = first(answerEntity.getRelations());
		IntermediaryRelationJoin<?, ?, ?, ?, ?> join = (IntermediaryRelationJoin<?, ?, ?, ?, ?>) relation.getJoin();
		
		assertThat(join.getJoinTable().getName()).isEqualTo("Toto");
		assertThat(((Column<?, ?>) first(join.getLeftAssociationKey().getColumns())).getName()).isEqualTo("left_side_id");
		assertThat(((Column<?, ?>) first(join.getRightAssociationKey().getColumns())).getName()).isEqualTo("right_side_id");
	}
	
	@Test
	void resolve_indexedRelation_usesIndexedAssociationTable() {
		FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceBuilder = entityBuilder(Choice.class, Identifier.LONG_TYPE)
				.mapKey(Choice::getId, ALREADY_ASSIGNED)
				.map(Choice::getLabel);
		
		FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, Identifier.LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, choiceBuilder)
						.indexedBy("myIdx");
		
		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Answer, Identifier<Long>, ?> answerEntity = testInstance.resolve(answerBuilder.getConfiguration());
		
		MappingJoin<?, ?, ?> relation = first(answerEntity.getRelations());
		IntermediaryRelationJoin<?, ?, ?, ?, ?> join = (IntermediaryRelationJoin<?, ?, ?, ?, ?>) relation.getJoin();
		Table<?> associationTable = join.getJoinTable();
		Column<?, ?> indexColumn = associationTable.getColumn("myIdx");
		
		assertThat(indexColumn).isNotNull();
	}
	
	@Test
	void resolve_reverseCollection_relationFixerPopulatesBothSides() {
		FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceBuilder = entityBuilder(Choice.class, Identifier.LONG_TYPE)
				.mapKey(Choice::getId, ALREADY_ASSIGNED)
				.map(Choice::getLabel);
		
		FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, Identifier.LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, choiceBuilder)
						.reverseCollection(Choice::getAnswers);
		
		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Answer, Identifier<Long>, ?> answerEntity = testInstance.resolve(answerBuilder.getConfiguration());
		
		org.codefilarete.stalactite.engine.configurer.model.ManyToManyRelation<Answer, Choice, ?, ?, ?, ?, ?, ?> relation =
				(org.codefilarete.stalactite.engine.configurer.model.ManyToManyRelation<Answer, Choice, ?, ?, ?, ?, ?, ?>) first(answerEntity.getRelations());
		
		Answer answer = new Answer(1L);
		Choice choice = new Choice(42L);
		relation.getRelationFixer().apply(answer, choice);
		
		assertThat(answer.getChoices()).containsExactly(choice);
		assertThat(choice.getAnswers()).containsExactly(answer);
	}
	
	@Test
	void resolve_insetRelation_associationTableAndColumnsAreShifted() {
		FluentEmbeddableMappingBuilder<Answer> answerBuilder = embeddableBuilder(Answer.class)
				.map(Answer::getComment)
				.mapManyToMany(Answer::getChoices, entityBuilder(Choice.class, Identifier.LONG_TYPE)
						.mapKey(Choice::getId, ALREADY_ASSIGNED)
						.map(Choice::getLabel));
		
		FluentEntityMappingBuilder<RecoveryQuestion, Long> mappingBuilder = entityBuilder(RecoveryQuestion.class, Long.class)
				.mapKey(RecoveryQuestion::getId, databaseAutoIncrement())
				.embed(RecoveryQuestion::getAnswer, answerBuilder);
		
		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<RecoveryQuestion, Long, ?> entity = testInstance.resolve(mappingBuilder.getConfiguration());
		
		MappingJoin<?, ?, ?> relation = first(entity.getRelations());
		IntermediaryRelationJoin<?, ?, ?, ?, ?> join = (IntermediaryRelationJoin<?, ?, ?, ?, ?>) relation.getJoin();
		assertThat(join.getJoinTable().getName()).isEqualTo("RecoveryQuestion_answer_choices");
		
		assertThat(((Column<?, ?>) first(join.getLeftAssociationKey().getColumns())).getName()).isEqualTo("recoveryQuestion_id");
		assertThat(((Column<?, ?>) first(join.getRightAssociationKey().getColumns())).getName()).isEqualTo("answer_choices_id");
	}
	
	@Test
	void resolve_insetRelation_reverseCollection_relationFixerUsesEmbeddedSource() {
		FluentEmbeddableMappingBuilder<Answer> answerBuilder = embeddableBuilder(Answer.class)
				.map(Answer::getComment)
				.mapManyToMany(Answer::getChoices, entityBuilder(Choice.class, Identifier.LONG_TYPE)
						.mapKey(Choice::getId, ALREADY_ASSIGNED)
						.map(Choice::getLabel))
				.reverseCollection(Choice::getAnswers);
		
		FluentEntityMappingBuilder<RecoveryQuestion, Long> mappingBuilder = entityBuilder(RecoveryQuestion.class, Long.class)
				.mapKey(RecoveryQuestion::getId, databaseAutoIncrement())
				.embed(RecoveryQuestion::getAnswer, answerBuilder);
		
		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<RecoveryQuestion, Long, ?> entity = testInstance.resolve(mappingBuilder.getConfiguration());
		
		org.codefilarete.stalactite.engine.configurer.model.ManyToManyRelation<RecoveryQuestion, Choice, ?, ?, ?, ?, ?, ?> relation =
				(org.codefilarete.stalactite.engine.configurer.model.ManyToManyRelation<RecoveryQuestion, Choice, ?, ?, ?, ?, ?, ?>) first(entity.getRelations());
		
		RecoveryQuestion question = new RecoveryQuestion();
		Choice choice = new Choice(42L);
		relation.getRelationFixer().apply(question, choice);
		
		assertThat(question.getAnswer()).isNotNull();
		assertThat(question.getAnswer().getChoices()).containsExactly(choice);
		assertThat(choice.getAnswers()).containsExactly(question.getAnswer());
	}
}


