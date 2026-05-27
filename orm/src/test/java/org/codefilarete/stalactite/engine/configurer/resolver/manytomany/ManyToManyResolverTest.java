package org.codefilarete.stalactite.engine.configurer.resolver.manytomany;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.JdbcForeignKey;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver;
import org.codefilarete.stalactite.engine.model.survey.Answer;
import org.codefilarete.stalactite.engine.model.survey.Choice;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

/**
 * Integration tests for {@link ManyToManyResolver} and {@link AggregateManyToManyAppender} exercised
 * end-to-end through {@link AggregateResolver}.
 */
public class ManyToManyResolverTest {
	
	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	private FluentEntityMappingBuilder<Choice, Identifier<Long>> choiceConfiguration;
	
	@BeforeEach
	void setUp() {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
		
		choiceConfiguration = entityBuilder(Choice.class, LONG_TYPE)
				.mapKey(Choice::getId, ALREADY_ASSIGNED)
				.map(Choice::getLabel);
	}
	
	// -------------------------------------------------------------------------
	// Association table naming / DDL
	// -------------------------------------------------------------------------
	
	@Nested
	class ForeignKeyCreation {
		
		@Test
		void defaultNaming_foreignKeysAreCreated() throws SQLException {
			FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapManyToMany(Answer::getChoices, choiceConfiguration)
					.cascading(RelationMode.READ_ONLY);
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			testInstance.resolve(answerBuilder.getConfiguration());
			
			new DDLDeployer(persistenceContext).deployDDL();
			
			Connection connection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSetIterator<JdbcForeignKey> fkIterator = new ResultSetIterator<JdbcForeignKey>(
					connection.getMetaData().getImportedKeys(null, null, "ANSWER_CHOICES")) {
				@Override
				public JdbcForeignKey convert(ResultSet rs) throws SQLException {
					return new JdbcForeignKey(
							rs.getString("FK_NAME"),
							rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
							rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME"));
				}
			};
			Set<String> foundForeignKeys = new HashSet<>();
			fkIterator.forEachRemaining(fk -> foundForeignKeys.add(fk.getSignature()));
			
			assertThat(foundForeignKeys).containsExactlyInAnyOrder(
					new JdbcForeignKey("FK_ANSWER_CHOICES_ANSWER_ID_ANSWER_ID",
							"ANSWER_CHOICES", "ANSWER_ID", "ANSWER", "ID").getSignature(),
					new JdbcForeignKey("FK_ANSWER_CHOICES_CHOICES_ID_CHOICE_ID",
							"ANSWER_CHOICES", "CHOICES_ID", "CHOICE", "ID").getSignature());
		}
		
		@Test
		void customJoinTable_tableAndColumnsAreApplied() throws SQLException {
			FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapManyToMany(Answer::getChoices, choiceConfiguration)
					.joinTable("Toto")
					.sourceJoinColumn("left_side_id")
					.targetJoinColumn("right_side_id")
					.cascading(RelationMode.READ_ONLY);
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			testInstance.resolve(answerBuilder.getConfiguration());
			
			new DDLDeployer(persistenceContext).deployDDL();
			
			Connection connection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSetIterator<JdbcForeignKey> fkIterator = new ResultSetIterator<JdbcForeignKey>(
					connection.getMetaData().getImportedKeys(null, null, "TOTO")) {
				@Override
				public JdbcForeignKey convert(ResultSet rs) throws SQLException {
					return new JdbcForeignKey(
							rs.getString("FK_NAME"),
							rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
							rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME"));
				}
			};
			Set<String> foundForeignKeys = new HashSet<>();
			fkIterator.forEachRemaining(fk -> foundForeignKeys.add(fk.getSignature()));
			
			assertThat(foundForeignKeys).containsExactlyInAnyOrder(
					new JdbcForeignKey("FK_TOTO_LEFT_SIDE_ID_ANSWER_ID",
							"TOTO", "LEFT_SIDE_ID", "ANSWER", "ID").getSignature(),
					new JdbcForeignKey("FK_TOTO_RIGHT_SIDE_ID_CHOICE_ID",
							"TOTO", "RIGHT_SIDE_ID", "CHOICE", "ID").getSignature());
		}
	}
	
	// -------------------------------------------------------------------------
	// Basic CRUD
	// -------------------------------------------------------------------------
	
	@Test
	void crud_insertAndSelect() {
		FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, choiceConfiguration);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Answer, Identifier<Long>> answerPersister = testInstance.resolve(answerBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Answer answer = new Answer(new PersistableIdentifier<>(1L));
		Choice one = new Choice(new PersistableIdentifier<>(10L));
		one.setLabel("one");
		Choice two = new Choice(new PersistableIdentifier<>(20L));
		two.setLabel("two");
		answer.addChoices(one, two);
		
		answerPersister.insert(answer);
		
		// Verify that association rows were written
		Long assocationRowCount = persistenceContext.newQuery("select count(*) as cnt from answer_choices", Long.class)
				.mapKey("cnt", Long.class)
				.execute(Accumulators.getFirst());
		assertThat(assocationRowCount).isEqualTo(2L);
		
		// Verify select reloads the collection
		Answer loaded = answerPersister.select(answer.getId());
		assertThat(loaded.getChoices()).extracting(Choice::getLabel)
				.containsExactlyInAnyOrder("one", "two");
	}
	
	@Test
	void crud_delete_associationRowsAreRemoved() {
		FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, choiceConfiguration);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Answer, Identifier<Long>> answerPersister = testInstance.resolve(answerBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Answer answer = new Answer(new PersistableIdentifier<>(1L));
		Choice one = new Choice(new PersistableIdentifier<>(10L));
		one.setLabel("one");
		answer.addChoices(one);
		answerPersister.insert(answer);
		
		answerPersister.delete(answer);
		
		Long associationRowCount = persistenceContext.newQuery("select count(*) as cnt from answer_choices", Long.class)
				.mapKey("cnt", Long.class)
				.execute(Accumulators.getFirst());
		assertThat(associationRowCount).isEqualTo(0L);
	}
	
	// -------------------------------------------------------------------------
	// Bidirectionality
	// -------------------------------------------------------------------------
	
	@Test
	void select_reverseCollection_targetSideIsPopulated() {
		FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, choiceConfiguration)
				.reverseCollection(Choice::getAnswers);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Answer, Identifier<Long>> answerPersister = testInstance.resolve(answerBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Answer answer = new Answer(new PersistableIdentifier<>(1L));
		Choice grenoble = new Choice(new PersistableIdentifier<>(13L));
		grenoble.setLabel("Grenoble");
		Choice lyon = new Choice(new PersistableIdentifier<>(17L));
		lyon.setLabel("Lyon");
		answer.addChoices(grenoble, lyon);
		answerPersister.insert(answer);
		
		Answer loaded = answerPersister.select(answer.getId());
		assertThat(loaded.getChoices()).extracting(Choice::getLabel)
				.containsExactlyInAnyOrder("Grenoble", "Lyon");
		// Each loaded choice should carry a reference back to the loaded answer
		loaded.getChoices().forEach(choice ->
				assertThat(choice.getAnswers()).extracting(Answer::getId).containsOnly(answer.getId()));
	}
	
	// -------------------------------------------------------------------------
	// Indexed (ordered collection)
	// -------------------------------------------------------------------------
	
	@Test
	void crud_indexedCollection_orderIsPreserved() {
		FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, choiceConfiguration)
				.indexedBy("myIdx")
				.initializeWith(KeepOrderSet::new)
				.cascading(RelationMode.ALL);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Answer, Identifier<Long>> answerPersister = testInstance.resolve(answerBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		// Verify the index column was created
		Long idxColumnCount = persistenceContext.newQuery(
						"select count(*) as cnt from information_schema.columns "
								+ "where table_name='ANSWER_CHOICES' and column_name='MYIDX'",
						Long.class)
				.mapKey("cnt", Long.class)
				.execute(Accumulators.getFirst());
		assertThat(idxColumnCount).isEqualTo(1L);
		
		Answer answer1 = new Answer(new PersistableIdentifier<>(1L));
		Choice lyon = new Choice(new PersistableIdentifier<>(17L));
		lyon.setLabel("Lyon");
		Choice grenoble = new Choice(new PersistableIdentifier<>(13L));
		grenoble.setLabel("Grenoble");
		// insertion order: lyon first, then grenoble
		answer1.addChoices(lyon, grenoble);
		answerPersister.insert(answer1);
		
		Answer loaded = answerPersister.select(answer1.getId());
		assertThat(loaded.getChoices()).isInstanceOf(KeepOrderSet.class);
		assertThat(loaded.getChoices()).containsExactly(lyon, grenoble);
	}
	
	// -------------------------------------------------------------------------
	// Association-only cascade
	// -------------------------------------------------------------------------
	
	@Test
	void crud_associationOnly_targetNotDeleted() {
		FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, choiceConfiguration)
				.cascading(RelationMode.ASSOCIATION_ONLY);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Answer, Identifier<Long>> answerPersister = testInstance.resolve(answerBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		// Pre-insert the choiceConfiguration independently (target is not cascade-inserted under ASSOCIATION_ONLY)
		EntityPersister<Choice, Identifier<Long>> choicePersister =
				entityBuilder(Choice.class, LONG_TYPE)
						.mapKey(Choice::getId, ALREADY_ASSIGNED)
						.map(Choice::getLabel)
						.build(persistenceContext);
		
		Choice one = new Choice(new PersistableIdentifier<>(10L));
		one.setLabel("one");
		choicePersister.insert(one);
		
		Answer answer = new Answer(new PersistableIdentifier<>(1L));
		answer.addChoices(one);
		answerPersister.insert(answer);
		
		// Association rows should exist
		Long associationRowCount = persistenceContext.newQuery("select count(*) as cnt from answer_choices", Long.class)
				.mapKey("cnt", Long.class)
				.execute(Accumulators.getFirst());
		assertThat(associationRowCount).isEqualTo(1L);
		
		answerPersister.delete(answer);
		
		// Association rows removed, but target Choice should still exist
		associationRowCount = persistenceContext.newQuery("select count(*) as cnt from answer_choices", Long.class)
				.mapKey("cnt", Long.class)
				.execute(Accumulators.getFirst());
		assertThat(associationRowCount).isEqualTo(0L);
		
		Long choiceRowCount = persistenceContext.newQuery("select count(*) as cnt from choice", Long.class)
				.mapKey("cnt", Long.class)
				.execute(Accumulators.getFirst());
		assertThat(choiceRowCount).isEqualTo(1L);
	}
	
	// -------------------------------------------------------------------------
	// Multiple relations
	// -------------------------------------------------------------------------
	
	@Test
	void crud_twoManyToManyRelationsOnSameEntity() {
		FluentEntityMappingBuilder<Answer, Identifier<Long>> answerBuilder = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, choiceConfiguration)
				.mapManyToMany(Answer::getSecondaryChoices, choiceConfiguration)
				.cascading(RelationMode.ALL);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Answer, Identifier<Long>> answerPersister = testInstance.resolve(answerBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Answer answer = new Answer(new PersistableIdentifier<>(1L));
		Choice primary = new Choice(new PersistableIdentifier<>(10L));
		primary.setLabel("primary");
		Choice secondary = new Choice(new PersistableIdentifier<>(20L));
		secondary.setLabel("secondary");
		answer.addChoices(primary);
		answer.addSecondaryChoices(secondary);
		answerPersister.insert(answer);
		
		Answer loaded = answerPersister.select(answer.getId());
		assertThat(loaded.getChoices()).extracting(Choice::getLabel).containsExactly("primary");
		assertThat(loaded.getSecondaryChoices()).extracting(Choice::getLabel).containsExactly("secondary");
	}
}
