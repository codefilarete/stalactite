package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityMappingConfigurationProvider.EntityMappingConfigurationProviderHolder;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.model.book.Author;
import org.codefilarete.stalactite.engine.model.book.Book;
import org.codefilarete.stalactite.engine.runtime.OptimizedUpdatePersister;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.ALL;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.READ_ONLY;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportManyToManyTest {
	
	private static final Dialect DIALECT = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private static FluentEntityMappingBuilder<Choice, Identifier<Long>> CHOICE_MAPPING_CONFIGURATION;
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(dataSource);
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
	}

	@BeforeEach
	void beforeTest() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		
		// We need to rebuild our choicePersister before each test because some of them alter it on answer relationship.
		// So schema contains FK twice with same name, ending in duplicate FK name exception
		CHOICE_MAPPING_CONFIGURATION = entityBuilder(Choice.class, LONG_TYPE)
				.mapKey(Choice::getId, ALREADY_ASSIGNED)
				.map(Choice::getLabel);
		

	}
	
	@Nested
	class ForeignKeyCreation {
		
		@Test
		void foreignKeysAreCreated() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = entityBuilder(Answer.class, LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapManyToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION)
					.cascading(READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<JdbcForeignKey> fkChoiceIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getImportedKeys(null, null,
					"ANSWER_CHOICES")) {
				@Override
				public JdbcForeignKey convert(ResultSet rs) throws SQLException {
					return new JdbcForeignKey(
							rs.getString("FK_NAME"),
							rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
							rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
					);
				}
			};
			Set<String> foundForeignKey = Iterables.collect(() -> fkChoiceIterator, JdbcForeignKey::getSignature, HashSet::new);
			assertThat(foundForeignKey).containsExactlyInAnyOrder(
					new JdbcForeignKey("FK_ANSWER_CHOICES_ANSWER_ID_ANSWER_ID", "ANSWER_CHOICES", "ANSWER_ID", "ANSWER", "ID").getSignature(),
					new JdbcForeignKey("FK_ANSWER_CHOICES_CHOICES_ID_CHOICE_ID", "ANSWER_CHOICES", "CHOICES_ID", "CHOICE", "ID").getSignature()
			);
		}
		
		@Test
		void withTargetTable_targetTableIsUsed() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = entityBuilder(Answer.class, LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapManyToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION, new Table("MyChoice"))
					.cascading(READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<Table> tableIterator = new ResultSetIterator<Table>(currentConnection.getMetaData().getTables(null, currentConnection.getSchema(),
					null, null)) {
				@Override
				public Table convert(ResultSet rs) throws SQLException {
					return new Table(
							rs.getString("TABLE_NAME")
					);
				}
			};
			Set<String> foundTables = Iterables.collect(() -> tableIterator, Table::getName, HashSet::new);
			assertThat(foundTables).containsExactlyInAnyOrder("ANSWER", "MYCHOICE", "ANSWER_CHOICES");
			
			ResultSetIterator<JdbcForeignKey> fkChoiceIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getImportedKeys(null, null,
					"ANSWER_CHOICES")) {
				@Override
				public JdbcForeignKey convert(ResultSet rs) throws SQLException {
					return new JdbcForeignKey(
							rs.getString("FK_NAME"),
							rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
							rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
					);
				}
			};
			Set<String> foundForeignKey = Iterables.collect(() -> fkChoiceIterator, JdbcForeignKey::getSignature, HashSet::new);
			assertThat(foundForeignKey).containsExactlyInAnyOrder(
					new JdbcForeignKey("FK_ANSWER_CHOICES_ANSWER_ID_ANSWER_ID", "ANSWER_CHOICES", "ANSWER_ID", "ANSWER", "ID").getSignature(),
					new JdbcForeignKey("FK_ANSWER_CHOICES_CHOICES_ID_MYCHOICE_ID", "ANSWER_CHOICES", "CHOICES_ID", "MYCHOICE", "ID").getSignature()
			);
		}
		
		@Test
		void withTargetTableSetByTargetEntity_tableSetByTargetEntityIsUSed() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = entityBuilder(Answer.class, LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapManyToMany(Answer::getChoices,  entityBuilder(Choice.class, LONG_TYPE)
							.onTable(new Table<>("PossibleChoices"))
							.mapKey(Choice::getId, ALREADY_ASSIGNED)
							.map(Choice::getLabel))
					.cascading(READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<Table> tableIterator = new ResultSetIterator<Table>(currentConnection.getMetaData().getTables(null, currentConnection.getSchema(),
					null, null)) {
				@Override
				public Table convert(ResultSet rs) throws SQLException {
					return new Table(
							rs.getString("TABLE_NAME")
					);
				}
			};
			Set<String> foundTables = Iterables.collect(() -> tableIterator, Table::getName, HashSet::new);
			assertThat(foundTables).containsExactlyInAnyOrder("ANSWER", "POSSIBLECHOICES", "ANSWER_CHOICES");
			
			ResultSetIterator<JdbcForeignKey> fkChoiceIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getImportedKeys(null, null,
					"ANSWER_CHOICES")) {
				@Override
				public JdbcForeignKey convert(ResultSet rs) throws SQLException {
					return new JdbcForeignKey(
							rs.getString("FK_NAME"),
							rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
							rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
					);
				}
			};
			Set<String> foundForeignKey = Iterables.collect(() -> fkChoiceIterator, JdbcForeignKey::getSignature, HashSet::new);
			assertThat(foundForeignKey).containsExactlyInAnyOrder(
					new JdbcForeignKey("FK_ANSWER_CHOICES_ANSWER_ID_ANSWER_ID", "ANSWER_CHOICES", "ANSWER_ID", "ANSWER", "ID").getSignature(),
					new JdbcForeignKey("FK_ANSWER_CHOICES_CHOICES_ID_POSSIBLECHOICES_ID", "ANSWER_CHOICES", "CHOICES_ID", "POSSIBLECHOICES", "ID").getSignature()
			);
		}
		
		@Test
		void withTargetTableAndTableSetByTargetEntity_targetTableIsUsed() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = entityBuilder(Answer.class, LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapManyToMany(Answer::getChoices,  entityBuilder(Choice.class, LONG_TYPE)
							.onTable(new Table<>("PossibleChoices"))
							.mapKey(Choice::getId, ALREADY_ASSIGNED)
							.map(Choice::getLabel), new Table("MyChoice"))
					.cascading(READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<Table> tableIterator = new ResultSetIterator<Table>(currentConnection.getMetaData().getTables(null, currentConnection.getSchema(),
					null, null)) {
				@Override
				public Table convert(ResultSet rs) throws SQLException {
					return new Table(
							rs.getString("TABLE_NAME")
					);
				}
			};
			Set<String> foundTables = Iterables.collect(() -> tableIterator, Table::getName, HashSet::new);
			assertThat(foundTables).containsExactlyInAnyOrder("ANSWER", "MYCHOICE", "ANSWER_CHOICES");
			
			ResultSetIterator<JdbcForeignKey> fkChoiceIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getImportedKeys(null, null,
					"ANSWER_CHOICES")) {
				@Override
				public JdbcForeignKey convert(ResultSet rs) throws SQLException {
					return new JdbcForeignKey(
							rs.getString("FK_NAME"),
							rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
							rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
					);
				}
			};
			Set<String> foundForeignKey = Iterables.collect(() -> fkChoiceIterator, JdbcForeignKey::getSignature, HashSet::new);
			assertThat(foundForeignKey).containsExactlyInAnyOrder(
					new JdbcForeignKey("FK_ANSWER_CHOICES_ANSWER_ID_ANSWER_ID", "ANSWER_CHOICES", "ANSWER_ID", "ANSWER", "ID").getSignature(),
					new JdbcForeignKey("FK_ANSWER_CHOICES_CHOICES_ID_MYCHOICE_ID", "ANSWER_CHOICES", "CHOICES_ID", "MYCHOICE", "ID").getSignature()
			);
		}
	}
	
	@Test
	void crud_relationContainsOneToMany() {
		EntityPersister<Answer, Identifier<Long>> persister = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION)
				.build(persistenceContext);

		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		Answer answer = new Answer(new PersistableIdentifier<>(1L));
		Choice grenoble = new Choice(new PersistableIdentifier<>(13L));
		grenoble.setLabel("Grenoble");
		Choice lyon = new Choice(new PersistableIdentifier<>(17L));
		lyon.setLabel("Lyon");
		answer.addChoices(grenoble, lyon);
		persister.insert(answer);
		
		ExecutableQuery<Long> longExecutableQuery3 = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class);
		Set<Long> choiceAnswerIds = longExecutableQuery3.execute(Accumulators.toSet());

		assertThat(choiceAnswerIds).containsExactlyInAnyOrder(answer.getId().getDelegate());

		// testing select
		Answer loadedAnswer = persister.select(answer.getId());
		assertThat(loadedAnswer.getChoices()).extracting(Choice::getLabel).containsExactlyInAnyOrder("Grenoble", "Lyon");
		
		// testing update : removal of a city, reversed column must be set to null
		Answer modifiedAnswer = new Answer(answer.getId());
		modifiedAnswer.addChoices(Iterables.first(answer.getChoices()));

		persister.update(modifiedAnswer, answer, false);
		
		ExecutableQuery<Long> longExecutableQuery2 = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class);
		choiceAnswerIds = longExecutableQuery2.execute(Accumulators.toSet());
		assertThat(choiceAnswerIds).containsExactlyInAnyOrder(answer.getId().getDelegate());
		
		// referenced Choices must not be deleted (we didn't ask for delete orphan)
		ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select id from choice", Long.class)
				.mapKey("id", Long.class);
		Set<Long> choiceIds = longExecutableQuery1.execute(Accumulators.toSet());
		assertThat(choiceIds).containsExactlyInAnyOrder(grenoble.getId().getDelegate(), lyon.getId().getDelegate());

		// testing delete
		persister.delete(modifiedAnswer);
		ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class);
		choiceAnswerIds = longExecutableQuery.execute(Accumulators.toSet());
		assertThat(choiceAnswerIds).isEmpty();
	}
	
	@Test
	void crud_relationContainsManyToMany() {
		EntityPersister<Answer, Identifier<Long>> persister = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ALL)
				.build(persistenceContext);

		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		Answer answer1 = new Answer(new PersistableIdentifier<>(1L));
		Answer answer2 = new Answer(new PersistableIdentifier<>(2L));
		Choice grenoble = new Choice(new PersistableIdentifier<>(13L));
		grenoble.setLabel("Grenoble");
		Choice lyon = new Choice(new PersistableIdentifier<>(17L));
		lyon.setLabel("Lyon");
		answer1.addChoices(grenoble, lyon);
		answer2.addChoices(grenoble, lyon);
		persister.insert(Arrays.asList(answer1, answer2));
		
		ExecutableQuery<Long> longExecutableQuery3 = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class);
		Set<Long> choiceAnswerIds = longExecutableQuery3.execute(Accumulators.toSet());

		assertThat(choiceAnswerIds).containsExactlyInAnyOrder(answer1.getId().getDelegate(), answer2.getId().getDelegate());

		// testing select
		Answer loadedAnswer = persister.select(answer1.getId());
		assertThat(loadedAnswer.getChoices()).extracting(Choice::getLabel).containsExactlyInAnyOrder("Grenoble", "Lyon");
		
		// testing update : removal of a city, reversed column must be set to null
		Answer modifiedAnswer = new Answer(answer1.getId());
		modifiedAnswer.addChoices(Iterables.first(answer1.getChoices()));

		persister.update(modifiedAnswer, answer1, false);
		
		ExecutableQuery<Long> longExecutableQuery2 = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class);
		choiceAnswerIds = longExecutableQuery2.execute(Accumulators.toSet());
		assertThat(choiceAnswerIds).containsExactlyInAnyOrder(answer1.getId().getDelegate(), answer2.getId().getDelegate());
		
		// referenced Choices must not be deleted (we didn't ask for orphan deletion)
		ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select id from choice", Long.class)
				.mapKey("id", Long.class);
		Set<Long> choiceIds = longExecutableQuery1.execute(Accumulators.toSet());
		assertThat(choiceIds).containsExactlyInAnyOrder(grenoble.getId().getDelegate(), lyon.getId().getDelegate());

		// testing delete
		persister.delete(modifiedAnswer);
		ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select answer_id from answer_choices", Long.class)
				.mapKey("answer_id", Long.class);
		choiceAnswerIds = longExecutableQuery.execute(Accumulators.toSet());
		assertThat(choiceAnswerIds).containsExactlyInAnyOrder(answer2.getId().getDelegate());
	}
	
	private static class Trio<L, M, R> {
		
		private static Trio<Integer, Integer, Integer> forInteger(Integer left, Integer middle, Integer right) {
			return new Trio<>(left, middle, right);
		}
		
		private final L left;
		private final M middle;
		private final R right;
		
		public Trio(L left, M middle, R right) {
			this.left = left;
			this.middle = middle;
			this.right = right;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Trio<?, ?, ?> trio = (Trio<?, ?, ?>) o;
			return Objects.equals(left, trio.left) && Objects.equals(middle, trio.middle) && Objects.equals(right, trio.right);
		}
		
		@Override
		public int hashCode() {
			return Objects.hashCode(left, middle, right);
		}
		
		@Override
		public String toString() {
			return "{left=" + left + ", middle=" + middle + ", right=" + right + '}';
		}
	}
	
	@Test
	void crud_relationContainsManyToMany_indexed() {
		EntityPersister<Answer, Identifier<Long>> persister = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION)
					.indexedBy("myIdx")
					.initializeWith(KeepOrderSet::new)
					.cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Answer answer1 = new Answer(new PersistableIdentifier<>(1L));
		Answer answer2 = new Answer(new PersistableIdentifier<>(2L));
		Choice grenoble = new Choice(new PersistableIdentifier<>(13L));
		grenoble.setLabel("Grenoble");
		Choice lyon = new Choice(new PersistableIdentifier<>(17L));
		lyon.setLabel("Lyon");
		answer1.addChoices(lyon, grenoble);
		answer2.addChoices(grenoble, lyon);
		persister.insert(Arrays.asList(answer1, answer2));
		
		ExecutableQuery<Trio<Integer, Integer, Integer>> trioExecutableQuery = persistenceContext.newQuery("select answer_id, choices_id, myIdx from answer_choices", (Class<Trio<Integer, Integer, Integer>>) (Class) Trio.class)
				.<Integer, Integer, Integer>mapKey(Trio::forInteger, "answer_id", "choices_id", "myIdx");
		Set<Trio<Integer, Integer, Integer>> choiceAnswerIds = trioExecutableQuery.execute(Accumulators.toSet());
		
		assertThat(choiceAnswerIds).containsExactlyInAnyOrder(new Trio<>(1, 17, 1), new Trio<>(1, 13, 2), new Trio<>(2, 13, 1), new Trio<>(2, 17, 2));

		Answer loadedAnswer1 = persister.select(answer1.getId());
		assertThat(loadedAnswer1.getChoices()).isInstanceOf(KeepOrderSet.class);
		assertThat(loadedAnswer1.getChoices()).containsExactly(lyon, grenoble);
		Answer loadedAnswer2 = persister.select(answer2.getId());
		assertThat(loadedAnswer2.getChoices()).isInstanceOf(KeepOrderSet.class);
		assertThat(loadedAnswer2.getChoices()).containsExactly(grenoble, lyon);
	}

	@Test
	void crud_relationContainsManyToMany_indexed_siblingProperties() {
		EntityPersister<Answer, Identifier<Long>> persister = entityBuilder(Answer.class, LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION)
					.indexedBy("myIdx")
					.initializeWith(KeepOrderSet::new)
				.cascading(ALL)
				// we map a second property to create a sibling which eventually confuses engine (the goal is that it doesn't confuse it)
				.mapManyToMany(Answer::getSecondaryChoices, CHOICE_MAPPING_CONFIGURATION)
					.indexedBy("myIdx2")
					.initializeWith(KeepOrderSet::new)
				.cascading(ALL)
				.build(persistenceContext);

		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		Answer answer1 = new Answer(new PersistableIdentifier<>(1L));
		Answer answer2 = new Answer(new PersistableIdentifier<>(2L));
		Choice grenoble = new Choice(new PersistableIdentifier<>(13L));
		grenoble.setLabel("Grenoble");
		Choice lyon = new Choice(new PersistableIdentifier<>(17L));
		lyon.setLabel("Lyon");
		answer1.addChoices(lyon, grenoble);
		answer1.addSecondaryChoices(lyon, grenoble);
		answer2.addChoices(grenoble, lyon);
		answer2.addSecondaryChoices(grenoble, lyon);
		persister.insert(Arrays.asList(answer1, answer2));

		ExecutableQuery<Trio<Integer, Integer, Integer>> answerChoicesQuery = persistenceContext.newQuery("select answer_id, choices_id, myIdx from answer_choices", (Class<Trio<Integer, Integer, Integer>>) (Class) Trio.class)
				.<Integer, Integer, Integer>mapKey(Trio::forInteger, "answer_id", "choices_id", "myIdx");
		Set<Trio<Integer, Integer, Integer>> answerChoicesIds = answerChoicesQuery.execute(Accumulators.toSet());
		
		assertThat(answerChoicesIds).containsExactlyInAnyOrder(new Trio<>(1, 17, 1), new Trio<>(1, 13, 2), new Trio<>(2, 13, 1), new Trio<>(2, 17, 2));

		ExecutableQuery<Trio<Integer, Integer, Integer>> answerSecondaryChoicesQuery = persistenceContext.newQuery("select answer_id, secondaryChoices_id, myIdx2 from answer_secondaryChoices", (Class<Trio<Integer, Integer, Integer>>) (Class) Trio.class)
				.<Integer, Integer, Integer>mapKey(Trio::forInteger, "answer_id", "secondaryChoices_id", "myIdx2");
		Set<Trio<Integer, Integer, Integer>> secondaryChoicesIds = answerSecondaryChoicesQuery.execute(Accumulators.toSet());

		assertThat(secondaryChoicesIds).containsExactlyInAnyOrder(new Trio<>(1, 17, 1), new Trio<>(1, 13, 2), new Trio<>(2, 13, 1), new Trio<>(2, 17, 2));

		Map<Identifier<Long>, Answer> answerMap = persister.select(answer1.getId(), answer2.getId())
				.stream()
				.collect(Collectors.toMap(Answer::getId, Function.identity()));
		Answer loadedAnswer1 = answerMap.get(answer1.getId());
		assertThat(loadedAnswer1.getChoices()).isInstanceOf(KeepOrderSet.class);
		assertThat(loadedAnswer1.getChoices()).containsExactly(lyon, grenoble);
		Answer loadedAnswer2 = answerMap.get(answer2.getId());
		assertThat(loadedAnswer2.getChoices()).isInstanceOf(KeepOrderSet.class);
		assertThat(loadedAnswer2.getChoices()).containsExactly(grenoble, lyon);
	}
	
	@Test
	void select_collectionFactory() throws SQLException {
		// mapping building thanks to fluent API
		EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION)
					// applying a Set that will sort cities by their name
					.initializeWith(() -> new TreeSet<>(Comparator.comparing(Choice::getLabel)))
				.build(persistenceContext);

		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		// preparing data
		persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Answer(id) values (42)").execute();
		persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Choice(id, label) values (1, 'Paris')").execute();
		persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Choice(id, label) values (2, 'Lyon')").execute();
		persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Answer_Choices(answer_id, choices_id) values (42, 1), (42, 2)").execute();

		Answer loadedAnswer = answerPersister.select(new PersistedIdentifier<>(42L));
		assertThat(loadedAnswer.getChoices().getClass()).isEqualTo(TreeSet.class);
		assertThat(loadedAnswer.getChoices()).extracting(Choice::getLabel).containsExactly("Lyon", "Paris");
	}
	
	@Nested
	class CascadeReadOnly {
		
		@Test
		void insert_onlySourceEntitiesArePersisted() {
			// mapping building thanks to fluent API
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					// no cascade, nor reverse side
					.mapManyToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Answer answer = new Answer(new PersistableIdentifier<>(1L));
			Choice grenoble = new Choice(new PersistableIdentifier<>(13L));
			grenoble.setLabel("Grenoble");
			Choice lyon = new Choice(new PersistableIdentifier<>(17L));
			lyon.setLabel("Lyon");
			answer.addChoices(grenoble, lyon);
			answerPersister.insert(answer);
			
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from answer", Long.class)
					.mapKey("id", Long.class);
			Set<Long> answerIds = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(answerIds).containsExactlyInAnyOrder(answer.getId().getDelegate());
			
			Long choiceAnswerCount = persistenceContext.newQuery("select count(*) as relationCount from answer_choices", Long.class)
					.mapKey("relationCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(choiceAnswerCount).isEqualTo(0);
			
			Long choiceCount = persistenceContext.newQuery("select count(*) as choiceCount from choice", Long.class)
					.mapKey("choiceCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(choiceCount).isEqualTo(0);
		}
	}
	
	@Nested
	class CascadeAll {

		@Test
		void update_associationTableIsMaintained() {
			// mapping building thanks to fluent API
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapOneToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ALL)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			LongProvider answerIdProvider = new LongProvider();
			Answer dummyAnswer = new Answer(answerIdProvider.giveNewIdentifier());
			LongProvider cityIdProvider = new LongProvider();
			Choice paris = new Choice(cityIdProvider.giveNewIdentifier());
			paris.setLabel("Paris");
			dummyAnswer.addChoices(paris);
			Choice lyon = new Choice(cityIdProvider.giveNewIdentifier());
			lyon.setLabel("Lyon");
			dummyAnswer.addChoices(lyon);
			answerPersister.insert(dummyAnswer);

			// Changing answer cities to see what happens when we save it to the database
			// removing Paris
			// adding Grenoble
			// renaming Lyon
			Answer persistedAnswer = answerPersister.select(dummyAnswer.getId());
			persistedAnswer.getChoices().remove(paris);
			Choice grenoble = new Choice(cityIdProvider.giveNewIdentifier());
			grenoble.setLabel("Grenoble");
			persistedAnswer.addChoices(grenoble);
			Iterables.first(persistedAnswer.getChoices()).setLabel("changed");

			answerPersister.update(persistedAnswer, dummyAnswer, true);

			Answer persistedAnswer2 = answerPersister.select(dummyAnswer.getId());
			assertThat(persistedAnswer2.getChoices()).extracting(Choice::getLabel).containsExactlyInAnyOrder("changed", "Grenoble");
		}
		
		@Test
		void delete_associationRecordsMustBeDeleted() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapOneToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ALL)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer(id) values (42, 666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Choice(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer_choices(answer_Id, choices_Id)" +
					" values (42, 100), (42, 200), (666, 300)");

			Answer answer1 = new Answer(new PersistedIdentifier<>(42L));
			Choice city1 = new Choice(new PersistedIdentifier<>(100L));
			Choice city2 = new Choice(new PersistedIdentifier<>(200L));
			answer1.addChoices(city1, city2);
			Answer answer2 = new Answer(new PersistedIdentifier<>(666L));
			Choice city3 = new Choice(new PersistedIdentifier<>(300L));
			answer2.addChoices(city3);

			// testing deletion
			answerPersister.delete(answer1);

			// Checking that we deleted what we wanted
			Long answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 42", Long.class)
					.mapKey("answerCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(answerCount).isEqualTo(0);
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			Long relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 42", Long.class)
					.mapKey("relationCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(relationCount).isEqualTo(0);
			// target entities are not deleted with cascade All
			ExecutableQuery<Long> longExecutableQuery3 = persistenceContext.newQuery("select id from Choice where id in (100, 200)", Long.class)
					.mapKey("id", Long.class);
			Set<Long> choiceIds = longExecutableQuery3.execute(Accumulators.toSet());
			assertThat(choiceIds).containsExactlyInAnyOrder(100L, 200L);

			// but we didn't delete everything !
			ExecutableQuery<Long> longExecutableQuery2 = persistenceContext.newQuery("select id from Answer where id = 666", Long.class)
					.mapKey("id", Long.class);
			Set<Long> answerIds = longExecutableQuery2.execute(Accumulators.toSet());
			assertThat(answerIds).containsExactlyInAnyOrder(666L);
			ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class);
			choiceIds = longExecutableQuery1.execute(Accumulators.toSet());
			assertThat(choiceIds).containsExactlyInAnyOrder(300L);

			// testing deletion of the last one
			answerPersister.delete(answer2);
			answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 666", Long.class)
					.mapKey("answerCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(answerCount).isEqualTo(0);
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 666", Long.class)
					.mapKey("relationCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(relationCount).isEqualTo(0);
			// target entities are not deleted with cascade All
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class);
			choiceIds = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(choiceIds).containsExactlyInAnyOrder(300L);
		}
	}
	
	@Nested
	class CascadeAllOrphanRemoval {

		@Test
		void update_removedElementsAreDeleted() {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapOneToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			LongProvider answerIdProvider = new LongProvider();
			Answer dummyAnswer = new Answer(answerIdProvider.giveNewIdentifier());
			LongProvider cityIdProvider = new LongProvider();
			Choice paris = new Choice(cityIdProvider.giveNewIdentifier());
			paris.setLabel("Paris");
			dummyAnswer.addChoices(paris);
			Choice lyon = new Choice(cityIdProvider.giveNewIdentifier());
			lyon.setLabel("Lyon");
			dummyAnswer.addChoices(lyon);
			answerPersister.insert(dummyAnswer);

			// Changing answer choices to see what happens when we save it to the database
			Answer persistedAnswer = answerPersister.select(dummyAnswer.getId());
			persistedAnswer.getChoices().remove(paris);
			Choice grenoble = new Choice(cityIdProvider.giveNewIdentifier());
			grenoble.setLabel("Grenoble");
			persistedAnswer.addChoices(grenoble);
			Iterables.first(persistedAnswer.getChoices()).setLabel("changed");

			answerPersister.update(persistedAnswer, dummyAnswer, true);

			Answer persistedAnswer2 = answerPersister.select(dummyAnswer.getId());
			// Checking deletion has been taken into account : the reloaded instance contains choices that are the same as the memory one
			// (comparison are done on equals/hashCode => id)
			assertThat(persistedAnswer2.getChoices()).isEqualTo(Arrays.asHashSet(lyon, grenoble));
			// Checking update is done too
			assertThat(persistedAnswer2.getChoices()).extracting(Choice::getLabel).containsExactlyInAnyOrder("changed", "Grenoble");
		}
		
		@Test
		void delete_associationRecordsMustBeDeleted() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapOneToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer(id) values (42, 666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Choice(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer_choices(answer_Id, choices_Id)" +
					" values (42, 100), (42, 200), (666, 300)");

			Answer answer1 = new Answer(new PersistedIdentifier<>(42L));
			Choice city1 = new Choice(new PersistedIdentifier<>(100L));
			Choice city2 = new Choice(new PersistedIdentifier<>(200L));
			answer1.addChoices(city1, city2);
			Answer answer2 = new Answer(new PersistedIdentifier<>(666L));
			Choice city3 = new Choice(new PersistedIdentifier<>(300L));
			answer2.addChoices(city3);

			// testing deletion
			answerPersister.delete(answer1);

			// Checking that we deleted what we wanted
			Long answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 42", Long.class)
					.mapKey("answerCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(answerCount).isEqualTo(0);
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			Long relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 42", Long.class)
					.mapKey("relationCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(relationCount).isEqualTo(0);
			// target entities are not deleted with cascade All
			ExecutableQuery<Long> longExecutableQuery3 = persistenceContext.newQuery("select id from Choice where id in (100, 200)", Long.class)
					.mapKey("id", Long.class);
			Set<Long> choiceIds = longExecutableQuery3.execute(Accumulators.toSet());
			assertThat(choiceIds).isEmpty();
			
			// but we didn't delete everything !
			ExecutableQuery<Long> longExecutableQuery2 = persistenceContext.newQuery("select id from Answer where id = 666", Long.class)
					.mapKey("id", Long.class);
			Set<Long> answerIds = longExecutableQuery2.execute(Accumulators.toSet());
			assertThat(answerIds).containsExactlyInAnyOrder(666L);
			ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class);
			choiceIds = longExecutableQuery1.execute(Accumulators.toSet());
			assertThat(choiceIds).containsExactlyInAnyOrder(300L);
			
			// testing deletion of the last one
			answerPersister.delete(answer2);
			answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 666", Long.class)
					.mapKey("answerCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(answerCount).isEqualTo(0);
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 666", Long.class)
					.mapKey("relationCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(relationCount).isEqualTo(0);
			// target entities are not deleted with cascade All
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class);
			choiceIds = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(choiceIds).isEmpty();
		}
	}

	@Nested
	class CascadeAssociationOnly {

		@Test
		void insert_associationRecordsMustBeInserted_butNotTargetEntities() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapOneToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// We need to insert target choices because they won't be inserted by ASSOCIATION_ONLY cascade
			// If they were inserted by cascade an constraint violation error will be thrown 
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Choice(id) values (100), (200), (300)");
			
			Answer answer1 = new Answer(new PersistableIdentifier<>(42L));
			Choice choice1 = new Choice(new PersistableIdentifier<>(100L));
			Choice choice2 = new Choice(new PersistableIdentifier<>(200L));
			answer1.addChoices(choice1, choice2);
			Answer answer2 = new Answer(new PersistableIdentifier<>(666L));
			Choice choice3 = new Choice(new PersistableIdentifier<>(300L));
			answer2.addChoices(choice3);
			
			// testing insertion
			answerPersister.insert(Arrays.asList(answer1, answer2));
			
			// Checking that we inserted what we wanted
			ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select id from Answer where id in (42, 666)", Long.class)
					.mapKey("id", Long.class);
			Set<Long> answerIds = longExecutableQuery1.execute(Accumulators.toSet());
			assertThat(answerIds).containsExactlyInAnyOrder(answer1.getId().getDelegate(), answer2.getId().getDelegate());
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select choices_Id from Answer_choices where answer_id in (42, 666)", Long.class)
					.mapKey("choices_id", Long.class);
			Set<Long> choicesInRelationIds = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(choicesInRelationIds).containsExactlyInAnyOrder(choice1.getId().getDelegate(), choice2.getId().getDelegate(), choice3.getId().getDelegate());
		}
		
		@Test
		void update_associationRecordsMustBeUpdated_butNotTargetEntities() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.map(Answer::getComment)
					.mapOneToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			// we need to insert target choices because they won't be inserted by ASSOCIATION_ONLY cascade
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Choice(id) values (100), (200)");

			Answer answer1 = new Answer(new PersistableIdentifier<>(42L));
			answer1.setComment("Hello world !");
			Choice choice1 = new Choice(new PersistableIdentifier<>(100L));
			Choice choice2 = new Choice(new PersistableIdentifier<>(200L));
			answer1.addChoices(choice1, choice2);

			answerPersister.insert(answer1);

			// changing values before update
			choice1.setLabel("Grenoble");
			answerPersister.update(answer1, answerPersister.select(answer1.getId()), true);

			ResultSet resultSet;
			// Checking that answer name was updated
			String answerComment = persistenceContext.newQuery("select comment from Answer where id = 42", String.class)
					.mapKey("comment", String.class)
					.execute(Accumulators.getFirst());
			assertThat(answerComment).isEqualTo(answer1.getComment());
			// .. but not its city name
			ExecutableQuery<String> stringExecutableQuery1 = persistenceContext.newQuery("select label from Choice where id = 100", String.class)
					.mapKey("label", String.class);
			Set<String> choiceLabels = stringExecutableQuery1.execute(Accumulators.toSet());
			assertThat(choiceLabels).containsExactlyInAnyOrder((String) null);
			
			// removing city doesn't have any effect either
			assertThat(answer1.getChoices().size()).isEqualTo(2);	// safeguard for unwanted regression on city removal, because it would totally corrupt this test
			answer1.getChoices().remove(choice1);
			assertThat(answer1.getChoices().size()).isEqualTo(1);	// safeguard for unwanted regression on city removal, because it would totally corrupt this test
			answerPersister.update(answer1, answerPersister.select(answer1.getId()), true);
			ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select label from Choice where id = 100", String.class)
					.mapKey("label", String.class);
			choiceLabels = stringExecutableQuery.execute(Accumulators.toSet());
			assertThat(choiceLabels).containsExactlyInAnyOrder((String) null);
		}

		@Test
		void delete_associationRecordsMustBeDeleted_butNotTargetEntities() throws SQLException {
			EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
					.mapKey(Answer::getId, ALREADY_ASSIGNED)
					.mapManyToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer(id) values (42, 666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Choice(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer_choices(answer_Id, choices_Id)" +
					" values (42, 100), (42, 200), (666, 300)");

			Answer answer1 = new Answer(new PersistedIdentifier<>(42L));
			Choice choice1 = new Choice(new PersistedIdentifier<>(100L));
			Choice choice2 = new Choice(new PersistedIdentifier<>(200L));
			answer1.addChoices(choice1, choice2);
			Answer answer2 = new Answer(new PersistedIdentifier<>(666L));
			Choice choice3 = new Choice(new PersistedIdentifier<>(300L));
			answer2.addChoices(choice3);

			// testing deletion
			answerPersister.delete(answer1);

			// Checking that we deleted what we wanted
			Long answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 42", Long.class)
					.mapKey("answerCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(answerCount).isEqualTo(0);
			// with ASSOCIATION_ONLY, association table records must be deleted
			Long relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 42", Long.class)
					.mapKey("relationCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(relationCount).isEqualTo(0);
			// ... but target entities are not deleted with ASSOCIATION_ONLY
			ExecutableQuery<Long> longExecutableQuery3 = persistenceContext.newQuery("select id from Choice where id in (100, 200)", Long.class)
					.mapKey("id", Long.class);
			Set<Long> choiceIds = longExecutableQuery3.execute(Accumulators.toSet());
			assertThat(choiceIds).containsExactlyInAnyOrder(100L, 200L);
			
			// but we didn't delete everything !
			ExecutableQuery<Long> longExecutableQuery2 = persistenceContext.newQuery("select id from Answer where id = 666", Long.class)
					.mapKey("id", Long.class);
			Set<Long> answerIds = longExecutableQuery2.execute(Accumulators.toSet());
			assertThat(answerIds).containsExactlyInAnyOrder(666L);
			ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class);
			choiceIds = longExecutableQuery1.execute(Accumulators.toSet());
			assertThat(choiceIds).containsExactlyInAnyOrder(300L);
			
			// testing deletion of the last one
			answerPersister.delete(answer2);
			answerCount = persistenceContext.newQuery("select count(*) as answerCount from Answer where id = 666", Long.class)
					.mapKey("answerCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(answerCount).isEqualTo(0);
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			relationCount = persistenceContext.newQuery("select count(*) as relationCount from Answer_choices where answer_Id = 666", Long.class)
					.mapKey("relationCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(relationCount).isEqualTo(0);
			// target entities are not deleted with cascade All
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from Choice where id = 300", Long.class)
					.mapKey("id", Long.class);
			choiceIds = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(choiceIds).containsExactlyInAnyOrder(300L);
			
		}
	}

	@Test
	void select_noRecordInAssociationTable_mustReturnEmptyCollection() throws SQLException {
		EntityPersister<Answer, Identifier<Long>> answerPersister = MappingEase.entityBuilder(Answer.class, Identifier.LONG_TYPE)
				.mapKey(Answer::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Answer::getChoices, CHOICE_MAPPING_CONFIGURATION).cascading(READ_ONLY)
				.build(persistenceContext);

		// this is a configuration safeguard, thus we ensure that configuration matches test below
		assertThat(((OptimizedUpdatePersister<Answer, Identifier<Long>>) answerPersister).getDelegate()
				.getEntityJoinTree().getJoin("Answer_Choices0")).isNull();

		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		// we only register one answer without any city
		persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Answer(id) values (42)");

		// Then : Answer must exist and have an empty city collection
		Answer loadedAnswer = answerPersister.select(new PersistedIdentifier<>(42L));
		assertThat(loadedAnswer.getChoices()).isEqualTo(null);

	}
	
	@Test
	void bidirectionality_reverselySetBy() {
		EntityMappingConfigurationProviderHolder<Author, Long> authorMappingConfiguration = new EntityMappingConfigurationProviderHolder<>();
		EntityMappingConfigurationProviderHolder<Book, Long> bookMappingConfiguration = new EntityMappingConfigurationProviderHolder<>();
		authorMappingConfiguration.setProvider(MappingEase.entityBuilder(Author.class, Long.class)
				.mapKey(Author::getId, IdentifierPolicy.databaseAutoIncrement())
				.map(Author::getName));
		bookMappingConfiguration.setProvider(MappingEase.entityBuilder(Book.class, Long.class)
				.mapKey(Book::getId, IdentifierPolicy.databaseAutoIncrement())
				.mapManyToMany(Book::getAuthors, authorMappingConfiguration)
					.reverselySetBy(Author::addBook)
					.reverselyInitializeWith(LinkedHashSet::new)
				.map(Book::getIsbn).columnName("isbn")
				.map(Book::getPrice)
				.map(Book::getTitle));
		
		Book book1 = new Book("a first book", 24.10, "AAA-BBB-CCC");
		Book book2 = new Book("a second book", 33.50, "XXX-YYY-ZZZ");
		Author author1 = new Author("John Doe");
		Author author2 = new Author("Jane Doe");
		
		book1.setAuthors(Arrays.asSet(author1));
		book2.setAuthors(Arrays.asSet(author1, author2));
		
		author1.setWrittenBooks(Arrays.asSet(book1, book2));
		author2.setWrittenBooks(Arrays.asSet(book2));
		
		PersistenceContext persistenceContext = new PersistenceContext(dataSource, DIALECT);
		EntityPersister<Book, Long> bookPersister = bookMappingConfiguration.getProvider().build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		bookPersister.insert(book1);
		bookPersister.insert(book2);
		
		Set<Book> select = bookPersister.select(Arrays.asSet(book1.getId(), book2.getId()));
		Book loadedBook1 = Iterables.find(select, Book::getTitle, "a first book"::equals).getLeft();
		Book loadedBook2 = Iterables.find(select, Book::getTitle, "a second book"::equals).getLeft();
		assertThat(loadedBook1.getAuthors()).allSatisfy(author -> {
			assertThat(author.getWrittenBooks()).isInstanceOf(LinkedHashSet.class);
			assertThat(author.getWrittenBooks()).containsExactly(loadedBook1, loadedBook2);
		});
		
		
		List<String> creationScripts = ddlDeployer.getCreationScripts();
		assertThat(creationScripts).containsExactlyInAnyOrder(
				"create table Book_authors(book_id bigint, authors_id bigint, unique (book_id, authors_id))",
				"create table Author(name varchar(255), id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, unique (id))",
				"create table Book(isbn varchar(255), price double, title varchar(255), id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, unique (id))",
				"alter table Book_authors add constraint FK_Book_authors_authors_id_Author_id foreign key(authors_id) references Author(id)",
				"alter table Book_authors add constraint FK_Book_authors_book_id_Book_id foreign key(book_id) references Book(id)"
		);
	}
	
	@Test
	void bidirectionality_reverseInitializedWith() {
		EntityMappingConfigurationProviderHolder<Author, Long> authorMappingConfiguration = new EntityMappingConfigurationProviderHolder<>();
		EntityMappingConfigurationProviderHolder<Book, Long> bookMappingConfiguration = new EntityMappingConfigurationProviderHolder<>();
		authorMappingConfiguration.setProvider(MappingEase.entityBuilder(Author.class, Long.class)
				.mapKey(Author::getId, IdentifierPolicy.databaseAutoIncrement())
				.map(Author::getName));
		bookMappingConfiguration.setProvider(MappingEase.entityBuilder(Book.class, Long.class)
				.mapKey(Book::getId, IdentifierPolicy.databaseAutoIncrement())
				.mapManyToMany(Book::getAuthors, authorMappingConfiguration)
						.reverselyInitializeWith(LinkedHashSet::new)
				.map(Book::getIsbn).columnName("isbn")
				.map(Book::getPrice)
				.map(Book::getTitle));
		
		Book book1 = new Book("a first book", 24.10, "AAA-BBB-CCC");
		Book book2 = new Book("a second book", 33.50, "XXX-YYY-ZZZ");
		Author author1 = new Author("John Doe");
		Author author2 = new Author("Jane Doe");
		
		book1.setAuthors(Arrays.asSet(author1));
		book2.setAuthors(Arrays.asSet(author1, author2));
		
		author1.setWrittenBooks(Arrays.asSet(book1, book2));
		author2.setWrittenBooks(Arrays.asSet(book2));
		
		PersistenceContext persistenceContext = new PersistenceContext(dataSource, DIALECT);
		EntityPersister<Book, Long> bookPersister = bookMappingConfiguration.getProvider().build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		bookPersister.insert(book1);
		bookPersister.insert(book2);
		
		Set<Book> select = bookPersister.select(Arrays.asSet(book1.getId(), book2.getId()));
		Book loadedBook1 = Iterables.find(select, Book::getTitle, "a first book"::equals).getLeft();
		Book loadedBook2 = Iterables.find(select, Book::getTitle, "a second book"::equals).getLeft();
		assertThat(loadedBook1.getAuthors()).allSatisfy(author -> assertThat(author.getWrittenBooks()).isInstanceOf(LinkedHashSet.class));
	}
	
	@Test
	void bidirectionality_reverseCollection() {
		EntityMappingConfigurationProviderHolder<Author, Long> authorMappingConfiguration = new EntityMappingConfigurationProviderHolder<>();
		EntityMappingConfigurationProviderHolder<Book, Long> bookMappingConfiguration = new EntityMappingConfigurationProviderHolder<>();
		authorMappingConfiguration.setProvider(MappingEase.entityBuilder(Author.class, Long.class)
				.mapKey(Author::getId, IdentifierPolicy.databaseAutoIncrement())
				.map(Author::getName));
		bookMappingConfiguration.setProvider(MappingEase.entityBuilder(Book.class, Long.class)
				.mapKey(Book::getId, IdentifierPolicy.databaseAutoIncrement())
				.mapManyToMany(Book::getAuthors, authorMappingConfiguration)
						.reverseCollection(Author::getBooks)
				.map(Book::getIsbn).columnName("isbn")
				.map(Book::getPrice)
				.map(Book::getTitle));
		
		Book book1 = new Book("a first book", 24.10, "AAA-BBB-CCC");
		Book book2 = new Book("a second book", 33.50, "XXX-YYY-ZZZ");
		Author author1 = new Author("John Doe");
		Author author2 = new Author("Jane Doe");
		
		book1.setAuthors(Arrays.asSet(author1));
		book2.setAuthors(Arrays.asSet(author1, author2));
		
		author1.setWrittenBooks(Arrays.asSet(book1, book2));
		author2.setWrittenBooks(Arrays.asSet(book2));
		
		PersistenceContext persistenceContext = new PersistenceContext(dataSource, DIALECT);
		EntityPersister<Book, Long> bookPersister = bookMappingConfiguration.getProvider().build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		bookPersister.insert(book1);
		bookPersister.insert(book2);
		
		Set<Book> select = bookPersister.select(Arrays.asSet(book1.getId(), book2.getId()));
		Book loadedBook1 = Iterables.find(select, Book::getTitle, "a first book"::equals).getLeft();
		Book loadedBook2 = Iterables.find(select, Book::getTitle, "a second book"::equals).getLeft();
		assertThat(loadedBook1.getAuthors()).allSatisfy(author -> {
			assertThat(author.getWrittenBooks()).isExactlyInstanceOf(HashSet.class);
			assertThat(author.getWrittenBooks()).containsExactlyInAnyOrder(loadedBook1, loadedBook2);
		});
	}
	
	public static class Answer implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private String comment;
		
		private Set<Choice> choices;
		
		private Set<Choice> secondaryChoices;
		
		private Answer() {
		}
		
		private Answer(Long id) {
			this(new PersistableIdentifier<>(id));
		}
		
		private Answer(Identifier<Long> id) {
			this.id = id;
		}
		
		public Answer(Choice choice) {
			this.choices.add(choice);
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public String getComment() {
			return comment;
		}
		
		public void setComment(String comment) {
			this.comment = comment;
		}
		
		public Set<Choice> getChoices() {
			return choices;
		}
		
		public void addChoices(Collection<Choice> choices) {
			this.choices.addAll(choices);
		}
		
		public void addChoices(Choice... choices) {
			if (this.choices == null) {
				this.choices = new LinkedHashSet<>();
			}
			this.choices.addAll(Arrays.asList(choices));
		}
		
		public void setChoices(Set<Choice> choices) {
			this.choices = choices;
		}

		public Set<Choice> getSecondaryChoices() {
			return secondaryChoices;
		}

		public void addSecondaryChoices(Collection<Choice> choices) {
			this.secondaryChoices.addAll(choices);
		}

		public void addSecondaryChoices(Choice... choices) {
			if (this.secondaryChoices == null) {
				this.secondaryChoices = new LinkedHashSet<>();
			}
			this.secondaryChoices.addAll(Arrays.asList(choices));
		}

		public void setSecondaryChoices(Set<Choice> secondaryChoices) {
			this.secondaryChoices = secondaryChoices;
		}
		
		@Override
		public String toString() {
			return "Answer{" +
					"id=" + id +
					'}';
		}
	}
	
	public static class Choice implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private String label;
		
		public Choice() {
		}
		
		public Choice(long id) {
			this.id = new PersistableIdentifier<>(id);
		}
		
		public Choice(Identifier<Long> id) {
			this.id = id;
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public String getLabel() {
			return label;
		}
		
		public void setLabel(String label) {
			this.label = label;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof Choice)) return false;
			Choice choice = (Choice) o;
			return Objects.equals(id, choice.id);
		}
		
		@Override
		public int hashCode() {
			return Objects.hashCode(id);
		}
		
		@Override
		public String toString() {
			return "Choice{id=" + id.getDelegate() + ", label='" + label + '\'' + '}';
		}
	}
}
