package org.codefilarete.stalactite.sql;

import java.util.Collections;
import java.util.Set;
import java.util.function.LongSupplier;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.sql.OracleDialectResolver.OracleDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.OracleDDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSQL;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.OracleParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.OracleTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.CaseInsensitiveSet;
import org.codefilarete.tool.collection.Iterables;

/**
 * 
 * @author Guillaume Mary
 */
public class OracleDatabaseSettings extends DatabaseVendorSettings {

	/**
	 * Oracle keywords, took from <a href="https://docs.oracle.com/cd/A97630_01/appdev.920/a42525/apb.htm">Oracle documentation</a> because those of
	 * it JDBC Drivers are not enough / accurate (see {@link oracle.jdbc.OracleDatabaseMetaData#getSQLKeywords()})
	 */
	@VisibleForTesting
	static final String[] KEYWORDS = new String[] {
			// Oracle Reserved Words
			"ACCESS", "ELSE", "MODIFY", "START",
			"ADD", "EXCLUSIVE", "NOAUDIT", "SELECT",
			"ALL", "EXISTS", "NOCOMPRESS", "SESSION",
			"ALTER", "FILE", "NOT", "SET",
			"AND", "FLOAT", "NOTFOUND", "SHARE",
			"ANY", "FOR", "NOWAIT", "SIZE",
			"ARRAYLEN", "FROM", "NULL", "SMALLINT",
			"AS", "GRANT", "NUMBER", "SQLBUF",
			"ASC", "GROUP", "OF", "SUCCESSFUL",
			"AUDIT", "HAVING", "OFFLINE", "SYNONYM",
			"BETWEEN", "IDENTIFIED", "ON", "SYSDATE",
			"BY", "IMMEDIATE", "ONLINE", "TABLE",
			"CHAR", "IN", "OPTION", "THEN",
			"CHECK", "INCREMENT", "OR", "TO",
			"CLUSTER", "INDEX", "ORDER", "TRIGGER",
			"COLUMN", "INITIAL", "PCTFREE", "UID",
			"COMMENT", "INSERT", "PRIOR", "UNION",
			"COMPRESS", "INTEGER", "PRIVILEGES", "UNIQUE",
			"CONNECT", "INTERSECT", "PUBLIC", "UPDATE",
			"CREATE", "INTO", "RAW", "USER",
			"CURRENT", "IS", "RENAME", "VALIDATE",
			"DATE", "LEVEL", "RESOURCE", "VALUES",
			"DECIMAL", "LIKE", "REVOKE", "VARCHAR",
			"DEFAULT", "LOCK", "ROW", "VARCHAR2",
			"DELETE", "LONG", "ROWID", "VIEW",
			"DESC", "MAXEXTENTS", "ROWLABEL", "WHENEVER",
			"DISTINCT", "MINUS", "ROWNUM", "WHERE",
			"DROP", "MODE", "ROWS", "WITH",
			
			// Oracle Keywords
			"ADMIN", "CURSOR", "FOUND", "MOUNT",
			"AFTER", "CYCLE", "FUNCTION", "NEXT",
			"ALLOCATE", "DATABASE", "GO", "NEW",
			"ANALYZE", "DATAFILE", "GOTO", "NOARCHIVELOG",
			"ARCHIVE", "DBA", "GROUPS", "NOCACHE",
			"ARCHIVELOG", "DEC", "INCLUDING", "NOCYCLE",
			"AUTHORIZATION", "DECLARE", "INDICATOR", "NOMAXVALUE",
			"AVG", "DISABLE", "INITRANS", "NOMINVALUE",
			"BACKUP", "DISMOUNT", "INSTANCE", "NONE",
			"BEGIN", "DOUBLE", "INT", "NOORDER",
			"BECOME", "DUMP", "KEY", "NORESETLOGS",
			"BEFORE", "EACH", "LANGUAGE", "NORMAL",
			"BLOCK", "ENABLE", "LAYER", "NOSORT",
			"BODY", "END", "LINK", "NUMERIC",
			"CACHE", "ESCAPE", "LISTS", "OFF",
			"CANCEL", "EVENTS", "LOGFILE", "OLD",
			"CASCADE", "EXCEPT", "MANAGE", "ONLY",
			"CHANGE", "EXCEPTIONS", "MANUAL", "OPEN",
			"CHARACTER", "EXEC", "MAX", "OPTIMAL",
			"CHECKPOINT", "EXPLAIN", "MAXDATAFILES", "OWN",
			"CLOSE", "EXECUTE", "MAXINSTANCES", "PACKAGE",
			"COBOL", "EXTENT", "MAXLOGFILES", "PARALLEL",
			"COMMIT", "EXTERNALLY", "MAXLOGHISTORY", "PCTINCREASE",
			"COMPILE", "FETCH", "MAXLOGMEMBERS", "PCTUSED",
			"CONSTRAINT", "FLUSH", "MAXTRANS", "PLAN",
			"CONSTRAINTS", "FREELIST", "MAXVALUE", "PLI",
			"CONTENTS", "FREELISTS", "MIN", "PRECISION",
			"CONTINUE", "FORCE", "MINEXTENTS", "PRIMARY",
			"CONTROLFILE", "FOREIGN", "MINVALUE", "PRIVATE",
			"COUNT", "FORTRAN", "MODULE", "PROCEDURE",
			
			// Oracle Keywords (continued):
			"PROFILE", "SAVEPOINT", "SQLSTATE", "TRACING",
			"QUOTA", "SCHEMA", "STATEMENT", "ID	TRANSACTION",
			"READ", "SCN", "STATISTICS", "TRIGGERS",
			"REAL", "SECTION", "STOP", "TRUNCATE",
			"RECOVER", "SEGMENT", "STORAGE", "UNDER",
			"REFERENCES", "SEQUENCE", "SUM", "UNLIMITED",
			"REFERENCING", "SHARED", "SWITCH", "UNTIL",
			"RESETLOGS", "SNAPSHOT", "SYSTEM", "USE",
			"RESTRICTED", "SOME", "TABLES", "USING",
			"REUSE", "SORT", "TABLESPACE", "WHEN",
			"ROLE", "SQL", "TEMPORARY", "WRITE",
			"ROLES", "SQLCODE", "THREAD", "WORK",
			"ROLLBACK", "SQLERROR", "TIME"
	};

	// Technical note: DO NOT declare settings BEFORE KEYWORDS field because it requires it and the JVM makes KEYWORDS null at this early stage (strange)
	public static final OracleDatabaseSettings ORACLE_23_0 = new OracleDatabaseSettings();

	private OracleDatabaseSettings() {
		this(new OracleSQLOperationsFactoriesBuilder(), new OracleParameterBinderRegistry());
	}
	
	private OracleDatabaseSettings(OracleSQLOperationsFactoriesBuilder sqlOperationsFactoriesBuilder, OracleParameterBinderRegistry parameterBinderRegistry) {
		super(new OracleDatabaseSignet(23, 0),
				Collections.unmodifiableSet(new CaseInsensitiveSet(KEYWORDS)),
				'"',
				new OracleTypeMapping(),
				parameterBinderRegistry,
				sqlOperationsFactoriesBuilder,
				new OracleGeneratedKeysReaderFactory(),
				1000,
				true);
	}

	private static class OracleSQLOperationsFactoriesBuilder implements SQLOperationsFactoriesBuilder {

		private final ReadOperationFactory readOperationFactory;
		private final OracleWriteOperationFactory writeOperationFactory;

		private OracleSQLOperationsFactoriesBuilder() {
			this.readOperationFactory = new ReadOperationFactory();
			this.writeOperationFactory = new OracleWriteOperationFactory();
		}
		
		private ReadOperationFactory getReadOperationFactory() {
			return readOperationFactory;
		}
		
		private OracleWriteOperationFactory getWriteOperationFactory() {
			return writeOperationFactory;
		}

		@Override
		public SQLOperationsFactories build(ParameterBinderIndex<Column, ParameterBinder> parameterBinders, DMLNameProviderFactory dmlNameProviderFactory, SqlTypeRegistry sqlTypeRegistry) {
			DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, DMLGenerator.NoopSorter.INSTANCE, dmlNameProviderFactory);
			OracleDDLTableGenerator ddlTableGenerator = new OracleDDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
			DDLSequenceGenerator ddlSequenceGenerator = new DDLSequenceGenerator(dmlNameProviderFactory);
			return new SQLOperationsFactories(writeOperationFactory, readOperationFactory, dmlGenerator, ddlTableGenerator, ddlSequenceGenerator, new OracleSequenceSelectorFactory(readOperationFactory));
		}
	}

	private static class OracleSequenceSelectorFactory implements DatabaseSequenceSelectorFactory {

		private final ReadOperationFactory readOperationFactory;

		private OracleSequenceSelectorFactory(ReadOperationFactory readOperationFactory) {
			this.readOperationFactory = readOperationFactory;
		}

		@Override
		public DatabaseSequenceSelector create(org.codefilarete.stalactite.sql.ddl.structure.Sequence databaseSequence, ConnectionProvider connectionProvider) {
			return new DatabaseSequenceSelector(databaseSequence, "select " + databaseSequence.getAbsoluteName() + ".nextval from dual", readOperationFactory, connectionProvider);
		}
	}
	
	/**
	 * {@link WriteOperationFactory} appropriate for Oracle : mainly indicates what columns must be retrieved while
	 * some generated key is expected.
	 *
	 * @author Guillaume Mary
	 * @see OracleGeneratedKeysReader
	 */
	@VisibleForTesting
	static class OracleWriteOperationFactory extends WriteOperationFactory {

		@Override
		public <T extends Table<T>> WriteOperation<Column<T, ?>> createInstanceForInsertion(ColumnParameterizedSQL<T> sqlGenerator,
																							ConnectionProvider connectionProvider,
																							LongSupplier expectedRowCount) {
			// Looking for autogenerated column (identifier policy is "after insertion") : it will be added to PreparedStatement descriptor
			Set<? extends Column<?, ?>> columns = ((ColumnParameterizedSQL<?>) sqlGenerator).getColumnIndexes().keySet();
			Column<?, ?> column = Iterables.find(columns, Column::isAutoGenerated);
			if (column != null) {
				return createInstance(sqlGenerator, connectionProvider,
						// Oracle requires passing the column name to be retrieved in the generated keys, else it gives back the RowId
						(connection, sql) -> connection.prepareStatement(sql, new String[] { column.getName() }), expectedRowCount);
			} else {
				// no autogenerated column => standard behavior
				return super.createInstanceForInsertion(sqlGenerator, connectionProvider, expectedRowCount);
			}
		}
	}
	
	/**
	 * Simple creator of {@link OracleGeneratedKeysReader}.
	 * 
	 * @author Guillaume Mary
	 */
	@VisibleForTesting
	static class OracleGeneratedKeysReaderFactory implements GeneratedKeysReaderFactory {
		
		@Override
		public <I> GeneratedKeysReader<I> build(String keyName, Class<I> columnType) {
			return (GeneratedKeysReader<I>) new OracleGeneratedKeysReader(keyName);
		}
	}
}
