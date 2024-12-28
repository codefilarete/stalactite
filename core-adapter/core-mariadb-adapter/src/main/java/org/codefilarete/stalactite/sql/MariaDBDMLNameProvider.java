package org.codefilarete.stalactite.sql;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;

/**
 * @author Guillaume Mary
 */
public class MariaDBDMLNameProvider extends DMLNameProvider {
	
	/** MariaDB keywords to be escaped. From {@link org.mariadb.jdbc.DatabaseMetaData#getSQLKeywords()} (client 3.0.7) */
	public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER, "ACCESSIBLE", "ANALYZE", "ASENSITIVE", "BEFORE", "BIGINT", "BINARY", "BLOB", "CALL", "CHANGE", "CONDITION", "DATABASE", "DATABASES",
			"DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DELAYED", "DETERMINISTIC", "DISTINCTROW", "DIV", "DUAL", "EACH",
			"ELSEIF", "ENCLOSED", "ESCAPED", "EXIT", "EXPLAIN", "FLOAT4", "FLOAT8", "FORCE", "FULLTEXT", "GENERAL", "HIGH_PRIORITY",
			"HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IGNORE_SERVER_IDS", "INDEX", "INFILE", "INOUT", "INT1", "INT2",
			"INT3", "INT4", "INT8", "ITERATE", "KEY", "KEYS", "KILL", "LEAVE", "LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK",
			"LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY", "MASTER_HEARTBEAT_PERIOD", "MASTER_SSL_VERIFY_SERVER_CERT",
			"MAXVALUE", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES",
			"NO_WRITE_TO_BINLOG", "OPTIMIZE", "OPTIONALLY", "OUT", "OUTFILE", "PURGE", "RANGE", "READ_WRITE", "READS", "REGEXP", "RELEASE",
			"RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESIGNAL", "RESTRICT", "RETURN", "RLIKE", "SCHEMAS", "SECOND_MICROSECOND", "SENSITIVE",
			"SEPARATOR", "SHOW", "SIGNAL", "SLOW", "SPATIAL", "SPECIFIC", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT",
			"SQLEXCEPTION", "SSL", "STARTING", "STRAIGHT_JOIN", "TERMINATED", "TINYBLOB", "TINYINT", "TINYTEXT", "TRIGGER", "UNDO", "UNLOCK",
			"UNSIGNED", "USE", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VARBINARY", "VARCHARACTER", "WHILE", "XOR", "YEAR_MONTH", "ZEROFILL",
			"label", "id", "owner", "idx", "ConstantChoice"
	));
	
	public MariaDBDMLNameProvider(Function<Fromable, String> tableAliaser) {
		super(tableAliaser);
	}
	
	public MariaDBDMLNameProvider(Map<Table, String> tableAliases) {
		super(tableAliases);
	}
	
	@Override
	public String getSimpleName(Selectable<?> column) {
		if (KEYWORDS.contains(column.getExpression())) {
			return "`" + column.getExpression() + "`";
		}
		return super.getSimpleName(column);
	}
	
	@Override
	public String getName(Fromable table) {
		if (KEYWORDS.contains(table.getName())) {
			return "`" + super.getName(table) + "`";
		}
		return super.getName(table);
	}
}
