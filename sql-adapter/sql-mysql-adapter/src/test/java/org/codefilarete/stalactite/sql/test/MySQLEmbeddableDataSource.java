package org.codefilarete.stalactite.sql.test;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.config.SchemaConfig;
import org.codefilarete.stalactite.sql.UrlAwareDataSource;

import static com.wix.mysql.config.Charset.UTF8;
import static com.wix.mysql.distribution.Version.v5_6_latest;

/**
 * Simple MariaDB DataSource for tests
 *
 * @author Guillaume Mary
 */
public class MySQLEmbeddableDataSource extends UrlAwareDataSource implements Closeable {
	
	public static final int DEFAULT_PORT = 3306;
	
	private static final Map<Integer, EmbeddedMysql> USED_PORTS = new HashMap<>();
	
	private EmbeddedMysql db;
	
	/** DB port. Stored because configuration of DB is not accessible. */
	private final int port;
	
	public MySQLEmbeddableDataSource() {
		this(DEFAULT_PORT);
	}
	
	public MySQLEmbeddableDataSource(int port) {
		// "random" URL to avoid collision in tests
		this(port, "test" + Integer.toHexString(new Random().nextInt()));
	}
	
	private MySQLEmbeddableDataSource(int port, String databaseName) {
		super("jdbc:mariadb://localhost:" + port + "/" + databaseName);
		this.port = port;
		start();
		db.addSchema(SchemaConfig.aSchemaConfig(databaseName).build());

		MySQLDataSource delegate = new MySQLDataSource("localhost:" + port, databaseName, db.getConfig().getUsername(), db.getConfig().getPassword());
		setDelegate(delegate);
	}
	
	private void start() {
		db = USED_PORTS.get(port);
		if (db == null) {
			MysqldConfig config = MysqldConfig.aMysqldConfig(v5_6_latest)
				.withCharset(UTF8)
				.withPort(port)
				.withUser("dev", "dev")
				// avoid warning popup about incoming network connections acceptance on some OS
				// see https://github.com/wix/wix-embedded-mysql/issues/122
				.withServerVariable("bind-address", "localhost")
				.build();
			
			// /!\ if Mysql doesn't start under Windows, you may miss MSVCR100.dll,
			// which is available in Microsoft Visual Studio 2010 redistributable package, download it and install it
			db = com.wix.mysql.EmbeddedMysql.anEmbeddedMysql(config).start();
			USED_PORTS.put(port, db);
		}
	}
	
	@Override
	public void close() {
		USED_PORTS.get(port).stop(); //optional, as there is a shutdown hook
	}
}
