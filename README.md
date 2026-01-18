**Stalactite** is a fluent ORM for Java that maps beans using *Method References* without bytecode enhancement. By providing a *direct and clear view of your mapping*, it ensures that persistence complexity remains visible and manageable. Because the mapping stays outside of your beans, it naturally **embraces Clean and Hexagonal Architectures** by keeping persistence logic entirely external.


[![Build Status](https://ci.codefilarete.org/jenkins/buildStatus/icon?job=Stalactite+pipeline)](https://ci.codefilarete.org/jenkins/job/Stalactite%20pipeline/)
[![Quality Gate Status](https://ci.codefilarete.org/sonar/api/project_badges/measure?project=Stalactite&metric=alert_status)](https://ci.codefilarete.org/sonar/dashboard?id=Stalactite)
[![Coverage](https://ci.codefilarete.org/sonar/api/project_badges/measure?project=Stalactite&metric=coverage)](https://ci.codefilarete.org/sonar/dashboard?id=Stalactite)
[![Vulnerabilities](https://ci.codefilarete.org/sonar/api/project_badges/measure?project=Stalactite&metric=vulnerabilities)](https://ci.codefilarete.org/sonar/dashboard?id=Stalactite)
[!["Buy Me A Coffee"](https://img.shields.io/badge/buy%20me%20a%20coffee-donate-yellow.svg)](https://www.buymeacoffee.com/codefilarete)

---

# ⚡ Example Usage

Here's an example of one can achieve with the ORM module :
```java
DataSource dataSource = ... // use whatever JDBC DataSource you want
PersistenceContext persistenceContext = new PersistenceContext(dataSource, HSQLDBDialectBuilder.defaultHSQLDBDialect());

EntityPersister<Country, Long> countryPersister = MappingEase.entityBuilder(Country.class, long.class)
    .mapKey(Country::getId, IdentifierPolicy.afterInsert())
    .map(Country::getName)
    .map(Country::getDescription)
    .mapOneToOne(Country::getPresident, MappingEase.entityBuilder(Person.class, long.class)
        .mapKey(Person::getId, IdentifierPolicy.afterInsert())
        .map(Person::getName))
    .build(persistenceContext);

Country country = new Country(42L);
country.setPresident(new Person(12L));
countryPersister.persist(country);


List<Car> allCars = persistenceContext.newQuery("select id, model, rgb from Car", Car.class)
    .mapKey(Car::new, "id", long.class)
    .map("model", Car::setModel)
    .map("rgb", Car::setColor, int.class, Color::new)
    .execute();
```

---

# 💡 Philosophy

*   **Zero Annotations/XML**: Mapping is defined via a Fluent API and method references. No bytecode manipulation (CGLib/AspectJ) is performed on your beans.
*   **Clean/Hexagonal Architecture**: Keep your domain model pure. Persistence logic remains entirely external to your business entities.
*   **Promotes Domain Aggregates**: No lazy-loading magic. Eager fetching promotes a better understanding of query complexity and performance.
*   **No "Session" Complexity**: Simplifies the programming model by avoiding "merge/attach" states typical in JPA.

---

---

# 🌍 Overview

The project is structured into three main layers:

*   📦 **[SQL](sql/README.md)**: Low-level JDBC helper and statement builders.
*   📦 **[Core](core/README.md)**: Basic persistence and mapping logic.
*   📦 **[ORM](orm/README.md)**: High-level entity mapping and relationship management.

---

# 🛠 Installation

Stalactite ORM module is available from Maven will below coordinates

```xml
<dependency>
    <groupId>org.codefilarete.stalactite</groupId>
    <artifactId>orm</artifactId>
    <version>${stalactite.version}</version>
</dependency>
```

Then you'll have to add Database Vendor adapter such as MariaDB one
```xml
<dependency>
    <groupId>org.codefilarete.stalactite</groupId>
    <artifactId>core-mariadb-adapter</artifactId>
    <version>${stalactite.version}</version>
</dependency>
```

For now, here are the supported databases and their matching adapters:

| Database   | Artifact ID               |
|------------|---------------------------|
| MariaDB    | `core-mariadb-adapter`    |
| MySQL      | `core-mysql-adapter`      |
| PostgreSQL | `core-postgresql-adapter` |
| HSQLDB     | `core-hsqldb-adapter`     |
| H2         | `core-h2-adapter`         |


If you're only interested in the SQL module, you only need the sql adapter such as MariaDB one:
```xml
<dependency>
    <groupId>org.codefilarete.stalactite</groupId>
    <artifactId>sql-mariadb-adapter</artifactId>
    <version>${stalactite.version}</version>
</dependency>
```

Finally, if you use Stalactite integrated in a Spring project which manages your transactions, you’ll have to add the following dependency to make Stalactite use them:
```xml
<dependency>
    <groupId>org.codefilarete.stalactite</groupId>
    <artifactId>spring-integration</artifactId>
    <version>${stalactite.version}</version>
</dependency>
```
And then use one of the subclasses of [PlatformTransactionManagerConnectionProvider](spring-integration/src/main/java/org/codefilarete/stalactite/sql/spring/PlatformTransactionManagerConnectionProvider.java) as the datasource of your Stalactite `PersistenceContext` by giving it your Spring `PlatformTransactionManger`.

---

# 📖 Documentation

Detailed guides and API references are available at the **[Codefilarete Official Site](https://www.codefilarete.org/stalactite-doc/2.0.0/)**.
