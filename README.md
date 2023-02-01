# JDBC Audit Proxy

This project provides a way for IT staff to connect to a database and submit raw SQL queries, ie enables:

* access from a non-production network to production databases (solves network restrictions)
* control over who can perform that task (ie supports complex authorization rules)
* logging all executed queries for auditing purposes (makes data misuse detectable)

In particular, this is intended to allow technical support to be provided for production systems while complying
with the requirements of privacy laws such as the European GDPR which allow access to privacy-sensitive data only
"with justified reason" and with appropriate auditing in place to detect abuse of that right.

# Components

This project provides:

* a server application (packed as a container image)
* a custom JDBC driver which can talk to that server

The server must be installed such that it is reachable from the development network, and can reach production databases.
It is configured with credentials that allow it to create JDBC connections to various databases, typically as
some read-only user.

The driver is then used in any JDBC-compliant DB GUI tool, eg Squirrel or the DB plugin for Intellij IDEA. Opening a
connection via such a tool causes the driver to open a socket from the (developer) system on which it is run to the
server, and provide credentials. The server verifies these credentials. The UI tool can then be used to define and
execute DB queries; the driver sends the query (as a compact binary message) to the server which logs the query being
executed (along with the id of the user executing it) then forwards it to the target production database (using
JDBC). Results from the database are forwarded from the server back to the driver which presents them to the UI.

The actual rules used by the server to verify the credentials are flexible. They can be a simple LDAP query to
ensure the specified (user,pwd) are valid and that the user has a specific role. Alternatively more complex
systems can be built, eg one in which temporary credentials are generated that must be approved by another
user ("4 eyes access"). Such mechanisms do need to be added to the code, but the framework for such authorization
is provided by the server component.

This proxy supports only READ operations on the target databases, ie is useful for investigating issues. Direct
modification ("fixes") of data must be done via other means (hopefully version-controlled ones!).

This solution has moderately high performance overhead, ie may not be appropriate for use by automated tools
which perform many queries or download significant amounts of data. However for interactive use it is adequate.

The JDBC driver supports only the minimum of functions required for a DB GUI interactive tool. In particular,
it does not support PreparedStatements - and therefore cannot be used as a driver by general-purpose applications.

This driver has been tested with the following jdbc-client tools:

* [SQuirreL SQL](https://squirrel-sql.sourceforge.io/)
* [DBeaver](https://dbeaver.io/)
* [Intellij IDEA](https://www.jetbrains.com/idea/) standard database plugin

# Installation Process

The general requirements are:

* the proxy must be installed in a network environment where it can access all the target databases while
  also being reachable by all potential users (eg from the development networks).
* the proxy can optionally be run as a cluster (to support multiple users or for high availability), in which
  case a load-balancer is required.
* an authentication-source must be provided
  * when using "file-based authentication", a (read-only) textfile of users must be made available to each proxy
    server instance
  * when using LDAP authentication, the URL of an LDAP server must be provided
* if audit-trails and sessions should be stored in a database (recommended), then a Postgres instance must be
  provided for the use of the proxy.
* the proxy must be configured with login credentials for each target database. It is recommended that these
  accounts be "read-only" users (ie with permission to SELECT but not UPDATE). The proxy supports only READ 
  access to databases, but enforcing this additionally via the (proxy-to-database) account is good security practice.
* each potential user must be given a copy of the application's "jdbc driver" jarfile (which is also the
  commandline tool used for tasks such as pre-registering sessions).


# Development and Testing

The server can be started in an IDE in the obvious way.

The client can be built using maven, and then any JDBC-compliant tool can be configured with the resulting "fat jar file" as the driver. To debug, start the UI tool with the following options then connect the IDE debugger to this "remote application":

```
JDK_JAVA_OPTIONS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=1044 {whatever-tool-you-use}
```

This definitely works for DB GUI tool [http://squirrel-sql.sourceforge.net].

An audit-database can be started locally with:

```
docker run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
```

Then execute file `dbsetup.sql` with whatever client you wish.

See [the official postgres image docs](https://hub.docker.com/_/postgres) for more info.

# Building

Java 17 or later is required. Note that module "server" produces java17-compatible output, but that module "driver"
produces java11-compatible output in order to be usable with a wider range of applications.

# Current Code Status

This application was developed for internal purposes. The code-quality is reasonable but far from perfect. In particular:

* error-handling is not elegant (lots of generic exception-types used)
* few automated tests
* several JDBC features are stubbed out because they aren't needed for our specific use-cases.
* and documentation can of course always be improved.

# Suggested Improvements

## Audit Trail

Currently there are two options for producing audit-logs:

* slf4j (ie info on who is doing what is written as normal log-messages) 
* database (info is inserted into a postgres instance)

It might be nice to have the option of emitting audit info to a message-broker (JMS and/or Kafka). Some other
separate component could then read these logs and store them appropriately. This would allow audit-info from
the jdbcauditproxy to be centralized (potentially combined with other existing audit trails).

## Security

It might be nice to support Kerberos tickets for authentication/authorization.

And TLS support would be nice too, as noted under "Network Traffic Encryption" below.

# Security Information

## Network Traffic Encryption

Currently network traffic between the JDBC driver and the proxy is not encrypted. Whether this is important or not
depends upon your threat model - though if you are concerned that an attacker may have control over your network routers
or load-balancers then you may have more significant issues to deal with.

Given that the server and driver are delivered as a pair, it is probably possible to create a self-signed certificate
and embed it in both the client and server parts, ie having access to a certificate-signing authority to generate
proper certs is not needed. The server could then on startup generate a new cert, and sign it with the embedded cert
(which the client trusts), resulting in encrypted network traffic with little hassle for client or server. With this
setup, there is a weakness that an attacker with access to the same server binary could impersonate the server (provide
their own one), but that's a relatively unlikely attack vector. Of course the server could also be provided with a
proper signed cert - though that's quite a hassle for an internal tool that itself is only useful together with valid
credentials for some user. Anyway, if you wish to implement any of these options, your contribution is welcome.

## User Credentials

User passwords are passed from the JDBC driver to the proxy, which then uses them to validate (against local file or
LDAP). An attacker with control over the jdbcauditproxy server could therefore intercept user credentials. An
alternative would be to integrate Kerberos or similar, allowing just a token to be passed from driver to server
rather than the underlying password - but this would be a moderately complex task.

# Alternatives to this Application

## Dynamic Permissions

A clean way to solve the requirements listed above is a "temporary permissions" system, ie to:

* integrate all target databases with an "internal user authentication system"
* provide a system that can modify a user's permissions so that they are granted the necessary db-login rights
  as and when they need them - and ensures these rights are removed afterwards.
* and enable native audit-trails within each target database (somehow centralizing the info if your environment
  includes multiple databases)

When using managed databases in a cloud environment, this could be a good approach. In other contexts, however,
this can be difficult/impossible - including for the employer of the original author of this tool.
The proxy-server approach has significantly poorer performance, and limited features. However it is relatively
easy to integrate into an existing environment, and "good enough" for the purpose of (infrequently) investigating
issues in production databases.

## Commercial Tools

The following are "heavy-weight" products with many features - some of which might possibly solve the requirements listed in the introductions section above.

* IBM Guardium Data Activity Monitor [official page](https://www.ibm.com/docs/en/guardium/10.0?topic=overview-guardium) / [overview](https://www.techtarget.com/searchsecurity/feature/IBM-Guardium-Database-security-tool-overview) - "Prospective customers must contact an IBM sales representative for pricing information specific to their environments." but [according to one comment](https://www.peerspot.com/questions/what-is-your-experience-regarding-pricing-and-costs-for-ibm-guardium-data-protection)
: "One of the deployments that I know of had three databases, and the yearly fees are approximately $50,000 USD." 
* Imperva SecureSphere
* [Oracle Audit Vault](https://www.oracle.com/security/database-security/audit-vault-database-firewall/) - just provides the "audit trail" part of the requirements above;  providing users with network connectivity and credentials to the target database is not included.
* McAfee DLP

Other Information:

*  [How to enable auditing for a specific user in an Oracle DB](https://docs.oracle.com/cd/E11882_01/network.112/e36292/auditing.htm#DBSEG725)

## Other Options

The following have some features that address the use-case for this tool (GDPR-compliant access to a set of databases).
However none of these completely solve the issue (otherwise this project would not exist!).

*  [ha-jdbc](http://ha-jdbc.org/doc.html) - a JDBC driver which distributes requests over a pool of connections to a clustered database. Client-side tool only, with no "proxy server", no custom authorization, and no auditing support.
*  [Amazon RDS proxy](https://aws.amazon.com/rds/proxy/) - solves the "network connectivity" part of the requirements, and the user-credentials (client authenticates to proxy, and only proxy has the DB credentials). However this does not address the auditing requirement - and only supports specific databases on AWS.
*  [Google Cloud SQL Auth Proxy](https://cloud.google.com/sql/docs/mysql/sql-proxy) - [sourcecode](https://github.com/GoogleCloudPlatform/cloudsql-proxy) - solves network connectivity problem and credentials prohlem. However this does not address auditing - and is tightly coupled to GCP authorization.
*  [](https://github.com/kloeckner-i/db-auth-gateway) - derived from Google Cloud SQL Auth Proxy
