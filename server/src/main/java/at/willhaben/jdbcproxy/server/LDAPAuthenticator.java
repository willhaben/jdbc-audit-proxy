package at.willhaben.jdbcproxy.server;

import at.willhaben.jdbcproxy.server.dao.SessionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.*;
import javax.naming.directory.*;
import java.io.IOException;
import java.util.Hashtable;

/**
 * Implement authentication of users by consulting an external LDAP server.
 */
@SuppressWarnings("squid:S1149") // suppress sonar: Hashtable is part of the legacy LDAP support library
public class LDAPAuthenticator implements Authenticator {
    public static class Config {
        private String host;
        private String distinguishedNameFormat;
        private String groupMemberAttr;
        private String groupFormat;

        /** Name of host on which LDAP server is running. */
        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        /** java.lang.String.format formatter mapping JDBC username to distinguished-name. */
        public String getDistinguishedNameFormat() {
            return distinguishedNameFormat;
        }

        public void setDistinguishedNameFormat(String distinguishedNameFormat) {
            this.distinguishedNameFormat = distinguishedNameFormat;
        }

        /** Attribute on user LDAP record holding their group memberships. */
        public String getGroupMemberAttr() {
            return groupMemberAttr;
        }

        public void setGroupMemberAttr(String groupMemberAttr) {
            this.groupMemberAttr = groupMemberAttr;
        }

        /** java.lang.String.format formatter mapping JDBC database-name to ldap-group-distinguished-name. */
        public String getGroupFormat() {
            return groupFormat;
        }

        public void setGroupFormat(String groupFormat) {
            this.groupFormat = groupFormat;
        }
    }

    private interface Validator {
        void validate(DirContext context, String dn) throws SessionException, NamingException;
    }

    // ============================================================================================

    private static final Logger logger = LoggerFactory.getLogger(LDAPAuthenticator.class);

    private final String host;
    private final String distinguishedNameFormat;
    private final String groupMemberAttr;
    private final String[] groupMemberAttrs;
    private final String groupFormat;
    private final String allGroupName;

    public LDAPAuthenticator(Config config) throws IOException {
        host = config.getHost();
        if (host == null) {
            throw new IOException("LDAP Authenticator enabled, but no LDAP host specified");
        }

        distinguishedNameFormat = config.getDistinguishedNameFormat();
        groupMemberAttr = config.getGroupMemberAttr();
        groupMemberAttrs = new String[] { groupMemberAttr };
        groupFormat = config.getGroupFormat();

        // A special group which allows a user to access all available databases
        allGroupName = String.format(groupFormat, "all");
    }

    public void authenticateAccess(String username, String password, String database) throws SessionException, IOException {
        // Verify that the connecting user is known and credentials are valid.
        // Then also check that they are member of a group that gives them access to the specified database
        String fullGroupName = String.format(groupFormat, database);
        authenticate(
            username,
            password,
            (dc, dn) -> {
                if (!isMemberOf(dc, dn, fullGroupName)) {
                    String errMsg = String.format(
                            "Authentication failed: user %s not member of group %s", dn, fullGroupName);
                    throw new SessionException(errMsg);
                }
            });
    }

    @Override
    public void authenticateGrant(String username, String password, String targetUser, String targetDatabase) throws SessionException, IOException {
        // For session-grants, it is sufficient to verify that the "granting" user is known and credentials are valid.
        // The "granted" user must still have rights to access the granted DB (checked on connect).
        authenticate(
            username,
            password,
            (dc, dn) -> {});
    }

    private void authenticate(String username, String password, Validator validator) throws SessionException, IOException {
        String dn = String.format(distinguishedNameFormat, username);

        // Yes, this code does assume we are running on a Sun JRE...
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put("com.sun.jndi.ldap.read.timeout", "5000");
        env.put("com.sun.jndi.ldap.connect.timeout", "5000");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");  // LDAP password-based authentication
        env.put(Context.PROVIDER_URL, "ldaps://" + host);
        env.put(Context.SECURITY_PRINCIPAL, dn);
        env.put(Context.SECURITY_CREDENTIALS, password);

        DirContext dc = null;
        try {
            dc = new InitialDirContext(env);

            validator.validate(dc, dn);
        } catch (AuthenticationException e) {
            logger.warn("LDAP user credentials invalid: {}", dn, e);
            throw new SessionException("Invalid credentials");
        } catch (NamingException e) {
            throw new IOException("Error while verifying user with LDAP", e);
        } finally {
            if (dc != null) {
                try {
                    dc.close();
                } catch(NamingException e) {
                    // ignore - can't do much else here
                }
            }
        }
    }

    private boolean isMemberOf(DirContext context, String dn, String groupName) throws NamingException {
        // Fetch just the "groups" attribute from the LDAP record for this user
        // (while connected to LDAP as the same user)
        Attributes attributes = context.getAttributes(dn, groupMemberAttrs);
        NamingEnumeration<? extends Attribute> all = attributes.getAll();
        while (all.hasMoreElements()) {
            Attribute next = all.next();

            String key = next.getID();
            if (!groupMemberAttr.equals(key)) {
                throw new NamingException("Unexpected attr found:" + key);
            }

            var groups = next.getAll();
            while (groups.hasMoreElements()) {
                var curr = groups.next();
                String groupDn = curr.toString();
                if (groupName.equals(groupDn) || allGroupName.equals(groupDn)) {
                    return true;
                }
            }
        }

        return false;
    }
}
