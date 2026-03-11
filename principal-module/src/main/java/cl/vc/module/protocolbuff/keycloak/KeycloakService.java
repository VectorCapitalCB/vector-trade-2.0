package cl.vc.module.protocolbuff.keycloak;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.crypt.AESEncryption;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import jakarta.ws.rs.core.Response;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.admin.client.CreatedResponseUtil;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.*;
import org.keycloak.representations.AccessToken;
import org.keycloak.representations.AccessTokenResponse;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.keycloak.util.JsonSerialization;


import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Data
public class KeycloakService {

    private String realm;
    private String keycloakUrl;
    private String clientId;
    private String clientSecret;
    private String adminUsername;
    private String adminPassword;

    @Getter
    private static final String CODE_OPERATOR = "CODE_OPERATOR";
    @Getter
    private static final String DESTINATION_ROUTING = "DESTINATION_ROUTING";
    @Getter
    private static final String DESTINO_MKD = "DESTINATION_MKD";
    @Getter
    private static final String BROKER = "BROKER";
    @Getter
    private static final String STRATEGY = "STRATEGY";
    @Getter
    private static final String DEFAULT_ROUTING = "DEFAULT_ROUTING";
    @Getter
    private static final String PERFIL = "PERFIL";

    @Getter
    private static final String ROLES = "ROLES";
    @Getter
    private static final String ACCOUNT = "ACCOUNT";
    @Getter
    private static final String CODE = "CODE";

    public KeycloakService(String adminUsername, String adminPassword, String keycloakUrl, String realm, String clientId, String clientSecret) {

        try {

            this.adminUsername = adminUsername;
            this.adminPassword = adminPassword;
            this.realm = realm;
            this.keycloakUrl = keycloakUrl;
            this.clientId = clientId;
            this.clientSecret = clientSecret;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void changePassword(String username, String newPassword) throws Exception {

        try {

            String decryptedUsername = AESEncryption.decrypt(username);
            String decryptedPassword = AESEncryption.decrypt(newPassword);

            Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
            UsersResource usersResource = keycloak.realm(realm).users();
            List<UserRepresentation> users = usersResource.search(decryptedUsername, true);
            UserRepresentation user = users.stream()
                    .filter(u -> u.getUsername().equalsIgnoreCase(decryptedUsername))
                    .findFirst()
                    .orElse(null);

            if (user == null) {
                log.error("Usuario no encontrado: {}", decryptedUsername);
                return;
            }

            CredentialRepresentation credential = new CredentialRepresentation();
            credential.setType(CredentialRepresentation.PASSWORD);
            credential.setValue(decryptedPassword);
            credential.setTemporary(false);

            usersResource.get(user.getId()).resetPassword(credential);
            usersResource.get(user.getId()).logout();

            keycloak.close();

            log.info("Contraseña cambiada y sesiones invalidadas para el usuario {}", decryptedUsername);

        } catch (Exception e) {
            throw new Exception("Error al cambiar la contraseña para el usuario " + username + e.getMessage());
        }
    }


    public List<UserRepresentation> getAllUseres() throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        UsersResource usersResource = keycloak.realm(realm).users();
        List<UserRepresentation> x = usersResource.list();
        keycloak.close();
        return x;
    }

    @Deprecated
    public HashMap<String, List<String>> getRolesKeycloak(String username, String password) throws Exception {

        Keycloak keycloak = KeycloakBuilder.builder()
                .serverUrl(keycloakUrl)
                .realm(realm)
                .clientId(clientId)
                .username(username)
                .password(password)
                .clientSecret(clientSecret)
                .build();

        //validamos que el usuario tenga un token valido
        AccessTokenResponse tokenResponse = keycloak.tokenManager().getAccessToken();
        String token = tokenResponse.getToken();
        AccessToken accessToken = JsonSerialization.readValue(Base64.getDecoder().decode(token.split("\\.")[1]), AccessToken.class);
        String userId = accessToken.getSubject();

        //pero para saber los datos de todos los grupos necesitamos un usuario admin
        //para eso usamos el keycloak de arrba


        UserRepresentation userIds = getUserByUsername(username);
        UserResource userResource = keycloak.realm(this.realm).users().get(userIds.getId());
        List<GroupRepresentation> groupIds = userResource.groups();

        List<String> defaultRouting = new ArrayList<>();
        List<String> strategy = new ArrayList<>();
        List<String> destinoMKD = new ArrayList<>();
        List<String> account = new ArrayList<>();

        List<String> roles = new ArrayList<>();
        List<String> destinoRouting = new ArrayList<>();
        List<String> brokers = new ArrayList<>();
        List<String> code = new ArrayList<>();

        userResource.roles().getAll().getRealmMappings().forEach(s -> {
            roles.add(s.getName());
        });

        RealmResource realm = keycloak.realm(this.realm);

        groupIds.forEach(s -> {

            List<GroupRepresentation> groupList = realm.groups().groups(s.getName(), 0, 1);

            groupList.forEach(r -> {

                r.getSubGroups().forEach(x -> {

                    GroupResource groupResource = realm.groups().group(x.getId());
                    GroupRepresentation group = groupResource.toRepresentation();

                    group.getRealmRoles().forEach(t -> {
                        if (!roles.contains(t)) {
                            roles.add(t);
                        }

                    });

                    if (group.getAttributes().containsKey("broker")) {
                        group.getAttributes().get("broker").forEach(t -> {
                            if (!brokers.contains(t)) {
                                brokers.add(t);
                            }
                        });
                    }

                    if (group.getAttributes().containsKey("destinoRouting")) {
                        group.getAttributes().get("destinoRouting").forEach(t -> {
                            if (!destinoRouting.contains(t)) {
                                destinoRouting.add(t);
                            }
                        });
                    }


                    if (group.getAttributes().containsKey("destinoMKD")) {
                        group.getAttributes().get("destinoMKD").forEach(t -> {
                            if (!destinoMKD.contains(t)) {
                                destinoMKD.add(t);
                            }
                        });
                    }

                    if (group.getAttributes().containsKey("strategy")) {
                        group.getAttributes().get("strategy").forEach(t -> {
                            if (!strategy.contains(t)) {
                                strategy.add(t);
                            }
                        });
                    }

                    if (group.getAttributes().containsKey("defaultRouting")) {
                        group.getAttributes().get("defaultRouting").forEach(t -> {
                            if (!defaultRouting.contains(t)) {
                                defaultRouting.add(t);
                            }
                        });
                    }

                });
            });
        });


        Map<String, List<String>> attributes = userResource.toRepresentation().getAttributes();
        if (attributes != null && attributes.containsKey("account") && attributes.get("account") != null) {
            attributes.get("account").forEach(s -> {
                if (!account.contains(s)) {
                    account.add(s);
                }

            });
        }

        if (attributes != null && attributes.containsKey("codeOperator") && attributes.get("codeOperator") != null) {
            attributes.get("codeOperator").forEach(s -> {
                if (!code.contains(s)) {
                    code.add(s);
                }

            });
        }

        HashMap<String, List<String>> rolesMaps = new HashMap<>();
        rolesMaps.put(ROLES, roles);
        rolesMaps.put(DEFAULT_ROUTING, defaultRouting);
        rolesMaps.put(DESTINATION_ROUTING, destinoRouting);
        rolesMaps.put(STRATEGY, strategy);
        rolesMaps.put(DESTINO_MKD, destinoMKD);
        rolesMaps.put(ACCOUNT, account);
        rolesMaps.put(BROKER, brokers);
        rolesMaps.put(CODE, code);

        keycloak.close();

        return rolesMaps;
    }


    public void updateUser(BlotterMessage.User user) throws Exception {

        if (!user.getEmail().isEmpty()) {
            updateUserEmail(user.getEmail(), user.getId());
        }

        if (!user.getFname().isEmpty()) {
            updateUserFname(user.getFname(), user.getId());
        }

        if (!user.getLname().isEmpty()) {
            updateUserLname(user.getLname(), user.getId());
        }

        if (!user.getPhone().isEmpty()) {
            updateUserPhone(user.getPhone(), user.getId());
        }
    }

    public BlotterMessage.User getRolesUserKeycloak(String username) throws Exception {

        try (Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl)
                .realm(this.realm).username(this.adminUsername).password(this.adminPassword)
                .clientId(this.clientId).clientSecret(this.clientSecret).build()) {

            List<UserRepresentation> users = keycloak.realm(realm).users().search(username);
            if (users.isEmpty()) {
                return null;
            }

            UserResource userResource = keycloak.realm(this.realm).users().get(users.getFirst().getId());
            UserRepresentation userRepresentation = userResource.toRepresentation();

            BlotterMessage.User.Builder user = BlotterMessage.User.newBuilder();
            BlotterMessage.UserRolesMaps.Builder userRolesMaps = BlotterMessage.UserRolesMaps.newBuilder();

            Map<String, List<String>> attributes = userRepresentation.getAttributes();
            user.setId(users.getFirst().getId());

            userResource.roles().getAll().getRealmMappings().forEach(s -> {
                userRolesMaps.addAccess(s.getName());
            });


            if (attributes != null) {

                if (attributes.containsKey("codeOperator")) {
                    userRolesMaps.addAllCodeOperator(attributes.get("codeOperator"));
                }

                if (attributes.containsKey("destinoRouting")) {
                    attributes.get("destinoRouting").forEach(s -> {
                        try {
                            userRolesMaps.addDestinoRouting(RoutingMessage.SecurityExchangeRouting.valueOf(s));
                        } catch (IllegalArgumentException e) {
                            log.error(e.getMessage(), e);
                        }
                    });
                }

                if (attributes.containsKey("destinoMKD")) {
                    attributes.get("destinoMKD").forEach(s -> {
                        try {
                            userRolesMaps.addDestinoMKD(MarketDataMessage.SecurityExchangeMarketData.valueOf(s));
                        } catch (IllegalArgumentException e) {
                            log.error(e.getMessage(), e);
                        }
                    });
                }

                if (attributes.containsKey("broker")) {
                    attributes.get("broker").forEach(s -> {
                        try {
                            userRolesMaps.addBroker(RoutingMessage.ExecBroker.valueOf(s));
                        } catch (IllegalArgumentException e) {
                            log.error(e.getMessage(), e);
                        }
                    });
                }

                if (attributes.containsKey("strategy")) {
                    attributes.get("strategy").forEach(s -> {
                        try {
                            userRolesMaps.addStrategy(RoutingMessage.StrategyOrder.valueOf(s));
                        } catch (IllegalArgumentException e) {
                            log.error(e.getMessage(), e);
                        }
                    });
                }

                if (attributes.containsKey("defaultRouting")) {
                    try {
                        userRolesMaps.addAllDefaultRouting(attributes.get("defaultRouting"));
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

                if (attributes.containsKey("perfil")) {
                    List<String> perfilList = attributes.get("perfil");
                    if (!perfilList.isEmpty()) {
                        userRolesMaps.setPerfil(perfilList.getFirst());
                    }
                }

                if (attributes.containsKey("palanca")) {
                    List<String> palancaList = attributes.get("palanca");
                    if (!palancaList.isEmpty()) {
                        userRolesMaps.setPalanca(palancaList.getFirst());
                    }
                }
            }

            AccessTokenResponse tokenResponse = null;
            try {

                tokenResponse = keycloak.tokenManager().getAccessToken();

            } catch (Exception e) {
                throw new RuntimeException("usuario sin token");
            }

            user.setRoles(userRolesMaps);
            user.setToken(tokenResponse.getToken());


            AtomicReference<Boolean> isadmin = new AtomicReference<>(false);

            List<RoleRepresentation> roles = userResource.roles().realmLevel().listEffective();

            roles.forEach(s -> {
                if (s.getName().equals("admin_user")) {
                    isadmin.set(true);
                }
            });

            user.setIsAdmin(isadmin.get());
            user.setId(userRepresentation.getId());
            user.setUsername(userRepresentation.getUsername());

            if (userRepresentation.getEmail() != null && userRepresentation.getEmail().isEmpty()) {
                user.setEmail(userRepresentation.getEmail());
            }

            user.setFname(userRepresentation.getFirstName() != null ? userRepresentation.getFirstName() : "");
            user.setLname(userRepresentation.getLastName() != null ? userRepresentation.getLastName() : "");

            if (attributes != null && attributes.containsKey("marginaccount")) {
                user.addAllMarginaccount(attributes.get("marginaccount"));
            }

            if (attributes != null && attributes.containsKey("phone")) {
                List<String> phoneList = attributes.get("phone");
                if (!phoneList.isEmpty()) {
                    user.setPhone(phoneList.getFirst());
                }
            }

            if (attributes != null && attributes.containsKey("account")) {
                user.addAllAccount(attributes.get("account"));
            }

            user.setActive(userRepresentation.isEnabled());

            if (attributes != null && attributes.containsKey("statusUser")) {
                user.setStatusUser(BlotterMessage.StatusUser.valueOf(attributes.get("statusUser").getFirst()));
            }

            if (attributes != null && attributes.containsKey("token")) {
                user.setToken(attributes.get("token").getFirst());
            }

            if (attributes != null && attributes.containsKey("password")) {
                user.setPassword(attributes.get("password").getFirst());
            }

            return user.build();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new Exception(e.getMessage());

        }
    }


    public UserRepresentation getUserByUsername(String username) throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        List<UserRepresentation> users = keycloak.realm(realm).users().search(username);
        if (users.isEmpty()) {
            return null;
        }
        UserRepresentation x = users.getFirst();
        keycloak.close();
        return x;

    }

    public String getTokenKeycloak(String username, String password) throws Exception {


        Keycloak keycloak = KeycloakBuilder.builder()
                .serverUrl(keycloakUrl)
                .realm(realm)
                .username(username)
                .password(password)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();

        AccessTokenResponse tokenResponse = keycloak.tokenManager().getAccessToken();
        String token = tokenResponse.getToken();
        keycloak.close();
        return token;
    }


    public List<GroupRepresentation> getGropupByUsers(String username) throws Exception {

        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();

        List<UserRepresentation> users = keycloak.realm(realm).users().search(username);

        if (!users.isEmpty()) {
            UserRepresentation user = users.getFirst();
            String userId = user.getId();
            List<GroupRepresentation> x = keycloak.realm(realm).users().get(userId).groups();
            keycloak.close();
            return x;
        }

        return null;
    }


    public boolean validateToken(String token) throws Exception {

        Keycloak keycloak = KeycloakBuilder.builder()
                .serverUrl(keycloakUrl)
                .realm(realm)
                .clientId(clientId)
                .authorization(token)
                .build();

        UsersResource usersResource = keycloak.realm(realm).users();
        List<UserRepresentation> users = usersResource.list();
        keycloak.close();
        return true;
    }


    public void updateUserMargin(String userId, String newMargin) throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        UsersResource usersResource = keycloak.realm(realm).users();
        UserRepresentation user = usersResource.get(userId).toRepresentation();
        user.getAttributes().put("margin", Collections.singletonList(newMargin));
        usersResource.get(userId).update(user);
        keycloak.close();
    }

    public void updateUserEmail(String userId, String newEmail) throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        UsersResource usersResource = keycloak.realm(realm).users();
        UserRepresentation user = usersResource.get(userId).toRepresentation();
        user.setEmail(newEmail);
        usersResource.get(userId).update(user);
        keycloak.close();
    }

    public void updateUserFname(String userId, String newFname) throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        UsersResource usersResource = keycloak.realm(realm).users();
        UserRepresentation user = usersResource.get(userId).toRepresentation();
        user.setFirstName(newFname);
        usersResource.get(userId).update(user);
        keycloak.close();
    }

    public void updateUserLname(String userId, String newLname) throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        UsersResource usersResource = keycloak.realm(realm).users();
        UserRepresentation user = usersResource.get(userId).toRepresentation();
        user.setLastName(newLname);
        usersResource.get(userId).update(user);
        keycloak.close();
    }

    public void updateUserPhone(String userId, String newPhone) {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        UsersResource usersResource = keycloak.realm(realm).users();
        UserRepresentation user = usersResource.get(userId).toRepresentation();
        user.getAttributes().put("phone", Collections.singletonList(newPhone));
        usersResource.get(userId).update(user);
        keycloak.close();
    }

    public String getUserID(String username) {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        List<UserRepresentation> users = keycloak.realm(realm).users().search(username);

        if (users.isEmpty()) {
            return null;
        }
        String id = users.getFirst().getId();
        keycloak.close();
        return id;
    }


    public void updateUserAccount(String userId, String account) throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        UsersResource usersResource = keycloak.realm(realm).users();
        UserRepresentation userRepresentation = usersResource.get(userId).toRepresentation();

        if (userRepresentation.getAttributes() == null) {
            Map<String, List<String>> accountmaps = new HashMap<>();
            accountmaps.put("account", Collections.singletonList(account));
            userRepresentation.setAttributes(accountmaps);
            usersResource.get(userId).update(userRepresentation);

        } else if (!userRepresentation.getAttributes().containsKey("account")) {
            userRepresentation.getAttributes().put("account", Collections.singletonList(account));
            usersResource.get(userId).update(userRepresentation);


        } else {
            if (!userRepresentation.getAttributes().get("account").contains(account)) {
                List<String> accountList = userRepresentation.getAttributes().get("account");
                accountList.add(account);
                userRepresentation.getAttributes().put("account", accountList);
                usersResource.get(userId).update(userRepresentation);
            }
        }
        keycloak.close();


    }


    public void updateUserAccountMargin(String userId, String account) throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        UsersResource usersResource = keycloak.realm(realm).users();
        UserRepresentation userRepresentation = usersResource.get(userId).toRepresentation();

        if (userRepresentation.getAttributes() == null) {
            Map<String, List<String>> accountmaps = new HashMap<>();
            accountmaps.put("marginaccount", Collections.singletonList(account));
            userRepresentation.setAttributes(accountmaps);
            usersResource.get(userId).update(userRepresentation);

        } else if (!userRepresentation.getAttributes().containsKey("marginaccount")) {
            userRepresentation.getAttributes().put("marginaccount", Collections.singletonList(account));
            usersResource.get(userId).update(userRepresentation);


        } else {
            if (!userRepresentation.getAttributes().get("marginaccount").contains(account)) {
                List<String> accountList = userRepresentation.getAttributes().get("marginaccount");
                accountList.add(account);
                userRepresentation.getAttributes().put("marginaccount", accountList);
                usersResource.get(userId).update(userRepresentation);
            }
        }
        keycloak.close();


    }



    public List<GroupRepresentation> getAllGroups() throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        RealmResource realmResource = keycloak.realm(realm);
        GroupsResource groupsResource = realmResource.groups();
        List<GroupRepresentation> x = groupsResource.groups();
        keycloak.close();
        return x;
    }

    public GroupRepresentation getGroupDetails(String groupId) throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        RealmResource realmResource = keycloak.realm(realm);
        GroupResource groupResource = realmResource.groups().group(groupId);
        GroupRepresentation x = groupResource.toRepresentation();
        keycloak.close();
        return x;
    }

    public void createUser(String username, String email, String firstName, String lastName, String password, String password2, String margin, String phone, String groupName, String subgroupName, String account, String codeOperator, String palanca) throws Exception {

        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();

        if (!password.equals(password2)) {
            throw new RuntimeException("The passwords do not match.");
        }
        UserRepresentation newUser = new UserRepresentation();
        newUser.setUsername(username);
        newUser.setEmail(email);
        newUser.setFirstName(firstName);
        newUser.setLastName(lastName);
        newUser.setEnabled(true);
        Map<String, List<String>> attributes = new HashMap<>();
        attributes.put("margin", Collections.singletonList(margin));
        attributes.put("phone", Collections.singletonList(phone));
        attributes.put("account", Collections.singletonList(account));
        attributes.put("codeOperator", Collections.singletonList(codeOperator));
        attributes.put("palanca", Collections.singletonList(palanca));
        newUser.setAttributes(attributes);

        Response response = keycloak.realm(realm).users().create(newUser);
        if (response.getStatus() != 201) {
            log.error("Error al crear el usuario: HTTP {}", response.getStatus());
            response.close();
            throw new RuntimeException("No se pudo crear el usuario en Keycloak.");
        }

        String userId = CreatedResponseUtil.getCreatedId(response);
        response.close();

        CredentialRepresentation credential = new CredentialRepresentation();
        credential.setType(CredentialRepresentation.PASSWORD);
        credential.setValue(password);
        credential.setTemporary(false);
        keycloak.realm(realm).users().get(userId).resetPassword(credential);

        if (groupName != null && !groupName.isEmpty()) {
            GroupRepresentation group = findGroupByName(groupName);
            if (group != null) {
                keycloak.realm(realm).users().get(userId).joinGroup(group.getId());

                if (subgroupName != null && !subgroupName.isEmpty()) {
                    GroupRepresentation subGroup = findSubGroupByName(group, subgroupName);
                    if (subGroup != null) {
                        keycloak.realm(realm).users().get(userId).joinGroup(subGroup.getId());
                    } else {
                        log.warn("Subgrupo no encontrado: {}", subgroupName);
                    }
                }
            } else {
                log.warn("Grupo no encontrado: {}", groupName);
            }
        }

        keycloak.close();
    }

    private GroupRepresentation findSubGroupByName(GroupRepresentation group, String subgroupName) {
        if (group.getSubGroups() != null) {
            for (GroupRepresentation subGroup : group.getSubGroups()) {
                if (subgroupName.equals(subGroup.getName())) {
                    return subGroup;
                }
            }
        }
        return null;
    }

    private GroupRepresentation findGroupByName(String groupName) {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl)
                .realm(this.realm).username(this.adminUsername).password(this.adminPassword)
                .clientId(this.clientId).clientSecret(this.clientSecret).build();
        return keycloak.realm(realm).groups().groups().stream()
                .filter(g -> groupName.equals(g.getName()))
                .findFirst()
                .orElse(null);
    }

    public Boolean toggleUserEnabled(String userId, boolean statusOriginal) throws Exception {

        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        UsersResource usersResource = keycloak.realm(realm).users();
        UserRepresentation user = usersResource.get(userId).toRepresentation();
        user.setEnabled(statusOriginal);

        usersResource.get(userId).update(user);
        keycloak.close();
        return statusOriginal;
    }


    public boolean isAdmin(String username) throws Exception {

        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl)
                .realm(this.realm).username(this.adminUsername).password(this.adminPassword)
                .clientId(this.clientId).clientSecret(this.clientSecret).build();
        UsersResource usersResource = keycloak.realm(realm).users();
        List<UserRepresentation> users = usersResource.search(username);

        if (users.isEmpty()) {
            log.error("User not found: " + username);
            return false;
        }


        UserRepresentation userRepresentation = users.get(0);
        String userId = userRepresentation.getId();


        log.debug("User ID for username " + username + ": " + userId);

        UserResource userResource = usersResource.get(userId);
        List<RoleRepresentation> roles = userResource.roles().realmLevel().listEffective();

        AtomicReference<Boolean> isadmin = new AtomicReference<>(false);

        roles.forEach(s -> {
            if (s.getName().equals("admin_user")) {
                isadmin.set(true);
            }
        });

        return isadmin.get();

    }


    public Boolean getStatus(String userId) throws Exception {
        Keycloak keycloak = KeycloakBuilder.builder().serverUrl(this.keycloakUrl).realm(this.realm).username(this.adminUsername).password(this.adminPassword).clientId(this.clientId).clientSecret(this.clientSecret).build();
        UsersResource usersResource = keycloak.realm(realm).users();
        UserRepresentation user = usersResource.get(userId).toRepresentation();
        keycloak.close();
        return user.isEnabled();
    }

    public Map<String, List<String>> getUserAttributes(String username) throws Exception {
        UserRepresentation user = getUserByUsername(username);
        if (user != null) {
            return user.getAttributes();
        }
        return Collections.emptyMap();
    }


}


