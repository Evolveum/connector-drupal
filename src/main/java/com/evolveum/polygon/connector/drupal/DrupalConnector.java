/**
 * Copyright (c) 2016 Evolveum
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.evolveum.polygon.connector.drupal;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.identityconnectors.common.Base64;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConnectorClass;

import com.evolveum.polygon.rest.AbstractRestConnector;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.*;

/**
 * @author semancik, oscar
 *
 */
@ConnectorClass(displayNameKey = "drupal.connector.display", configurationClass = DrupalConfiguration.class)
public class DrupalConnector extends AbstractRestConnector<DrupalConfiguration> implements PoolableConnector, TestOp, SchemaOp, CreateOp, DeleteOp, UpdateOp, SearchOp<DrupalFilter> {

    private static final Log LOG = Log.getLog(DrupalConnector.class);

    protected static final String ATTR_NAME = "name"; //icfs:name
    private static final String ATTR_PASS = "pass"; // OperationalAttributes.PASSWORD
    private static final String ATTR_STATUS = "status"; // OperationalAttributes.ENABLE
    public static final String ATTR_MAIL = "mail";
    public static final String ATTR_THEME = "theme";
    private static final String ATTR_SIGNATURE = "signature";
    private static final String ATTR_SIGNATURE_FORMAT = "signature_format";
    private static final String ATTR_CREATED = "created";
    private static final String ATTR_ACCESS = "access";
    private static final String ATTR_LOGIN = "login";
    private static final String ATTR_TIMEZONE = "timezone";
    private static final String ATTR_LANGUAGE = "language";
    // TODO avatar?
    public static final String ATTR_ROLES = "roles";

    public static final String STATUS_ENABLED = "1";
    public static final String STATUS_BLOCKED = "0";

    private static final String USER = "/user";
    protected static final String TAXONOMY_TERM = "/taxonomy_term";
    protected static final String NODE = "/node";
    protected static final String FILE = "/file";

    // json strings
    private static final String UND = "und";
    private static final String UID = "uid"; // user ID
    protected static final String TID = "tid"; // taxonomy ID
    protected static final String VID = "vid"; // vocabulary ID
    protected static final String NID = "nid"; // node ID
    protected static final String FID = "fid"; // file ID
    protected static final String VALUE = "value"; // field value
    private static final String CONTENT_TYPE = "application/json";
    protected static final String TRANSFORMED_POSTFIX = "_transformed";

    // taxonomy
    public static final String OC_TERM_Prefix = "term_";
    protected static final String ATTR_TAX_VOCABULARY_MACHINE_NAME = "vocabulary_machine_name";
    private static final String ATTR_TAX_DESCRIPTION = "description";
    private static final String ATTR_TAX_FORMAT = "format";
    public static final String ATTR_TAX_WEIGHT = "weight";
    protected static final String ATTR_TAX_WEIGHT_DEFAULT = "0";
    public static final String ATTR_TAX_PARENT = "parent";

    // node
    public static final String NODE_KEY = "KEY";
    public static final String OC_NODE_Prefix = "node_";
    protected static final String ATTR_NODE_TITLE = "title";
    public static final String ATTR_NODE_STATUS = "status";
    protected static final String ATTR_NODE_TYPE = "type";
    public static final String ATTR_NODE_BODY = "body";
    private static final String ATTR_NODE_CREATED = "created";
    private static final String ATTR_NODE_CHANGED = "changed";

    // file
    private static final String ATTR_FILE_FILENAME = "filename";
    private static final String ATTR_FILE_STATUS = "status";
    private static final String ATTR_FILE_STATUS_DEFAULT = "0";
    private static final String ATTR_FILE_FILE = "file";

    private TaxonomyCache taxonomyCache;

    public NodeCache nodeCache;

    @Override
    public void test() {
        if (getConfiguration().getSkipTestConnection()){
            LOG.ok("test is skipd");
        } else {
            LOG.ok("test - reading admin user");
            try {
                HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + USER + "/1");
                callRequest(request, true);
            } catch (IOException e) {
                throw new ConnectorIOException("Error when testing connection: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void init(Configuration configuration) {
        super.init(configuration);
        LOG.ok("configuration: {0}", ((DrupalConfiguration) this.getConfiguration()).toString());

        getConfiguration().parseMetadatas();

        try {
            taxonomyCache = new TaxonomyCache(this);
            nodeCache = new NodeCache(this);
        } catch (IOException e) {
            throw new ConnectorIOException("Error while initializing taxonomyCache: " + e.getMessage(), e);
        }
    }

    @Override
    public void dispose() {
        super.dispose();
        if (taxonomyCache != null) {
            taxonomyCache.clear();
            taxonomyCache = null;
        }
        if (nodeCache != null) {
            nodeCache.clear();
            nodeCache = null;
        }
    }

    @Override
    public Schema schema() {
        SchemaBuilder schemaBuilder = new SchemaBuilder(DrupalConnector.class);
        buildAccountObjectClass(schemaBuilder);
        buildTaxonomyObjectClasses(schemaBuilder);
        buildNodeObjectClasses(schemaBuilder);
        // TODO: file?
        return schemaBuilder.build();
    }

    private void buildAccountObjectClass(SchemaBuilder schemaBuilder) {
        ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();

        // UID & NAME are defaults

        AttributeInfoBuilder attrMailBuilder = new AttributeInfoBuilder(ATTR_MAIL);
        attrMailBuilder.setRequired(true);
        attrMailBuilder.setReturnedByDefault(false); // TODO: list not returned by default, only user details
        objClassBuilder.addAttributeInfo(attrMailBuilder.build());

        AttributeInfoBuilder attrThemeBuilder = new AttributeInfoBuilder(ATTR_THEME);
        objClassBuilder.addAttributeInfo(attrThemeBuilder.build());
        AttributeInfoBuilder attrSignatureBuilder = new AttributeInfoBuilder(ATTR_SIGNATURE);
        objClassBuilder.addAttributeInfo(attrSignatureBuilder.build());
        AttributeInfoBuilder attrSignatureFormatBuilder = new AttributeInfoBuilder(ATTR_SIGNATURE_FORMAT);
        objClassBuilder.addAttributeInfo(attrSignatureFormatBuilder.build());
        AttributeInfoBuilder attrCreateBuilder = new AttributeInfoBuilder(ATTR_CREATED);
        objClassBuilder.addAttributeInfo(attrCreateBuilder.build());
        AttributeInfoBuilder attrAccessBuilder = new AttributeInfoBuilder(ATTR_ACCESS);
        objClassBuilder.addAttributeInfo(attrAccessBuilder.build());
        AttributeInfoBuilder attrLoginBuilder = new AttributeInfoBuilder(ATTR_LOGIN);
        objClassBuilder.addAttributeInfo(attrLoginBuilder.build());
        AttributeInfoBuilder attrTimezoneBuilder = new AttributeInfoBuilder(ATTR_TIMEZONE);
        objClassBuilder.addAttributeInfo(attrTimezoneBuilder.build());
        AttributeInfoBuilder attrLanguageBuilder = new AttributeInfoBuilder(ATTR_LANGUAGE);
        objClassBuilder.addAttributeInfo(attrLanguageBuilder.build());

        AttributeInfoBuilder attrRolesBuilder = new AttributeInfoBuilder(ATTR_ROLES, String.class);
        attrRolesBuilder.setMultiValued(true);
        attrRolesBuilder.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(attrRolesBuilder.build());

        objClassBuilder.addAttributeInfo(OperationalAttributeInfos.ENABLE);     // status
        // additional fields
        addFields(getConfiguration().getUserMetadatas().keySet(), getConfiguration().getRequiredFieldsList(), objClassBuilder);

        // file fields transformed
        for (String fileField : getConfiguration().getUser2files()) {
            AttributeInfoBuilder attrFieldTransformBuilder = new AttributeInfoBuilder(fileField + TRANSFORMED_POSTFIX);
            attrFieldTransformBuilder.setReturnedByDefault(false);
            objClassBuilder.addAttributeInfo(attrFieldTransformBuilder.build());
        }

        schemaBuilder.defineObjectClass(objClassBuilder.build());
    }

    private void buildTaxonomyObjectClasses(SchemaBuilder schemaBuilder) {

        for (String machineName : getConfiguration().getTaxonomiesMetadatas().keySet()) {
            ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
            objClassBuilder.setType(OC_TERM_Prefix + machineName);

            // UID & NAME are defaults

            AttributeInfoBuilder attrDescriptionBuilder = new AttributeInfoBuilder(ATTR_TAX_DESCRIPTION);
            objClassBuilder.addAttributeInfo(attrDescriptionBuilder.build());
            AttributeInfoBuilder attrFormatBuilder = new AttributeInfoBuilder(ATTR_TAX_FORMAT);
            objClassBuilder.addAttributeInfo(attrFormatBuilder.build());
            AttributeInfoBuilder attrWeightBuilder = new AttributeInfoBuilder(ATTR_TAX_WEIGHT);
            objClassBuilder.addAttributeInfo(attrWeightBuilder.build());
            AttributeInfoBuilder attrParentBuilder = new AttributeInfoBuilder(ATTR_TAX_PARENT);
            objClassBuilder.addAttributeInfo(attrParentBuilder.build());

            // additional fields
            addFields(getConfiguration().getTaxonomiesMetadatas().get(machineName).keySet(), getConfiguration().getRequiredFieldsList(), objClassBuilder);

            schemaBuilder.defineObjectClass(objClassBuilder.build());
        }
    }

    private void buildNodeObjectClasses(SchemaBuilder schemaBuilder) {

        for (String types : getConfiguration().getNodesMetadatas().keySet()) {
            ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
            objClassBuilder.setType(OC_NODE_Prefix + types);

            // UID & NAME (ATTR_NODE_TITLE) are defaults

            AttributeInfoBuilder attrStatusBuilder = new AttributeInfoBuilder(ATTR_NODE_STATUS);
            objClassBuilder.addAttributeInfo(attrStatusBuilder.build());
            AttributeInfoBuilder attrBodyBuilder = new AttributeInfoBuilder(ATTR_NODE_BODY);
            attrBodyBuilder.setReturnedByDefault(false);
            objClassBuilder.addAttributeInfo(attrBodyBuilder.build());
            AttributeInfoBuilder attrCreatedBuilder = new AttributeInfoBuilder(ATTR_NODE_CREATED);
            objClassBuilder.addAttributeInfo(attrCreatedBuilder.build());
            AttributeInfoBuilder attrChangedBuilder = new AttributeInfoBuilder(ATTR_NODE_CHANGED);
            objClassBuilder.addAttributeInfo(attrChangedBuilder.build());

            // additional fields
            addFields(getConfiguration().getNodesMetadatas().get(types).keySet(), getConfiguration().getRequiredFieldsList(), objClassBuilder);

            schemaBuilder.defineObjectClass(objClassBuilder.build());
        }
    }

    private void addFields(Set<String> fields, List<String> requiredField, ObjectClassInfoBuilder objClassBuilder) {
        if (fields != null) {
            for (String field : fields) {
                AttributeInfoBuilder attrFieldBuilder = new AttributeInfoBuilder(field);
                attrFieldBuilder.setReturnedByDefault(false);
                if (requiredField.contains(field)) {
                    attrFieldBuilder.setRequired(true);
                }
                objClassBuilder.addAttributeInfo(attrFieldBuilder.build());

                // need to transform taxonomy
                String machineName = getConfiguration().getUser2taxonomies().get(field);
                if (machineName != null) {
                    AttributeInfoBuilder attrFieldTransformBuilder = new AttributeInfoBuilder(field + TRANSFORMED_POSTFIX);
                    attrFieldTransformBuilder.setReturnedByDefault(false);
                    objClassBuilder.addAttributeInfo(attrFieldTransformBuilder.build());
                }

                // need to transform node
                String type = getConfiguration().getUser2nodes().get(field);
                if (type != null) {
                    AttributeInfoBuilder attrFieldTransformBuilder = new AttributeInfoBuilder(field + TRANSFORMED_POSTFIX);
                    attrFieldTransformBuilder.setReturnedByDefault(false);
                    objClassBuilder.addAttributeInfo(attrFieldTransformBuilder.build());
                }
            }
        }
    }

    @Override
    public Uid create(ObjectClass objectClass, Set<Attribute> attributes, OperationOptions operationOptions) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {    // __ACCOUNT__
            return createOrUpdateUser(null, attributes);
        } else {
            for (String machineName : getConfiguration().getTaxonomiesMetadatas().keySet()) {
                if (objectClass.is(OC_TERM_Prefix + machineName)) {
                    return createOrUpdateTaxonomy(null, attributes, machineName);
                }
            }
            for (String machineName : getConfiguration().getTaxonomiesMetadatas().keySet()) {
                if (objectClass.is(OC_NODE_Prefix + machineName)) {
                    return createOrUpdateNode(null, attributes, machineName);
                }
            }
            // not found
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    protected JSONObject callRequest(HttpEntityEnclosingRequestBase request, JSONObject jo) throws IOException {
        // don't log request here - password field !!!
        LOG.ok("request URI: {0}", request.getURI());
        request.setHeader("Content-Type", CONTENT_TYPE);
        HttpEntity entity = new ByteArrayEntity(jo.toString().getBytes("UTF-8"));
        request.setEntity(entity);
        CloseableHttpResponse response = execute(request);
        LOG.ok("response: {0}", response);
        processDrupalResponseErrors(response);

        String result = EntityUtils.toString(response.getEntity());
        LOG.ok("response body: {0}", result);
        closeResponse(response);
        return new JSONObject(result);
    }

    protected JSONObject callRequest(HttpRequestBase request, boolean parseResult) throws IOException {
        LOG.ok("request URI: {0}", request.getURI());
        request.setHeader("Content-Type", CONTENT_TYPE);
        CloseableHttpResponse response = null;
        response = execute(request);
        LOG.ok("response: {0}", response);
        processDrupalResponseErrors(response);

        if (!parseResult) {
            closeResponse(response);
            return null;
        }
        String result = EntityUtils.toString(response.getEntity());
        LOG.ok("response body: {0}", result);
        closeResponse(response);
        return new JSONObject(result);
    }

    protected JSONArray callRequest(HttpRequestBase request) throws IOException {
        LOG.ok("request URI: {0}", request.getURI());
        request.setHeader("Content-Type", CONTENT_TYPE);
        CloseableHttpResponse response = execute(request);
        LOG.ok("response: {0}", response);
        processDrupalResponseErrors(response);

        String result = EntityUtils.toString(response.getEntity());
        LOG.ok("response body: {0}", result);
        closeResponse(response);
        return new JSONArray(result);
    }

    private void processDrupalResponseErrors(CloseableHttpResponse response){
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 406) {
            String result = null;
            try {
                result = EntityUtils.toString(response.getEntity());
            } catch (IOException e) {
                throw new ConnectorIOException("Error when trying to get response entity: "+response, e);
            }
            if (result.contains("There is no user with ID") || result.contains("There is no term with ID") || result.contains("There is no ")) {
                closeResponse(response);
                throw new UnknownUidException(result);
            }
            JSONObject err;
            try {
                LOG.ok("Result body: {0}", result);
                JSONObject jo = new JSONObject(result);
                err = jo.getJSONObject("form_errors");
            } catch (JSONException e) {
                closeResponse(response);
                throw new ConnectorIOException(e.getMessage() + " when parsing result: " + result, e);
            }
            if (err.has(ATTR_NAME)) {
                closeResponse(response);
                throw new AlreadyExistsException(err.getString(ATTR_NAME)); // The name test_evolveum is already taken.
            } else if (err.has(ATTR_MAIL)) {
                closeResponse(response);
                throw new AlreadyExistsException(err.getString(ATTR_MAIL)); // The e-mail address test@evolveum.com is already taken.
            } else if (err. has(ATTR_MAIL)) { // {"field_pub_team][und":"Team field is required."}
                closeResponse(response);
                throw new AlreadyExistsException(err.getString(ATTR_MAIL)); // The e-mail address test@evolveum.com is already taken.
            } else {
                if (err != null && err.length()>0) {
                    for (String key : err.keySet()){
                        String value = err.getString(key);
                        if (value!=null && value.contains("field is required.")) {
                            closeResponse(response);
                            throw new InvalidAttributeValueException("Missing mandatory attribute " + key+", full message: "+err);
                        }
                    }
                }
                closeResponse(response);
                throw new ConnectorIOException("Error when process response: " + result);
            }
        }
        super.processResponseErrors(response);
    }

    private Uid createOrUpdateUser(Uid uid, Set<Attribute> attributes) {
        LOG.ok("createOrUpdateUser, Uid: {0}, attributes: {1}", uid, attributes);
        if (attributes == null || attributes.isEmpty()) {
            LOG.ok("request ignored, empty attributes");
            return uid;
        }
        boolean create = uid == null;
        JSONObject jo = new JSONObject();
        String mail = getStringAttr(attributes, ATTR_MAIL);
        if (create && StringUtil.isBlank(mail)) {
            throw new InvalidAttributeValueException("Missing mandatory attribute " + ATTR_MAIL);
        }
        if (mail != null) {
            jo.put(ATTR_MAIL, mail);
        }

        String name = getStringAttr(attributes, Name.NAME);
        if (create && StringUtil.isBlank(name)) {
            throw new InvalidAttributeValueException("Missing mandatory attribute " + Name.NAME);
        }
        if (name != null) {
            jo.put(ATTR_NAME, name);
        }

        final List<String> passwordList = new ArrayList<String>(1);
        GuardedString guardedPassword = getAttr(attributes, OperationalAttributeInfos.PASSWORD.getName(), GuardedString.class);
        if (guardedPassword != null) {
            guardedPassword.access(new GuardedString.Accessor() {
                @Override
                public void access(char[] chars) {
                    passwordList.add(new String(chars));
                }
            });
        }
        String password = null;
        if (!passwordList.isEmpty()) {
            password = passwordList.get(0);
        }

        handleEnable(attributes, jo);

        putFieldIfExists(attributes, ATTR_THEME, jo);
        putFieldIfExists(attributes, ATTR_SIGNATURE, jo);
        putFieldIfExists(attributes, ATTR_SIGNATURE_FORMAT, jo);
        putFieldIfExists(attributes, ATTR_CREATED, jo);
        putFieldIfExists(attributes, ATTR_ACCESS, jo);
        putFieldIfExists(attributes, ATTR_LOGIN, jo);
        putFieldIfExists(attributes, ATTR_TIMEZONE, jo);
        putFieldIfExists(attributes, ATTR_LANGUAGE, jo);

        putRolesIfExists(attributes, jo);

        for (Map.Entry<String, String> entry : getConfiguration().getUserMetadatas().entrySet()) {
            boolean singleValueField = false;
            if (getConfiguration().getSingleValueUserFields().contains(entry.getKey())) {
                singleValueField = true;
            }
            putUndFieldIfExists(attributes, entry.getKey(), jo, entry.getValue(), singleValueField, create);
        }

        LOG.ok("user request (without password): {0}", jo.toString());

        if (password != null) {
            jo.put(ATTR_PASS, password);
        }

        try {
            handleFiles(attributes, jo, uid, name);

            HttpEntityEnclosingRequestBase request;
            if (create) {
                request = new HttpPost(getConfiguration().getServiceAddress() + USER);
            } else {
                // update
                request = new HttpPut(getConfiguration().getServiceAddress() + USER + "/" + uid.getUidValue());
            }
            JSONObject jores = callRequest(request, jo);

            String newUid = jores.getString(UID);
            LOG.info("response UID: {0}", newUid);
            return new Uid(newUid);
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private void handleFiles(Set<Attribute> attributes, JSONObject jo, Uid uid, String userName) throws IOException {
        for (String fileFieldName : getConfiguration().getUser2files()) {
            byte[] fileContent = getAttr(attributes, fileFieldName + TRANSFORMED_POSTFIX, byte[].class);
            if (fileContent != null && fileContent.length > 0) {
                String newFid = null; // don't need update
                // update user
                if (uid != null) {
                    // read & compare with existing file
                    LOG.ok("need to update user file for field {0}, reading existing & comparing for UID: {1} ", fileFieldName, uid.getUidValue());
                    HttpGet requestUserDetail = new HttpGet(getConfiguration().getServiceAddress() + USER + "/" + uid.getUidValue());
                    JSONObject user = callRequest(requestUserDetail, true);
                    String fid = getFidValue(user, fileFieldName);
                    if (userName == null) {
                        userName = user.getString(ATTR_NAME);
                    }

                    if (fid != null) {
                        // existing user has file now

                        HttpGet requestFileDetail = new HttpGet(getConfiguration().getServiceAddress() + FILE + "/" + fid);
                        JSONObject file = callRequest(requestFileDetail, true);

                        String fileFromResource = file.getString(ATTR_FILE_FILE);
                        String fileFromMidPoint = Base64.encode(fileContent);
                        if (!fileFromMidPoint.equals(fileFromResource)) {
                            // need to create new file (old file is unlinked and deleted when no reference over drupal cron)
                            newFid = createFile(fileContent, userName);
                        }
                        // else we have already the same file content in resource, update is ignored
                    } else {
                        // existing user don't has file now
                        newFid = createFile(fileContent, userName);

                    }
                } else if (uid == null) {
                    // create user
                    newFid = createFile(fileContent, userName);
                }

                if (newFid != null) {
                    // put new file reference to JSON

                    JSONArray undArray = new JSONArray();
                    JSONObject value = new JSONObject();
                    value.put(FID, newFid);
                    undArray.put(value);
                    JSONObject und = new JSONObject();
                    und.put(UND, undArray);
                    jo.put(fileFieldName, und);
                }
            }
        }
    }

    private String createFile(byte[] fileContent, String fileName) throws IOException {
        // determine image type
        String extension = "jpg";
        try {
            ByteArrayInputStream is = new ByteArrayInputStream(fileContent);
            ImageInputStream iis = ImageIO.createImageInputStream(is);

            Iterator<ImageReader> imageReaders = ImageIO.getImageReaders(iis);

            while (imageReaders.hasNext()) {
                ImageReader reader = (ImageReader) imageReaders.next();
                extension = reader.getFormatName();
            }
        } catch (IOException e) {
            throw new ConnectorIOException("not parseable image extension (JPEG/PNG/...): " + e.getMessage(), e);
        }

        JSONObject jo = new JSONObject();
        jo.put(ATTR_FILE_STATUS, ATTR_FILE_STATUS_DEFAULT);
        jo.put(ATTR_FILE_FILE, Base64.encode(fileContent));
        jo.put(ATTR_FILE_FILENAME, fileName + "." + extension);


        HttpPost request = new HttpPost(getConfiguration().getServiceAddress() + FILE);
        JSONObject file = callRequest(request, jo);

        return file.getString(FID);
    }

    private String getFidValue(JSONObject user, String fileField) {
        if (user.has(fileField) && (user.opt(fileField) instanceof JSONObject)) {
            JSONArray und = user.getJSONObject(fileField).getJSONArray(UND);
            if (und.length() > 0) {
                if (und.getJSONObject(0).has(FID)) {
                    String fid = und.getJSONObject(0).getString(FID);

                    return fid;
                }
            }
        }
        // TODO: support build in picture field?

        return null;
    }

    private void handleEnable(Set<Attribute> attributes, JSONObject jo) {
        Boolean enable = getAttr(attributes, OperationalAttributes.ENABLE_NAME, Boolean.class);

        if (enable != null) {
            jo.put(ATTR_STATUS, enable ? STATUS_ENABLED : STATUS_BLOCKED);
        }
    }

    private void putRolesIfExists(Set<Attribute> attributes, JSONObject jo) {
        String[] roles = getMultiValAttr(attributes, ATTR_ROLES, null);

        if (roles != null) {
            // "roles":{"2":"2", "3":"3" }
            JSONObject joRoles = new JSONObject();
            for (String role : roles) {
                joRoles.put(role, role);
            }
            jo.put(ATTR_ROLES, joRoles);
        }
    }

    private Uid createOrUpdateTaxonomy(Uid uid, Set<Attribute> attributes, String machineName) {
        LOG.ok("createOrUpdateTaxonomy, Uid: {0}, machine_name: {1} attributes: {2}", uid, machineName, attributes);
        if (attributes == null || attributes.isEmpty()) {
            LOG.ok("request ignored, empty attributes");
            return uid;
        }
        // FIXME: check machineName when update
        boolean create = uid == null;
        JSONObject jo = new JSONObject();
        String name = getStringAttr(attributes, Name.NAME);
        if (create && StringUtil.isBlank(name)) {
            throw new InvalidAttributeValueException("Missing mandatory attribute " + Name.NAME);
        }
        if (create) {
            // check existence with the same name
            try {
                HttpGet requestFind = new HttpGet(getConfiguration().getServiceAddress() + TAXONOMY_TERM +
                        "?parameters[" + VID + "]=" + getConfiguration().getTaxonomiesKeys().get(machineName) + "&parameters[" + ATTR_NAME + "]=" + URLEncoder.encode(name, "UTF-8"));
                JSONArray entities = callRequest(requestFind);
                if (entities.length() > 0) {
                    throw new AlreadyExistsException("term with name '" + name + "' and machine name '" + machineName + "' already exists: " + entities);
                }
            } catch (IOException e) {
                throw new ConnectorIOException(e.getMessage(), e);
            }
        }

        String weight = getStringAttr(attributes, ATTR_TAX_WEIGHT);
        if (create && StringUtil.isBlank(weight)) {
            throw new InvalidAttributeValueException("Missing mandatory attribute " + ATTR_TAX_WEIGHT + " please use at least default = '0'");
        }
        if (name != null) {
            jo.put(ATTR_NAME, name);
        }
        if (weight != null) {
            jo.put(ATTR_TAX_WEIGHT, weight);
        }
        putFieldValueIfExists(attributes, ATTR_TAX_DESCRIPTION, jo); // "description": {"value": "created by midPoint"},
        putFieldIfExists(attributes, ATTR_TAX_FORMAT, jo);
        putArrayIfExists(attributes, ATTR_TAX_PARENT, jo);

        for (Map.Entry<String, String> entry : getConfiguration().getTaxonomiesMetadatas().get(machineName).entrySet()) {
            putUndFieldIfExists(attributes, entry.getKey(), jo, entry.getValue(), false, create);
        }

        LOG.ok("request body: {0}", jo.toString());

        try {
            HttpEntityEnclosingRequestBase request;
            if (create) {
                jo.put(ATTR_TAX_VOCABULARY_MACHINE_NAME, machineName);
                request = new HttpPost(getConfiguration().getServiceAddress() + TAXONOMY_TERM);
            } else {
                // update
                request = new HttpPut(getConfiguration().getServiceAddress() + TAXONOMY_TERM + "/" + uid.getUidValue());
            }
            JSONObject jores = callRequest(request, jo);
            String tid = jores.getString(TID);
            LOG.info("response TID: {0}", tid);
            return new Uid(tid);
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private Uid createOrUpdateNode(Uid uid, Set<Attribute> attributes, String type) {
        LOG.ok("createOrUpdateNode, Uid: {0}, type: {1} attributes: {2}", uid, type, attributes);
        if (attributes == null || attributes.isEmpty()) {
            LOG.ok("request ignored, empty attributes");
            return uid;
        }
        // FIXME: check type when update

        boolean create = uid == null;
        JSONObject jo = new JSONObject();
        String name = getStringAttr(attributes, Name.NAME);
        if (name != null) {
            jo.put(ATTR_NODE_TITLE, name);
        }

        putFieldIfExists(attributes, ATTR_NODE_STATUS, jo);
        putUndFieldIfExists(attributes, ATTR_NODE_BODY, jo, VALUE, false, create);
        putFieldIfExists(attributes, ATTR_NODE_CREATED, jo);
        putFieldIfExists(attributes, ATTR_NODE_CHANGED, jo);

        for (Map.Entry<String, String> entry : getConfiguration().getNodesMetadatas().get(type).entrySet()) {
            putUndFieldIfExists(attributes, entry.getKey(), jo, entry.getValue(), false, create);
        }

        LOG.ok("request body: {0}", jo.toString());

        try {
            HttpEntityEnclosingRequestBase request;
            if (create) {
                jo.put(ATTR_NODE_TYPE, type);
                request = new HttpPost(getConfiguration().getServiceAddress() + NODE);
            } else {
                //update
                request = new HttpPut(getConfiguration().getServiceAddress() + NODE + "/" + uid.getUidValue());
            }
            JSONObject jores = callRequest(request, jo);
            String nid = jores.getString(NID);
            LOG.info("response NID: {0}", nid);
            return new Uid(nid);
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private void putFieldIfExists(Set<Attribute> attributes, String fieldName, JSONObject jo) {
        String fieldValue = getStringAttr(attributes, fieldName);
        if (fieldValue != null) {
            jo.put(fieldName, fieldValue);
        } else if (getConfiguration().getRequiredFieldsList().contains(fieldName)) {
            throw new InvalidAttributeValueException("Missing mandatory attribute " + fieldName);
        }
    }

    private void putFieldValueIfExists(Set<Attribute> attributes, String fieldName, JSONObject jo) {
        String value = getStringAttr(attributes, fieldName);
        if (value != null) {
//            "description": {"value": "created by midPoint"}
            JSONObject joValue = new JSONObject();
            joValue.put(VALUE, value);
            jo.put(fieldName, joValue);
        }
    }


    private void putArrayIfExists(Set<Attribute> attributes, String attributeName, JSONObject jo) {
        String parent = getStringAttr(attributes, attributeName);
        if (parent != null) {
            JSONArray pa = new JSONArray();
            pa.put(parent);
            jo.put(attributeName, pa);
        }

    }

    private void putUndFieldIfExists(Set<Attribute> attributes, String fieldName, JSONObject jo, String subFieldName, boolean singleValueField, boolean create) {
        String fieldValue = getStringAttr(attributes, fieldName);
        String tranformedFieldValue = null;
        if (fieldValue == null && getConfiguration().getUser2taxonomies().containsKey(fieldName)) {
            // try to get value from transformed attribute
            tranformedFieldValue = getStringAttr(attributes, fieldName + TRANSFORMED_POSTFIX);
            if (tranformedFieldValue != null) {
                fieldValue = tranformedFieldValue;
            }
        } else if (fieldValue == null && getConfiguration().getUser2nodes().containsKey(fieldName)) {
            // try to get value from transformed attribute
            tranformedFieldValue = getStringAttr(attributes, fieldName + TRANSFORMED_POSTFIX);
            if (tranformedFieldValue != null) {
                fieldValue = tranformedFieldValue;
            }
        }
        if (fieldValue != null) {
            // transformed field value, need also to convert his value to ID
            if (tranformedFieldValue != null) {
                // taxonomy
                String machineName = getConfiguration().getUser2taxonomies().get(fieldName);
                if (machineName != null) {
                    fieldValue = taxonomyCache.getIdOrCreate(machineName, fieldValue);
                }
                // node
                String type = getConfiguration().getUser2nodes().get(fieldName);
                if (type != null) {
                    fieldValue = nodeCache.getIdOrCreate(type, fieldValue);
                }
            }

            JSONArray undArray = new JSONArray();
            // when creating this way (object):
            // "field_pub_department":{"und":[ { "tid":"298" } ]
            // when updating whis way (array):
            // "field_pub_department":{"und":[ "298" ]
            if (create && singleValueField || !create && (NID.equals(subFieldName) || (TID.equals(subFieldName) && singleValueField) /*|| FID.equals(subFieldName)*/ || UID.equals(subFieldName))) {
                subFieldName = null;
            }
            if (subFieldName != null) {
                // "field_department":{"und":[{"nid":"123"}]}
                JSONObject value = new JSONObject();
                value.put(subFieldName, fieldValue);
                undArray.put(value);
            } else {
                // ""field_department":{"und":["1314"]
                undArray.put(fieldValue);
            }
            JSONObject und = new JSONObject();
            und.put(UND, undArray);
            jo.put(fieldName, und);
        } else if (create && getConfiguration().getRequiredFieldsList().contains(fieldName)) {
            throw new InvalidAttributeValueException("Missing mandatory attribute " + fieldName);
        }
    }

    @Override
    public void checkAlive() {
        test();
        // TODO quicker test?
    }

    @Override
    public void delete(ObjectClass objectClass, Uid uid, OperationOptions operationOptions) {
        try {
            if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
                if (getConfiguration().getUserDeleteDisabled()) {
                    LOG.ok("disable user instead of delete, Uid: {0}", uid);
                    JSONObject jo = new JSONObject();
                    jo.put(ATTR_STATUS, STATUS_BLOCKED);

                    HttpPut request = new HttpPut(getConfiguration().getServiceAddress() + USER + "/" + uid.getUidValue());
                    callRequest(request, jo);
                } else {
                    LOG.ok("delete user, Uid: {0}", uid);
                    HttpDelete request = new HttpDelete(getConfiguration().getServiceAddress() + USER + "/" + uid.getUidValue());
                    callRequest(request, false);
                }
            } else {
                for (String machineName : getConfiguration().getTaxonomiesMetadatas().keySet()) {
                    if (objectClass.is(OC_TERM_Prefix + machineName)) {
                        LOG.ok("delete taxonomy {0}, Uid: {1}", machineName, uid);
                        HttpDelete request = new HttpDelete(getConfiguration().getServiceAddress() + TAXONOMY_TERM + "/" + uid.getUidValue());
                        callRequest(request, false);
                        return; // found
                    }
                }
                for (String type : getConfiguration().getNodesMetadatas().keySet()) {
                    if (objectClass.is(OC_NODE_Prefix + type)) {
                        LOG.ok("delete node {0}, Uid: {1}", type, uid);
                        HttpDelete request = new HttpDelete(getConfiguration().getServiceAddress() + NODE + "/" + uid.getUidValue());
                        callRequest(request, false);
                        return; // found
                    }
                }
                // not found
                throw new UnsupportedOperationException("Unsupported object class " + objectClass);
            }
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    @Override
    public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> attributes, OperationOptions operationOptions) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            return createOrUpdateUser(uid, attributes);
        } else {
            for (String machineName : getConfiguration().getTaxonomiesMetadatas().keySet()) {
                if (objectClass.is(OC_TERM_Prefix + machineName)) {
                    return createOrUpdateTaxonomy(uid, attributes, machineName);
                }
            }
            for (String type : getConfiguration().getNodesMetadatas().keySet()) {
                if (objectClass.is(OC_NODE_Prefix + type)) {
                    return createOrUpdateNode(uid, attributes, type);
                }
            }
            // not found
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }


    @Override
    public FilterTranslator<DrupalFilter> createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {
        return new DrupalFilterTranslator();
    }

    public void executeQuery(ObjectClass objectClass, DrupalFilter query, ResultsHandler handler, OperationOptions options) {
        try {
            LOG.info("executeQuery on {0}, query: {1}, options: {2}", objectClass, query, options);
            if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
                //find by Uid (user Primary Key)
                if (query != null && query.byUid != null) {
                    HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + USER + "/" + query.byUid);
                    JSONObject user = callRequest(request, true);
                    ConnectorObject connectorObject = convertUserToConnectorObject(user);
                    handler.handle(connectorObject);
                }// find by name
                else if (query != null && query.byName != null) {
                    HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + USER + "?parameters[" + ATTR_NAME + "]=" + URLEncoder.encode(query.byName, "UTF-8"));
                    handleUsers(request, handler, options, false);

                    //find by emailAddress
                } else if (query != null && query.byEmailAddress != null) {
                    HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + USER + "?parameters[" + ATTR_MAIL + "]=" + query.byEmailAddress);
                    handleUsers(request, handler, options, false);

                } else {
                    // find required page
                    String pageing = processPageOptions(options);
                    if (!StringUtil.isEmpty(pageing)) {
                        HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + USER + "?" + pageing);
                        handleUsers(request, handler, options, false);
                    }
                    // find all
                    else {
                        int pageSize = getConfiguration().getPageSize();
                        int page = 0;
                        while (true) {
                            pageing = processPaging(page, pageSize);
                            HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + USER + "?" + pageing);
                            boolean finish = handleUsers(request, handler, options, true);
                            if (finish) {
                                break;
                            }
                            page++;
                        }
                    }
                }

            } else {
                for (String machineName : getConfiguration().getTaxonomiesMetadatas().keySet()) {
                    if (objectClass.is(OC_TERM_Prefix + machineName)) {
                        //find by Tid (taxonomy Primary Key)
                        if (query != null && query.byUid != null) {
                            HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + TAXONOMY_TERM + "/" + query.byUid);
                            JSONObject taxonomy = null;
                            try {
                                taxonomy = callRequest(request, true);
                            } catch (ConnectorException ce){
                                if (getConfiguration().getHandle500asUnknownUidException() && ce.getMessage().contains("HTTP error 500 Internal Server Error"))
                                {
                                    LOG.warn(ce, "probably already deleted TID, returning UnknownUidException..." + ce);
                                    throw new UnknownUidException("Request to find taxonomy term with ID: "+query.byUid+", but got 500 and changing to UnknownUidException is enabled (workaround for bug in Drupal)");
                                }
                                else {
                                    throw ce;
                                }
                            } catch (IOException ioe){
                                throw new ConnectorIOException(ioe.getMessage(), ioe);
                            }

                            String machineNameFromResource = taxonomy.getString(ATTR_TAX_VOCABULARY_MACHINE_NAME);
                            if (!machineName.equals(machineNameFromResource)) {
                                throw new InvalidAttributeValueException("Expected " + machineName + ", but get " + machineNameFromResource + " for TID:" + query.byUid);
                            }
                            ConnectorObject connectorObject = convertTaxonomyToConnectorObject(taxonomy, machineName);
                            handler.handle(connectorObject);
                        }// find by name
                        else if (query != null && query.byName != null) {
                            HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + TAXONOMY_TERM +
                                    "?parameters[" + VID + "]=" + getConfiguration().getTaxonomiesKeys().get(machineName) + "&parameters[" + ATTR_NAME + "]=" + URLEncoder.encode(query.byName, "UTF-8"));
                            handleTaxonomies(request, machineName, handler, options);
                        } else {
                            // find required page
                            String pageing = processPageOptions(options);
                            if (!StringUtil.isEmpty(pageing)) {
                                HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + TAXONOMY_TERM + "?parameters[" + VID + "]=" + getConfiguration().getTaxonomiesKeys().get(machineName) + pageing);
                                handleTaxonomies(request, machineName, handler, options);
                            }
                            // find all
                            else {
                                int pageSize = getConfiguration().getPageSize();
                                int page = 0;
                                while (true) {
                                    pageing = processPaging(page, pageSize);
                                    HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + TAXONOMY_TERM + "?parameters[" + VID + "]=" + getConfiguration().getTaxonomiesKeys().get(machineName) + pageing);
                                    boolean finish = handleTaxonomies(request, machineName, handler, options);
                                    if (finish) {
                                        break;
                                    }
                                    page++;
                                }
                            }

                        }
                        return; // found OC
                    }
                }
                for (String type : getConfiguration().getNodesMetadatas().keySet()) {
                    if (objectClass.is(OC_NODE_Prefix + type)) {
                        //find by Nid (node Primary Key)
                        if (query != null && query.byUid != null) {
                            HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + NODE + "/" + query.byUid);
                            JSONObject node = callRequest(request, true);
                            String typeFromResource = node.getString(ATTR_NODE_TYPE);
                            if (!type.equals(typeFromResource)) {
                                throw new InvalidAttributeValueException("Expected " + type + ", but get " + typeFromResource + " for NID:" + query.byUid);
                            }
                            ConnectorObject connectorObject = convertNodeToConnectorObject(node, type);
                            handler.handle(connectorObject);
                        }// find by name
                        else if (query != null && query.byName != null) {
                            HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + NODE +
                                    "?parameters[" + ATTR_NODE_TYPE + "]=" + type + "&parameters[" + ATTR_NODE_TITLE + "]=" + URLEncoder.encode(query.byName, "UTF-8"));
                            handleNodes(request, type, handler, options);
                            // find all
                        } else {
                            // find required page
                            String pageing = processPageOptions(options);
                            if (!StringUtil.isEmpty(pageing)) {
                                HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + NODE + "?parameters[" + ATTR_NODE_TYPE + "]=" + type + pageing);
                                handleNodes(request, type, handler, options);
                            }
                            // find all
                            else {
                                int pageSize = getConfiguration().getPageSize();
                                int page = 0;
                                while (true) {
                                    pageing = processPaging(page, pageSize);
                                    HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + NODE + "?parameters[" + ATTR_NODE_TYPE + "]=" + type + pageing);
                                    boolean finish = handleNodes(request, type, handler, options);
                                    if (finish) {
                                        break;
                                    }
                                    page++;
                                }
                            }

                        }
                        return; // found OC
                    }
                }
                // not found
                throw new UnsupportedOperationException("Unsupported object class " + objectClass);
            }
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private boolean handleUsers(HttpGet request, ResultsHandler handler, OperationOptions options, boolean findAll) throws IOException {
        JSONArray users = callRequest(request);
        LOG.ok("Number of users: {0}, pageResultsOffset: {1}, pageSize: {2} ", users.length(), options == null ? "null" : options.getPagedResultsOffset(), options == null ? "null" : options.getPageSize());

        for (int i = 0; i < users.length(); i++) {
            if (i % 10 == 0) {
                LOG.ok("executeQuery: processing {0}. of {1} users", i, users.length());
            }
            // only basic fields
            JSONObject user = users.getJSONObject(i);
            if (this.getConfiguration().getDontReadUserDetailsWhenFindAllUsers() && findAll){
                if (i % user.length() == 0) {
                    LOG.ok("DontReadUserDetailsWhenFindAllUsers property is enabled and finnAll is catched - ignoring reading user details");
                }
            }
            else if (getConfiguration().getUserMetadatas().size() > 1) {
                // when using extended fields we need to get it each by one
                HttpGet requestUserDetail = new HttpGet(getConfiguration().getServiceAddress() + USER + "/" + user.getString(UID));
                user = callRequest(requestUserDetail, true);
            }

            ConnectorObject connectorObject = convertUserToConnectorObject(user);
            boolean finish = !handler.handle(connectorObject);
            if (finish) {
                return true;
            }
        }

        // last page exceed
        if (getConfiguration().getPageSize() > users.length()) {
            return true;
        }
        // need next page
        return false;
    }

    private ConnectorObject convertUserToConnectorObject(JSONObject user) throws IOException {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
        builder.setUid(new Uid(user.getString(UID)));
        if (user.has(ATTR_NAME)) {
            builder.setName(user.getString(ATTR_NAME));
        }
        getIfExists(user, ATTR_MAIL, builder);
        getIfExists(user, ATTR_THEME, builder);
        getIfExists(user, ATTR_SIGNATURE, builder);
        getIfExists(user, ATTR_SIGNATURE_FORMAT, builder);
        getIfExists(user, ATTR_CREATED, builder);
        getIfExists(user, ATTR_ACCESS, builder);
        getIfExists(user, ATTR_LOGIN, builder);
        getIfExists(user, ATTR_TIMEZONE, builder);
        getIfExists(user, ATTR_LANGUAGE, builder);

        if (user.has(ATTR_STATUS)) {
            boolean enable = STATUS_ENABLED.equals(user.getString(ATTR_STATUS)) ? true : false;
            addAttr(builder, OperationalAttributes.ENABLE_NAME, enable);
        }

        if (user.has(ATTR_ROLES)) {
            JSONObject roles = user.getJSONObject(ATTR_ROLES);
            String[] roleArray = roles.keySet().toArray(new String[roles.keySet().size()]);
            builder.addAttribute(ATTR_ROLES, roleArray);
        }

        for (Map.Entry<String, String> entry : getConfiguration().getUserMetadatas().entrySet()) {
            getUndFieldIfExists(user, entry.getKey(), builder, entry.getValue());
        }

        // read files (avatar) if is needed & exists
        Map<String, byte[]> files = new HashMap<>();
        for (String fileField : getConfiguration().getUser2files()) {
            String fid = getFidValue(user, fileField);
            if (fid != null) {
                HttpGet requestUserDetail = new HttpGet(getConfiguration().getServiceAddress() + FILE + "/" + fid);
                JSONObject file = callRequest(requestUserDetail, true);

                builder.addAttribute(fileField + TRANSFORMED_POSTFIX, Base64.decode(file.getString(ATTR_FILE_FILE)));
            }
        }

        ConnectorObject connectorObject = builder.build();
        LOG.ok("convertUserToConnectorObject, user: {0}, \n\tconnectorObject: {1}",
                user.getString(UID), connectorObject);
        return connectorObject;
    }

    private boolean handleTaxonomies(HttpGet request, String machineName, ResultsHandler handler, OperationOptions options) throws IOException {
        JSONArray taxonomies = callRequest(request);
        LOG.ok("Number of taxonomies: {0}, pageResultsOffset: {1}, pageSize: {2} ", taxonomies.length(), options == null ? "null" : options.getPagedResultsOffset(), options == null ? "null" : options.getPageSize());

        for (int i = 0; i < taxonomies.length(); i++) {
            if (i % 10 == 0) {
                LOG.ok("executeQuery: processing {0}. of {1} users", i, taxonomies.length());
            }
            // only basic fields
            JSONObject taxonomy = taxonomies.getJSONObject(i);
            String machineVidFromResource = taxonomy.getString(VID);
            if (!getConfiguration().getTaxonomiesKeys().get(machineName).equals(machineVidFromResource)) {
                throw new InvalidAttributeValueException("Expected taxonomy machine name" + machineName + " (" + getConfiguration().getTaxonomiesKeys().get(machineName) + ")" + ", but get " + machineVidFromResource);
            }

            if (getConfiguration().getTaxonomiesMetadatas().get(machineName).size() > 0) {
                // with advanced fields we need to get it each one
                HttpGet requestDetail = new HttpGet(getConfiguration().getServiceAddress() + TAXONOMY_TERM + "/" + taxonomy.getString(TID));
                taxonomy = callRequest(requestDetail, true);
            }

            ConnectorObject connectorObject = convertTaxonomyToConnectorObject(taxonomy, machineName);
            boolean finish = !handler.handle(connectorObject);
            if (finish) {
                return true;
            }
        }

        // last page exceed
        if (getConfiguration().getPageSize() > taxonomies.length()) {
            return true;
        }
        // need next page
        return false;
    }

    private ConnectorObject convertTaxonomyToConnectorObject(JSONObject taxonomy, String machineName) {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
        ObjectClass objectClass = new ObjectClass(OC_TERM_Prefix + machineName);
        builder.setObjectClass(objectClass);
        builder.setUid(new Uid(taxonomy.getString(TID)));
        if (taxonomy.has(ATTR_NAME)) {
            builder.setName(taxonomy.getString(ATTR_NAME));
        }

        getIfExists(taxonomy, ATTR_TAX_DESCRIPTION, builder);
        getIfExists(taxonomy, ATTR_TAX_FORMAT, builder);
        getIfExists(taxonomy, ATTR_TAX_WEIGHT, builder);
        getMultiIfExists(taxonomy, ATTR_TAX_PARENT, builder);

        for (Map.Entry<String, String> entry : getConfiguration().getTaxonomiesMetadatas().get(machineName).entrySet()) {
            getUndFieldIfExists(taxonomy, entry.getKey(), builder, entry.getValue());
        }

        ConnectorObject connectorObject = builder.build();
        LOG.ok("convertTaxonomyToConnectorObject, taxonomy term: {0}, \n\tconnectorObject: {1}",
                taxonomy.getString(TID), connectorObject);
        return connectorObject;
    }

    private boolean handleNodes(HttpGet request, String type, ResultsHandler handler, OperationOptions options) throws IOException {
        JSONArray nodes = callRequest(request);
        LOG.ok("Number of nodes: {0}, pageResultsOffset: {1}, pageSize: {2} ", nodes.length(), options == null ? "null" : options.getPagedResultsOffset(), options == null ? "null" : options.getPageSize());

        for (int i = 0; i < nodes.length(); i++) {
            if (i % 10 == 0) {
                LOG.ok("executeQuery: processing {0}. of {1} nodes", i, nodes.length());
            }
            JSONObject node = nodes.getJSONObject(i);
            String typeFromResource = node.getString(ATTR_NODE_TYPE);
            if (!type.equals(typeFromResource)) {
                throw new InvalidAttributeValueException("Expected node type " + type + ", but get " + typeFromResource);
            }

            if (getConfiguration().getNodesMetadatas().get(type).size() > 0) {
                // with advanced fields we need to get it each one
                HttpGet requestDetail = new HttpGet(getConfiguration().getServiceAddress() + NODE + "/" + node.getString(NID));
                node = callRequest(requestDetail, true);
            }

            ConnectorObject connectorObject = convertNodeToConnectorObject(node, type);
            boolean finish = !handler.handle(connectorObject);
            if (finish) {
                return true;
            }
        }

        // last page exceed
        if (getConfiguration().getPageSize() > nodes.length()) {
            return true;
        }
        // need next page
        return false;
    }

    private ConnectorObject convertNodeToConnectorObject(JSONObject node, String type) {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
        ObjectClass objectClass = new ObjectClass(OC_NODE_Prefix + type);
        builder.setObjectClass(objectClass);
        builder.setUid(new Uid(node.getString(NID)));
        builder.setName(node.has(ATTR_NODE_TITLE) ? node.getString(ATTR_NODE_TITLE) : ""); // must contains

        getIfExists(node, ATTR_NODE_STATUS, builder);
        getIfExists(node, ATTR_NODE_CREATED, builder);
        getIfExists(node, ATTR_NODE_CHANGED, builder);
        getUndFieldIfExists(node, ATTR_NODE_BODY, builder, VALUE);

        for (Map.Entry<String, String> entry : getConfiguration().getNodesMetadatas().get(type).entrySet()) {
            getUndFieldIfExists(node, entry.getKey(), builder, entry.getValue());
        }

        ConnectorObject connectorObject = builder.build();
        LOG.ok("convertNodeToConnectorObject, node: {0}, \n\tconnectorObject: {1}",
                node.getString(NID), connectorObject);
        return connectorObject;
    }

    private String processPageOptions(OperationOptions options) {
        if (options != null) {
            Integer pageSize = options.getPageSize();
            Integer pagedResultsOffset = options.getPagedResultsOffset();
            if (pageSize != null && pagedResultsOffset != null) {

                return processPaging(pagedResultsOffset, pageSize);
            }
        }
        return "";
    }

    public String processPaging(int page, int pageSize) {
        StringBuilder queryBuilder = new StringBuilder();
        LOG.ok("creating paging with page: {0}, pageSize: {1}", page, pageSize);
        queryBuilder.append("&page=").append(page).append("&").append("pagesize=")
                .append(pageSize);

        return queryBuilder.toString();
    }

    private void getUndFieldIfExists(JSONObject object, String field, ConnectorObjectBuilder builder, String subFieldName) {
        if (object.has(field) && (object.opt(field) instanceof JSONObject)) {

//            System.out.println("object.get(field) = " + object.opt(field).getClass());
//            System.out.println("object.getJSONArray(field) = " + object.getJSONArray(field));
            //field_key_roles":[],"field_skype":[],"field_user_location":{"und":[{"tid":"179"}]}
            JSONArray und = object.getJSONObject(field).getJSONArray(UND);
            if (und.length() > 0) {
                if (und.getJSONObject(0).has(subFieldName)) {
                    String value = und.getJSONObject(0).getString(subFieldName);
                    addAttr(builder, field, value);
                    transformKeyToValue(builder, field, value, subFieldName);
                }
            } else {
                // send null
                addAttr(builder, field, null);
            }
        }
    }

    private void transformKeyToValue(ConnectorObjectBuilder builder, String fieldName, String value, String subFieldName) {
        // taxonomy
        String machineName = getConfiguration().getUser2taxonomies().get(fieldName);
        if (machineName != null) { // need to transform
            String transformedValue = taxonomyCache.getName(machineName, value);
            addAttr(builder, fieldName + TRANSFORMED_POSTFIX, transformedValue);
        }
        // node
        String type = getConfiguration().getUser2nodes().get(fieldName);
        if (type != null) { // need to transform
            String transformedValue = nodeCache.getName(type, value);
            addAttr(builder, fieldName + TRANSFORMED_POSTFIX, transformedValue);
        }
    }

    private void getIfExists(JSONObject object, String attrName, ConnectorObjectBuilder builder) {
        if (object.has(attrName)) {
            if (object.get(attrName) != null && !JSONObject.NULL.equals(object.get(attrName))) {
                addAttr(builder, attrName, object.getString(attrName));
            }
        }
    }

    private void getMultiIfExists(JSONObject object, String attrName, ConnectorObjectBuilder builder) {
        if (object.has(attrName)) {
            Object valueObject = object.get(attrName);
            if (object.get(attrName) != null && !JSONObject.NULL.equals(valueObject)) {
                List<String> values = new ArrayList<>();
                if (valueObject instanceof JSONArray) {
                    JSONArray objectArray = object.getJSONArray(attrName);
                    for (int i = 0; i < objectArray.length(); i++) {
                        values.add(objectArray.getString(i));
                    }
                    builder.addAttribute(attrName, values.toArray());
                } else if (valueObject instanceof String) {
                    addAttr(builder, attrName, object.getString(attrName));
                } else {
                    throw new InvalidAttributeValueException("Unsupported value '" + valueObject + "' for attribute name '" + attrName + "' from " + object);
                }
            }
        }
    }


}
