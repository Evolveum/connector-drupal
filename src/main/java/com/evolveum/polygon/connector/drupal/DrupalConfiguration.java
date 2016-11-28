/**
 * Copyright (c) 2016 Evolveum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.evolveum.polygon.connector.drupal;

import com.evolveum.polygon.rest.AbstractRestConfiguration;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.ConfigurationProperty;

import java.util.*;

/**
 * @author semancik
 *
 */
public class DrupalConfiguration extends AbstractRestConfiguration {

    /**
     * if true, instead of deletion, only disable user (default=false)
     */
    private Boolean userDeleteDisabled = false;

    /**
     * default page size when listing
     */
    private int pageSize = 20;

    /**
     * Array of custom fields configuration, for example:
     * <ul>
     *     <li>name of user custom field : name of JSON key, where put custom field value (field_first_name:value)</li>
     *     <li>name of user custom field : string 'tid' (taxonomy ID) representing reference to taxonomy :
     *      name of taxonomy defined in 'taxonomies' parameter (field_user_location:tid:location).
     *      Attribute 'field_user_location' contains ID of taxonomy (TID), his related name is transformed to attribute 'field_user_location_transformed'.</li>
     *     <li>name of user custom field : string 'nid' (node ID) representing reference to node (field_department:nid)</li>
     *     <li>name of user custom field : string 'fid' (file ID) representing reference to file (field_avatar:fid)
     *     Attribute 'field_avatar' contains ID of file (FID), his related content is transformed to attribute 'field_avatar_transformed'.</li>
     * </ul>
     */
    private String[] userFields;

    /**
     * Array of taxonomy machine names (taxonomy vocabulary), for example:
     * <ul>
     *     <li>name of machine name : related vocabulary ID (VID) (location:4)</li>
     *     <li>name of machine name : related vocabulary ID (VID) = list of custom fields separated by comma (',')
     *     and format is name of user custom field : name of JSON key, where put custom field value  (company_structure:1=field_structure_department:value)</li>
     * </ul>
     */
    private String[] taxonomies;

    /**
     * If taxonomy name for these taxonomy machine names not found in drupal, create it, elsewhere throw InvalidAttributeValueException (read only).
     */
    private String[] createTaxonomyWhenNameNotExists;

    /**
     * Array of node types (content types), for example:
     * <ul>
     *     <li>name of node type (article)</li>
     *     <li>name of machine name : related vocabulary ID (VID) = list of custom fields separated by comma (',')
     *     and format is name of user custom field : name of JSON key, where put custom field value (department=title_field:value)</li>
     * </ul>
     */
    private String[] nodes;

    /**
     * If node name not found in drupal for these content type, create it, elsewhere throw InvalidAttributeValueException (read only).
     */
    private String[] createNodeWhenTitleNotExists;

    /**
     * Array of required custom fields, for example 'field_first_name'. If field not set throws InvalidAttributeValueException.
     */
    private String[] requiredFields;

    /**
     * if true, ignore in cache content Type/machine name mismatch and return null, elsewhere InvalidAttributeValueException is returned
     */
    private Boolean ignoreTypeMismatch = false;

    /**
     * if true, when GET of not existing taxonomy term returned 500, transformed to UnknownUidException (workaround for Drupal bug)
     */
    private Boolean handle500asUnknownUidException = false;

    /**
     * if true, reading admin user details is skipped (only cache is created)
     */
    private Boolean skipTestConnection = false;

    /* * * * * * * * * * * * * * * * * * *
    only parsed metadatas from configuration
     * * * * * * * * * * * * * * * * * * * */

    /**
     * which user custom fields and which appropriated nud key is used
     */
    private Map<String, String> userMetadatas;

    /**
     * list of single valued user fields
     */
    private List<String> singleValueUserFields = new LinkedList<>();

    /**
     * which user fields are FILE references
     */
    private List<String> user2files = new LinkedList<>();

    /**
     * which user custom fields to which taxonomy machine name is referenced (caches taxonomies)
     */
    private Map<String, String> user2taxonomies = new LinkedHashMap();

    /**
     * which taxonomies vocabulary, which custom fields and which appropriated nud key is used
     */
    private Map<String, Map<String, String>> taxonomiesMetadatas;

    /**
     * which vocabulary_machine_name has which VID (vocabulary ID)
     */
    private Map<String, String> taxonomiesKeys = new LinkedHashMap();

    /**
     * which node type, which custom fields and which appropriated nud key is used
     */
    private Map<String, Map<String, String>> nodesMetadatas;

    /**
     * which user custom fields to which node type (content type) is referenced (caches nodes)
     */
    private Map<String, String> user2nodes = new LinkedHashMap();

    /**
     * which node type has which KEY (column name instead of title)
     */
    private Map<String, String> nodesKeys = new LinkedHashMap();

    @Override
    public String toString() {
        return "DrupalConfiguration{" +
                "username=" + getUsername() +
                ", serviceAddress=" + getServiceAddress() +
                ", authMethod=" + getAuthMethod() +
                ", userDeleteDisabled=" + userDeleteDisabled +
                ", userFields=" + Arrays.toString(userFields) +
                ", taxonomies=" + Arrays.toString(taxonomies) +
                ", nodes=" + Arrays.toString(nodes) +
                ", requiredFields=" + Arrays.toString(requiredFields) +
                '}';
    }

    void parseMetadatas() {
        taxonomiesMetadatas = parseEntities(taxonomies);
        nodesMetadatas = parseEntities(nodes);
        userMetadatas = parseFields(userFields, null, true);
    }

    private Map<String, Map<String, String>> parseEntities(String[] entities) {
        Map<String, Map<String, String>> entityMetadatas = new LinkedHashMap<String, Map<String, String>>();
        if (entities == null || (entities.length == 1 && StringUtil.isEmpty(entities[0]))) {
            return entityMetadatas;
        }

        if (entities != null) {
            for (String tableDef : entities) {
                if (StringUtil.isEmpty(tableDef)){
                    continue;
                }
                Map<String, String> fields = new HashMap();
                String[] entity = tableDef.split("=");
                String entityName = entity[0];

                if (entityName.contains(":")){
                    // taxonomy and has his keys
                    String[] vocabulary = entityName.split(":");
                    if (vocabulary == null || vocabulary.length != 2) {
                        throw new ConfigurationException("please use correct vocabulary schema definition, for example: 'positions:2' ({vocabulary_machine_name}:{vocabulary ID}), got: " + entityName);
                    }
                    entityName = vocabulary[0];
                    String vid = vocabulary[1];
                    taxonomiesKeys.put(entityName, vid);
                }

                if (entity != null && entity.length == 1 ){
                    entityMetadatas.put(entityName, fields); // no custom fields
                    continue;
                }
                if (entity == null || entity.length != 2) {
                    throw new ConfigurationException("please use correct schema definition, for example: 'department=field_department:value,field_image:fid', got: " + tableDef);
                }

                String[] fieldsMetaDatas = entity[1].split(",");
                if (StringUtil.isEmpty(entity[1]) || fieldsMetaDatas == null || fieldsMetaDatas.length == 0) {
                    entityMetadatas.put(entityName, fields); // no custom fields
                    continue;
                }

                fields = parseFields(fieldsMetaDatas, entityName, false);

                entityMetadatas.put(entityName, fields);
            }
        }

        return entityMetadatas;
    }

    private Map<String, String> parseFields(String[] fieldsMetaDatas, String entityName, boolean user){
        Map<String, String> fields = new HashMap();
        if (fieldsMetaDatas == null) {
            return fields;
        }
        for (String fieldMetaData : fieldsMetaDatas) {
            if (StringUtil.isEmpty(fieldMetaData)){
                continue;
            }
            String[] field = fieldMetaData.split(":");
            if (field == null || field.length < 2) {
                throw new ConfigurationException("please use correct attribute schema definition, for example: 'field_first_name:value', got: " + fieldMetaData);
            }
            String fieldName = field[0];
            String fieldNudKey = field[1];
            if (field.length>=3) {
                String reference = field[2];
                if (DrupalConnector.NID.equals(fieldNudKey)) {
                    user2nodes.put(fieldName, reference);
                }
                else if (DrupalConnector.TID.equals(fieldNudKey)) {
                    user2taxonomies.put(fieldName, reference);
                }
                if (entityName != null && DrupalConnector.NODE_KEY.equals(reference)) {
                    nodesKeys.put(entityName, fieldName);
                }
            }
            if (field.length>=4) {
                String widgetType = field[3]; // when this is "Select List", we need to send other data when user is created
                if (widgetType.contains("SINGLE") || widgetType.contains("Select List")) {
                    singleValueUserFields.add(fieldName);
                }
            }

            fields.put(fieldName, fieldNudKey);
            if (user && DrupalConnector.FID.equals(fieldNudKey)) {
                user2files.add(fieldName);
            }
        }
        return fields;
    }

    List<String> getRequiredFieldsList(){
        if (requiredFields == null || (requiredFields.length == 1 && StringUtil.isEmpty(requiredFields[0]))) {
            return new ArrayList<>();
        }
        return Arrays.asList(requiredFields);
    }

    @ConfigurationProperty(displayMessageKey = "drupal.config.userDeleteDisabled",
            helpMessageKey = "drupal.config.userDeleteDisabled.help")
    public Boolean getUserDeleteDisabled() {
        return userDeleteDisabled;
    }

    public void setUserDeleteDisabled(Boolean userDeleteDisabled) {
        this.userDeleteDisabled = userDeleteDisabled;
    }

    @ConfigurationProperty(displayMessageKey = "drupal.config.pageSize",
            helpMessageKey = "drupal.config.pageSize.help")
    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    @ConfigurationProperty(displayMessageKey = "drupal.config.userFields",
            helpMessageKey = "drupal.config.userFields.help")
    public String[] getUserFields() {
        return userFields;
    }

    public void setUserFields(String[] userFields) {
        this.userFields = userFields;
    }

    @ConfigurationProperty(displayMessageKey = "drupal.config.taxonomies",
            helpMessageKey = "drupal.config.taxonomies.help")
    public String[] getTaxonomies() {
        return taxonomies;
    }

    public void setTaxonomies(String[] taxonomies) {
        this.taxonomies = taxonomies;
    }

    @ConfigurationProperty(displayMessageKey = "drupal.config.nodes",
            helpMessageKey = "drupal.config.nodes.help")
    public String[] getNodes() {
        return nodes;
    }

    public void setNodes(String[] nodes) {
        this.nodes = nodes;
    }

    @ConfigurationProperty(displayMessageKey = "drupal.config.requiredFields",
            helpMessageKey = "drupal.config.requiredFields.help")
    public String[] getRequiredFields() {
        return requiredFields;
    }

    public void setRequiredFields(String[] requiredFields) {
        this.requiredFields = requiredFields;
    }

    @ConfigurationProperty(displayMessageKey = "drupal.config.ignoreTypeMismatch",
            helpMessageKey = "drupal.config.ignoreTypeMismatch.help")
    public Boolean getIgnoreTypeMismatch() {
        return ignoreTypeMismatch;
    }

    public void setIgnoreTypeMismatch(Boolean ignoreTypeMismatch) {
        this.ignoreTypeMismatch = ignoreTypeMismatch;
    }

    @ConfigurationProperty(displayMessageKey = "drupal.config.handle500asUnknownUidException",
            helpMessageKey = "drupal.config.handle500asUnknownUidException.help")
    public Boolean getHandle500asUnknownUidException() {
        return handle500asUnknownUidException;
    }

    public void setHandle500asUnknownUidException(Boolean handle500asUnknownUidException) {
        this.handle500asUnknownUidException = handle500asUnknownUidException;
    }

    @ConfigurationProperty(displayMessageKey = "drupal.config.skipTestConnection",
            helpMessageKey = "drupal.config.skipTestConnection.help")
    public Boolean getSkipTestConnection() {
        return skipTestConnection;
    }

    public void setSkipTestConnection(Boolean skipTestConnection) {
        this.skipTestConnection = skipTestConnection;
    }

    @ConfigurationProperty(displayMessageKey = "drupal.config.createTaxonomyWhenNameNotExists",
            helpMessageKey = "drupal.config.createTaxonomyWhenNameNotExists.help")
    public String[] getCreateTaxonomyWhenNameNotExists() {
        return createTaxonomyWhenNameNotExists;
    }

    public void setCreateTaxonomyWhenNameNotExists(String[] createTaxonomyWhenNameNotExists) {
        this.createTaxonomyWhenNameNotExists = createTaxonomyWhenNameNotExists;
    }

    public Map<String, String> getUserMetadatas() {
        return userMetadatas;
    }

    public Map<String, Map<String, String>> getTaxonomiesMetadatas() {
        return taxonomiesMetadatas;
    }

    public Map<String, String> getTaxonomiesKeys() {
        return taxonomiesKeys;
    }

    public Map<String, Map<String, String>> getNodesMetadatas() {
        return nodesMetadatas;
    }

    @ConfigurationProperty(displayMessageKey = "drupal.config.createNodeWhenTitleNotExists",
            helpMessageKey = "drupal.config.createNodeWhenTitleNotExists.help")
    public String[] getCreateNodeWhenTitleNotExists() {
        return createNodeWhenTitleNotExists;
    }

    public void setCreateNodeWhenTitleNotExists(String[] createNodeWhenTitleNotExists) {
        this.createNodeWhenTitleNotExists = createNodeWhenTitleNotExists;
    }

    public Map<String, String> getUser2taxonomies() {
        return user2taxonomies;
    }

    public List<String> getUser2files() {
        return user2files;
    }

    public Map<String, String> getUser2nodes() {
        return user2nodes;
    }

    public Map<String, String> getNodesKeys() {
        return nodesKeys;
    }

    public List<String> getSingleValueUserFields() {
        return singleValueUserFields;
    }

    public boolean isCreateNodeWhenTitleNotExists(String contentType) {
        if (createNodeWhenTitleNotExists==null || createNodeWhenTitleNotExists.length == 0) {
            return false;
        }
        for (String type : createNodeWhenTitleNotExists){
            if (contentType.equals(type)) {
                return true;
            }
        }

        return false;
    }

    public boolean isCreateTaxonomyWhenNameNotExists(String machineName) {
        if (createTaxonomyWhenNameNotExists==null || createTaxonomyWhenNameNotExists.length == 0) {
            return false;
        }
        for (String type : createTaxonomyWhenNameNotExists){
            if (machineName.equals(type)) {
                return true;
            }
        }

        return false;
    }

}
