package com.evolveum.polygon.connector.drupal;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.*;

/**
 * Created by gpalos on 22. 8. 2016.
 */
public class TaxonomyCache {
    private static final Log LOG = Log.getLog(TaxonomyCache.class);

    Map<String, Map<String, String>> cacheById = new HashMap<>();
    Map<String, Map<String, String>> cacheByName = new HashMap<>();

    DrupalConnector connector;

    public TaxonomyCache(DrupalConnector connector) throws IOException {
        this.connector = connector;
        Set<String> proceed = new HashSet<>();
        for (String machineName : connector.getConfiguration().getUser2taxonomies().values()) {
            if (proceed.contains(machineName)) {
                continue;
            }
            else {
                proceed.add(machineName);
            }
            int pageSize = connector.getConfiguration().getPageSize();
            int page = 0;
            while (true) {
                String pageing = connector.processPaging(page, pageSize);
                HttpGet request = new HttpGet(connector.getConfiguration().getServiceAddress() + connector.TAXONOMY_TERM + "?parameters[" + connector.VID + "]="
                        + connector.getConfiguration().getTaxonomiesKeys().get(machineName) + pageing + "&fields=tid,name");
                JSONArray taxonomies = connector.callRequest(request);

                for (int i=0; i<taxonomies.length(); i++){
                    JSONObject taxonomy = taxonomies.getJSONObject(i);
                    String key = taxonomy.getString(connector.TID);
                    String value = taxonomy.getString(connector.ATTR_NAME);
                    putToCache(machineName, key, value);
                }

                if (taxonomies.length()==0 || taxonomies.length()<pageSize)
                {
                    break;
                }
                page++;
            }
            LOG.ok("taxonomyCache for machine name "+machineName+" initialized, count {0}: values {1}", cacheById.get(machineName).size(), cacheById.get(machineName));
        }
    }

    private void putToCache(String machineName, String key, String value) {
        if (cacheById.get(machineName) == null) {
            cacheById.put(machineName, new HashMap<String, String>());
            cacheByName.put(machineName, new HashMap<String, String>());
        }

        if (cacheById.get(machineName).containsKey(key)) {
            throw new InvalidAttributeValueException("TID '"+key+"' (value: '"+value+"') for machine name '"+machineName+"' already exists in taxonomyCache" + cacheById.get(machineName));
        }
        else {
            cacheById.get(machineName).put(key, value);
        }

        if (cacheByName.get(machineName).containsKey(value)) {
            throw new InvalidAttributeValueException("Value '"+value+"' (TID: "+key+") for machine name '"+machineName+"' already exists in taxonomyCache: " + cacheByName.get(machineName));
        }
        else if (StringUtil.isNotEmpty(value)){
            cacheByName.get(machineName).put(value, key);
        }
    }

    public void clear() {
        if (cacheById != null) {
            cacheById.clear();
            cacheById = null;
        }
        if (cacheByName != null) {
            cacheByName.clear();
            cacheByName = null;
        }

        this.connector = null;
    }

    public String getName(String machineName, String id) {
        LOG.ok("getName for machine name {0} and id {1}", machineName, id);
        if (!cacheById.get(machineName).containsKey(id)) {
            // read it and put to taxonomyCache

            HttpGet request = new HttpGet(connector.getConfiguration().getServiceAddress() + connector.TAXONOMY_TERM + "/" + id);
            JSONObject taxonomy = null;
            try {
                taxonomy = connector.callRequest(request, true);
            } catch (ConnectorException ce){
                if (ce.getMessage().contains("HTTP error 500 Internal Server Error"))
                {
                    LOG.warn(ce, "probably already deleted TID, returning NULL as his value");
                    return null;
                }
                else {
                    throw ce;
                }
            } catch (IOException ioe){
                throw new ConnectorIOException(ioe.getMessage(), ioe);
            }


            String value = taxonomy.getString(connector.ATTR_NAME);

            String machineNameFromResource = taxonomy.getString(connector.ATTR_TAX_VOCABULARY_MACHINE_NAME);
            if (!machineName.equals(machineNameFromResource)) {
                if (connector.getConfiguration().getIgnoreTypeMismatch()) {
                    LOG.warn("Expected \"+machineName+\", but get \"+machineNameFromResource+\" for TID:" + id + " (" + value + "), returning NULL");
                    return null;
                } else {
                    throw new InvalidAttributeValueException("Expected " + machineName + ", but get " + machineNameFromResource + " for TID:" + id + " (" + value + ")");
                }
            }

            cacheById.get(machineName).put(id, value);
            cacheByName.get(machineName).put(value, id);
        }

        return cacheById.get(machineName).get(id);
    }

    public String getIdOrCreate(String machineName, String fieldValue) {
        LOG.ok("getIdOrCreate for machine name {0} and value {1}", machineName, fieldValue);
        String id = cacheByName.get(machineName).get(fieldValue);
        if (StringUtil.isNotEmpty(id)) {
            return id; // exists & is OK
        }
        else {
            try {
                // check if not created before
                HttpGet requestFind = new HttpGet(connector.getConfiguration().getServiceAddress() + connector.TAXONOMY_TERM +
                        "?parameters[" + connector.VID + "]=" + connector.getConfiguration().getTaxonomiesKeys().get(machineName) + "&parameters["+connector.ATTR_NAME+"]="+ URLEncoder.encode(fieldValue, "UTF-8"));
                JSONArray entities = connector.callRequest(requestFind);
                if (entities.length()>1){
                    List<String> tids = new LinkedList<>();
                    for (int i=0; i<entities.length();i++) {
                        JSONObject entity = entities.getJSONObject(i);
                        tids.add(entity.getString(connector.TID));
                    }
                    throw new InvalidAttributeValueException("Value '"+fieldValue+"' is not unique, has more than one TID: "+tids+" for machine name '"+machineName+"', transformation is not possible");
                }
                else if (entities.length()==1){
                    JSONObject entity = entities.getJSONObject(0);
                    String tid = entity.getString(connector.TID);
                    String value = entity.getString(connector.ATTR_NAME);

                    cacheById.get(machineName).put(tid, value);
                    cacheByName.get(machineName).put(value, tid);
                    LOG.ok("Existing value found on resource for value: "+fieldValue+", machineName: "+machineName+", TID: "+tid);
                    return tid;
                }
                LOG.ok("Existing value not found on resource for value: "+fieldValue+", machineName: "+machineName+", creating new...");
                // else not found
                if (!connector.getConfiguration().isCreateTaxonomyWhenNameNotExists(machineName)){
                    throw new InvalidAttributeValueException("Value '"+fieldValue+"' in machine name '"+machineName+"' not existst and auto-create is disabled");
                }

                // creating new taxonomy
                JSONObject jo = new JSONObject();
                jo.put(connector.ATTR_NAME, fieldValue);
                jo.put(connector.ATTR_TAX_WEIGHT, connector.ATTR_TAX_WEIGHT_DEFAULT);
                jo.put(connector.ATTR_TAX_VOCABULARY_MACHINE_NAME, machineName);
                LOG.ok("request body: {0}", jo.toString());

                HttpEntityEnclosingRequestBase requestCreate = new HttpPost(connector.getConfiguration().getServiceAddress() + connector.TAXONOMY_TERM);
                JSONObject jores = connector.callRequest(requestCreate, jo);
                String newId = jores.getString(connector.TID);
                LOG.info("response TID: {0}", newId);
                cacheById.get(machineName).put(newId, fieldValue);
                cacheByName.get(machineName).put(fieldValue, newId);
                return newId;
            } catch (IOException e) {
                throw new ConnectorIOException(e.getMessage(), e);
            }
        }
    }
}
