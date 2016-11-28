package com.evolveum.polygon.connector.drupal;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.*;

/**
 * Created by gpalos on 6. 9. 2016.
 */
public class NodeCache {
    private static final Log LOG = Log.getLog(NodeCache.class);

    Map<String, Map<String, String>> cacheById = new HashMap<>();
    Map<String, Map<String, String>> cacheByName = new HashMap<>();

    DrupalConnector connector;

    public NodeCache(DrupalConnector connector) throws IOException {
        this.connector = connector;
        Set<String> proceed = new HashSet<>();
        for (String type : connector.getConfiguration().getUser2nodes().values()) {
            if (proceed.contains(type)) {
                continue;
            }
            else {
                proceed.add(type);
            }
            int pageSize = connector.getConfiguration().getPageSize();
            int page = 0;
            while (true) {
                String pageing = connector.processPaging(page, pageSize);
                HttpGet request = new HttpGet(connector.getConfiguration().getServiceAddress() + connector.NODE + "?parameters[" + connector.ATTR_NODE_TYPE + "]="
                        + type + pageing + "&fields=nid,title");
                JSONArray nodes = connector.callRequest(request);

                for (int i=0; i<nodes.length(); i++){
                    JSONObject taxonomy = nodes.getJSONObject(i);
                    String key = taxonomy.getString(connector.NID);
                    String value = taxonomy.getString(connector.ATTR_NODE_TITLE);
                    putToCache(type, key, value);
                }

                if (nodes.length()==0 || nodes.length()<pageSize)
                {
                    break;
                }
                page++;
            }
            LOG.ok("nodeCache for type "+type+" initialized, count {0}: values {1}", cacheById.get(type).size(), cacheById.get(type));
        }
    }

    private void putToCache(String type, String key, String value) {
        if (cacheById.get(type) == null) {
            cacheById.put(type, new HashMap<String, String>());
            cacheByName.put(type, new HashMap<String, String>());
        }

        if (cacheById.get(type).containsKey(key)) {
            throw new InvalidAttributeValueException("NID '"+key+"' (value: '"+value+"') for type '"+type+"' already exists in nodeCache" + cacheById.get(type));
        }
        else {
            cacheById.get(type).put(key, value);
        }

        if (cacheByName.get(type).containsKey(value)) {
            throw new InvalidAttributeValueException("Value '"+value+"' (TID: "+key+") for type '"+type+"' already exists in nodeCache: " + cacheByName.get(type));
        }
        else if (StringUtil.isNotEmpty(value)){
            cacheByName.get(type).put(value, key);
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

    public String getName(String type, String id) {
        LOG.ok("getName for type {0} and id {1}", type, id);
        if (!cacheById.get(type).containsKey(id)) {
            // read it and put to cache

            try {
                HttpGet request = new HttpGet(connector.getConfiguration().getServiceAddress() + connector.NODE + "/" + id);
                JSONObject node = connector.callRequest(request, true);
                String value = node.getString(connector.ATTR_NODE_TITLE);

                String typeFromResource = node.getString(connector.ATTR_NODE_TYPE);
                if (!type.equals(typeFromResource)){
                    if (connector.getConfiguration().getIgnoreTypeMismatch()){
                        LOG.warn("Expected " + type + ", but get " + typeFromResource + " for NID:" + id+" ("+value+"), returning NULL");
                        return null;
                    }
                    else {
                        throw new InvalidAttributeValueException("Expected " + type + ", but get " + typeFromResource + " for NID:" + id+" ("+value+")");
                    }
                }

                putToCache(type, id, value);
            } catch (IOException e) {
                throw new ConnectorIOException(e.getMessage(), e);
            }
        }

        return cacheById.get(type).get(id);
    }

    public String getIdOrCreate(String type, String fieldValue) {
        LOG.ok("getIdOrCreate for type {0} and value {1}", type, fieldValue);
        String id = cacheByName.get(type).get(fieldValue);
        if (StringUtil.isNotEmpty(id)) {
            return id; // exists & is OK
        }
        else  {
            try {
                // check if not created before
                HttpGet requestFind = new HttpGet(connector.getConfiguration().getServiceAddress() + connector.NODE +
                        "?parameters[" + connector.ATTR_NODE_TYPE + "]=" + type + "&parameters["+connector.ATTR_NODE_TITLE+"]="+ URLEncoder.encode(fieldValue, "UTF-8"));
                JSONArray entities = connector.callRequest(requestFind);
                if (entities.length()>1){
                    List<String> nids = new LinkedList<>();
                    for (int i=0; i<entities.length();i++) {
                        JSONObject entity = entities.getJSONObject(i);
                        nids.add(entity.getString(connector.NID));
                    }
                    throw new InvalidAttributeValueException("Value '"+fieldValue+"' is not unique, has more than one NID: "+nids+" for type '"+type+"', transformation is not possible");
                }
                else if (entities.length()==1){
                    JSONObject entity = entities.getJSONObject(0);
                    String nid = entity.getString(connector.NID);
                    String value = entity.getString(connector.ATTR_NODE_TITLE);

                    putToCache(type, nid, value);
                    LOG.ok("Existing value found on resource for value: "+fieldValue+", type: "+type+", NID: "+nid);
                    return nid;
                }
                LOG.ok("Existing value not found on resource for value: "+fieldValue+", type: "+type+", creating new...");
                // else not found
                if (!connector.getConfiguration().isCreateNodeWhenTitleNotExists(type)){
                    throw new InvalidAttributeValueException("Value '"+fieldValue+"' in type '"+type+"' not existst and auto-create is disabled");
                }

                // creating new node
                JSONObject jo = new JSONObject();
                jo.put(connector.ATTR_NODE_TITLE, fieldValue);
                jo.put(connector.ATTR_NODE_TYPE, type);
                // body is mandatory
                JSONObject bodyValue = new JSONObject();
                bodyValue.put(DrupalConnector.VALUE, "<p></p>");
                JSONArray bodyUndArray = new JSONArray();
                bodyUndArray.put(bodyValue);
                JSONObject bodyUnd = new JSONObject();
                bodyUnd.put("und", bodyUndArray);
                jo.put(connector.ATTR_NODE_BODY, bodyUnd);
                // if we need to use not only basic title, but also extended field, also fill it
                // TODO: do you need also this when you read it?
                for (String key : connector.getConfiguration().getNodesKeys().keySet())
                {
                    String keyValue = connector.getConfiguration().getNodesKeys().get(key);
                    // "title_field":{"und":[{"value":"department title"}]
                    JSONObject value = new JSONObject();
                    value.put(connector.getConfiguration().getNodesMetadatas().get(type).get(keyValue), fieldValue);
                    JSONArray undArray = new JSONArray();
                    undArray.put(value);
                    JSONObject und = new JSONObject();
                    und.put("und", undArray);
                    jo.put(keyValue, und);
                }
                LOG.ok("request body: {0}", jo.toString());

                HttpEntityEnclosingRequestBase requestCreate = new HttpPost(connector.getConfiguration().getServiceAddress() + connector.NODE);
                JSONObject jores = connector.callRequest(requestCreate, jo);
                String newId = jores.getString(connector.NID);
                LOG.info("response NID: {0}", newId);
                putToCache(type, newId, fieldValue);
                return newId;
            } catch (IOException e) {
                throw new ConnectorIOException(e.getMessage(), e);
            }
        }
    }
}
