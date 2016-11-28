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

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.identityconnectors.common.Base64;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.objects.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author oscar
 */
public class TestClient {

    private static final Log LOG = Log.getLog(TestClient.class);

    private static DrupalConfiguration conf;
    private static DrupalConnector conn;

    ObjectClass accountObjectClass = new ObjectClass(ObjectClass.ACCOUNT_NAME);

    @BeforeClass
    public static void setUp() throws Exception {
        String fileName = "test.properties";

        final Properties properties = new Properties();
        InputStream inputStream = TestClient.class.getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            throw new IOException("Sorry, unable to find " + fileName);
        }
        properties.load(inputStream);

        conf = new DrupalConfiguration();
        conf.setUsername(properties.getProperty("username"));
        conf.setPassword(new GuardedString(properties.getProperty("password").toCharArray()));
        conf.setServiceAddress(properties.getProperty("serviceAddress"));
        conf.setAuthMethod(properties.getProperty("authMethod"));
        conf.setTrustAllCertificates(Boolean.parseBoolean(properties.getProperty("trustAllCertificates")));

        conf.setUserDeleteDisabled(Boolean.parseBoolean(properties.getProperty("userDeleteDisabled")));

        conf.setPageSize(Integer.parseInt(properties.getProperty("pageSize")));

        if (properties.containsKey("userFields")) {
            String[] userFields = properties.getProperty("userFields").split(";");
            conf.setUserFields(userFields);
        }

        if (properties.containsKey("taxonomies")) {
            String[] taxonomies = properties.getProperty("taxonomies").split(";");
            conf.setTaxonomies(taxonomies);
        }

        if (properties.containsKey("createTaxonomyWhenNameNotExists")) {
            String[] createTaxonomyWhenNameNotExists = properties.getProperty("createTaxonomyWhenNameNotExists").split(";");
            conf.setCreateTaxonomyWhenNameNotExists(createTaxonomyWhenNameNotExists);
        }

        if (properties.containsKey("createNodeWhenTitleNotExists")) {
            String[] createNodeWhenTitleNotExists = properties.getProperty("createNodeWhenTitleNotExists").split(";");
            conf.setCreateNodeWhenTitleNotExists(createNodeWhenTitleNotExists);
        }

        if (properties.containsKey("userFields")) {
            String[] userFields = properties.getProperty("userFields").split(";");
            conf.setUserFields(userFields);
        }

        if (properties.containsKey("requiredFields")) {
            String[] requiredFields = properties.getProperty("requiredFields").split(";");
            conf.setRequiredFields(requiredFields);
        }
        conn = new DrupalConnector();
        conn.init(conf);
    }

    @Test
    public void testConn() {
        conn.test();
    }

    @Test
    public void testSchema() {
        Schema schema = conn.schema();
        LOG.info("schema: " + schema);
    }

    @Test
    public void testGetAdmin() throws IOException {
        HttpGet request = new HttpGet(conf.getServiceAddress() + "/user/1");
        CloseableHttpResponse response = conn.execute(request);
        conn.processResponseErrors(response);
        LOG.info("resp: {0}", response);
        String result = EntityUtils.toString(response.getEntity());
        LOG.info("content: {0}", result);
        JSONObject json = new JSONObject(result);
        LOG.info("name: {0}", json.getJSONObject("field_display_name").getJSONArray("und").getJSONObject(0).get("value"));
    }

    @Test
    public void testCreateJson() throws IOException {
        JSONObject jo = new JSONObject();
        jo.put("name", "evo");

        JSONObject roles = new JSONObject();
        roles.put("1", "1");
        roles.put("2", "2");
        jo.put("roles", roles);

        JSONObject value = new JSONObject();
        value.put("tid", "123");
        JSONArray undArray = new JSONArray();
        undArray.put(value);
        JSONObject und = new JSONObject();
        und.put("und", undArray);
        jo.put("field_department", und);

        LOG.info("json: {0}", jo.toString());
        throw new IOException("e");
    }

    @Test
    public void testParseJson() {
        JSONArray users = new JSONArray("[ {\"uid\": \"1\"}, {\"uid\": \"2\"}]");
        JSONArray emptyUsers = new JSONArray("[]");
    }

    @Test
    public void testCreateUser() {

        //create
        Set<Attribute> attributes = new HashSet<Attribute>();
        String randName = "test_conn9";// + (new Random()).nextInt();
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_MAIL, randName + "@evolveum.com"));
        attributes.add(AttributeBuilder.build(Name.NAME, randName));
//        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_THEME, "theme1"));
        attributes.add(AttributeBuilder.build("field_first_name", "first name"));
        attributes.add(AttributeBuilder.build("field_display_name", "display name"));
        attributes.add(AttributeBuilder.build("field_department", "1308"));
//        attributes.add(AttributeBuilder.build("field_pub_department", "284")); //Integrations
        attributes.add(AttributeBuilder.build("field_pub_department" + conn.TRANSFORMED_POSTFIX, "Integrations")); //Integrations
        attributes.add(AttributeBuilder.build("field_pub_location", "226"));
        attributes.add(AttributeBuilder.build("field_pub_team", "207"));

        String[] roles = {"17", "5", "6"};
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_ROLES, roles));

        GuardedString gs = new GuardedString("test123".toCharArray());
//        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.PASSWORD.getName(), gs));

//        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.ENABLE.getName(), true));

        Uid userUid = conn.create(accountObjectClass, attributes, null);
        LOG.ok("New user Uid is: {0}, name: {1}", userUid.getUidValue(), randName);
    }

    @Test
    public void testDeleteUser() {
        Uid uid = new Uid("2461");
        conn.delete(accountObjectClass, uid, null);
    }

    @Test
    public void testUpdateUser() {

        Uid uid = new Uid("2465");
        //create
        Set<Attribute> attributes = new HashSet<Attribute>();
        String randName = "test_conn8v2";// + (new Random()).nextInt();
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_MAIL, randName + "@evolveum.com"));
        attributes.add(AttributeBuilder.build(Name.NAME, randName));
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_THEME, "theme1"));
        attributes.add(AttributeBuilder.build("field_first_name", "first name v2"));
        attributes.add(AttributeBuilder.build("field_display_name", "display name v2"));
//        attributes.add(AttributeBuilder.build("field_department", "1307"));
//        attributes.add(AttributeBuilder.build("field_pub_department" + conn.TRANSFORMED_POSTFIX, "Integrations")); //Integrations
////        attributes.add(AttributeBuilder.build("field_pub_department", "283"));
//        attributes.add(AttributeBuilder.build("field_pub_location", "173"));
//        attributes.add(AttributeBuilder.build("field_pub_team", "300"));

        String[] roles = {"17", "7"};
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_ROLES, roles));

        GuardedString gs = new GuardedString("test123".toCharArray());
        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.PASSWORD.getName(), gs));

        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.ENABLE.getName(), false));

        Uid userUid = conn.update(accountObjectClass, uid, attributes, null);
        LOG.ok("User {0} updated", userUid.getUidValue());
    }

    @Test
    public void testUpdateUser2() {

        Uid uid = new Uid("2476");
        //create
        Set<Attribute> attributes = new HashSet<Attribute>();
        String randName = "test_conn8v2";// + (new Random()).nextInt();
        attributes.add(AttributeBuilder.build("field_pub_location", "179"));

        Uid userUid = conn.update(accountObjectClass, uid, attributes, null);
        LOG.ok("User {0} updated", userUid.getUidValue());
    }

    @Test
    public void findByUid() {
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // searchByUId
        DrupalFilter searchByUid = new DrupalFilter();
        searchByUid.byUid = "2470";
        LOG.ok("start finding");
        conn.executeQuery(accountObjectClass, searchByUid, rh, null);
        LOG.ok("end finding");
    }

    @Test
    public void findByName() {
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // searchByUId
        DrupalFilter searchByUid = new DrupalFilter();
        searchByUid.byName = "test_evolveum";
        conn.executeQuery(accountObjectClass, searchByUid, rh, null);
    }

    @Test
    public void findByMail() {
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // searchByUId
        DrupalFilter searchByUid = new DrupalFilter();
        searchByUid.byEmailAddress = "gustav.palos@evolveum.com";
        conn.executeQuery(accountObjectClass, searchByUid, rh, null);
    }

    @Test
    public void findAll() {
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // all
        DrupalFilter filter = new DrupalFilter();
        conn.executeQuery(accountObjectClass, filter, rh, null);
    }

    @Test
    public void findOnePage() {
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // all
        DrupalFilter filter = new DrupalFilter();

        Integer pageSize = 10;// options.getPageSize();
        Integer pagedResultsOffset = 1; //options.getPagedResultsOffset();
        Map<String, Object> map = new HashMap<>();
        map.put(OperationOptions.OP_PAGE_SIZE, pageSize);
        map.put(OperationOptions.OP_PAGED_RESULTS_OFFSET, pagedResultsOffset);
        OperationOptions options = new OperationOptions(map);
        conn.executeQuery(accountObjectClass, filter, rh, options);
    }

    private static String nodeId = "27506"; //"27506";//"27483"; // Marketing
    private static String nodeName = "test_node";
    private static final ObjectClass nodeDepartmentObjectClass = new ObjectClass(DrupalConnector.OC_NODE_Prefix + "department");

    @Test
    public void testCreateNode() {

        //create
        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build("title_field", nodeName));
//        attributes.add(AttributeBuilder.build(Name.NAME, randName));
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_NODE_STATUS, "1"));
        attributes.add(AttributeBuilder.build("field_image", "4489"));
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_NODE_BODY, nodeName + " body"));
        attributes.add(AttributeBuilder.build("field_icon", "4489"));

        Uid nodeNid = conn.create(nodeDepartmentObjectClass, attributes, null);
        LOG.ok("New node NID is: {0}, name: {1}", nodeNid.getUidValue(), nodeName);
        TestClient.nodeId = nodeNid.getUidValue();
    }

    @Test
    public void testSearchNodeByUid() {

        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // searchByUId
        DrupalFilter filter = new DrupalFilter();
        filter.byUid = nodeId;
        conn.executeQuery(nodeDepartmentObjectClass, filter, rh, null);
    }

    @Test
    public void testSearchNodeByName() {
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // searchByUId
        DrupalFilter filter = new DrupalFilter();
        filter.byName = nodeName;
        conn.executeQuery(nodeDepartmentObjectClass, filter, rh, null);
    }

    @Test
    public void testSearchAllNodes() {
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // searchByUId
        DrupalFilter filter = new DrupalFilter();
        conn.executeQuery(nodeDepartmentObjectClass, filter, rh, null);
    }

    @Test
    public void testUpdateNode() {

        Uid uid = new Uid(nodeId);
        Set<Attribute> attributes = new HashSet<Attribute>();
        String newName = nodeName + "V2";// + (new Random()).nextInt();
        attributes.add(AttributeBuilder.build("title_field", newName));
//        attributes.add(AttributeBuilder.build(Name.NAME, randName));
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_NODE_STATUS, "1"));
        attributes.add(AttributeBuilder.build("field_image", "4513"));
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_NODE_BODY, newName + " body"));
        attributes.add(AttributeBuilder.build("field_icon", "4515"));

        Uid userUid = conn.update(nodeDepartmentObjectClass, uid, attributes, null);
        LOG.ok("Node {0} updated", userUid.getUidValue());
    }


    @Test
    public void testDeleteNode() {
        Uid uid = new Uid(nodeId);
        conn.delete(nodeDepartmentObjectClass, uid, null);
    }

    private static String termId = "677"; //test_node
    private static String termName = "test_node";
    private static final ObjectClass termCompanyStructureObjectClass = new ObjectClass(DrupalConnector.OC_TERM_Prefix + "company_structure");

    @Test
    public void testCreateTerm() {

        //create
        Set<Attribute> attributes = new HashSet();
        attributes.add(AttributeBuilder.build(Name.NAME, termName));
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_TAX_WEIGHT, "1"));
        attributes.add(AttributeBuilder.build("field_structure_department", "0"));
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_TAX_PARENT, "261")); //Account Management

        Uid nodeNid = conn.create(termCompanyStructureObjectClass, attributes, null);
        LOG.ok("New term TID is: {0}, name: {1}", nodeNid.getUidValue(), termName);
        TestClient.termId = nodeNid.getUidValue();
    }

    @Test
    public void testSearchTermByUid() {

        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // searchByUId
        DrupalFilter filter = new DrupalFilter();
        filter.byUid = termId;
        conn.executeQuery(termCompanyStructureObjectClass, filter, rh, null);
    }

    @Test
    public void testSearchTermByName() {
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // searchByUId
        DrupalFilter filter = new DrupalFilter();
        filter.byName = termName;
        conn.executeQuery(termCompanyStructureObjectClass, filter, rh, null);
    }

    @Test
    public void testSearchAllTerms() {
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                return true;
            }
        };

        // searchByUId
        DrupalFilter filter = new DrupalFilter();
        conn.executeQuery(termCompanyStructureObjectClass, filter, rh, null);
    }

    @Test
    public void testUpdateTerm() {

        Uid uid = new Uid(termId);
        Set<Attribute> attributes = new HashSet<Attribute>();
        String newName = termName + "V2";// + (new Random()).nextInt();
        attributes.add(AttributeBuilder.build(Name.NAME, newName));
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_TAX_WEIGHT, "-1")); // first, default is "0"
        attributes.add(AttributeBuilder.build("field_structure_department", "0"));
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_TAX_PARENT, "250")); // Generic Teams


        Uid userUid = conn.update(termCompanyStructureObjectClass, uid, attributes, null);
        LOG.ok("Term {0} updated", userUid.getUidValue());
    }

    @Test
    public void testUpdateToRootParentTerm() {

        Uid uid = new Uid(termId);
        Set<Attribute> attributes = new HashSet<Attribute>();
        String root = "0";
        attributes.add(AttributeBuilder.build(DrupalConnector.ATTR_TAX_PARENT, root)); // Generic Teams


        Uid userUid = conn.update(termCompanyStructureObjectClass, uid, attributes, null);
        LOG.ok("Term {0} updated", userUid.getUidValue());
    }

    @Test
    public void testDeleteTerm() {
        Uid uid = new Uid(termId);
        conn.delete(termCompanyStructureObjectClass, uid, null);
    }

    @Test
    public void testNodeCache() {
        conn.nodeCache.getIdOrCreate("department", "IT Services");
    }


    @Test
    public void testCreateFile() throws IOException {
        Path path = Paths.get("C:\\Users\\Public\\Pictures\\Sample Pictures\\Koala.jpg");
        byte[] image = Files.readAllBytes(path);

        String encodedBytes = Base64.encode(image);
        LOG.ok("encodedBytes " + encodedBytes);


//        JSONObject jo = new JSONObject();
//        jo.put("filename", "koala.jpg");
//        jo.put("status", "1");
//        jo.put("file", encodedBytes);
//        HttpPost request = new HttpPost(conf.getServiceAddress() + conn.FILE);
//        JSONObject jores = conn.callRequest(request, jo);
//
//        String newFid = jores.getString(conn.FID);
//        LOG.ok("new FID: " + newFid);


        HttpGet request = new HttpGet(conf.getServiceAddress() + conn.FILE + "/" + "4565");
        JSONObject jores = conn.callRequest(request, true);
        String fileContent = jores.getString("file");
        LOG.ok("new file content: " + fileContent);
        byte[] data = Base64.decode(fileContent);

        Path pathTo = Paths.get("C:\\Users\\Public\\Pictures\\Sample Pictures\\Koala2.jpg");
        Files.write(pathTo, data);
    }

    @Test
    public void testEncode() throws UnsupportedEncodingException {
        String encoded = URLEncoder.encode("value with spaces", "UTF-8");
        System.out.println("encoded = " + encoded);
    }


    @Test
    public void getNotExisting() throws IOException {
        HttpGet requestFind = new HttpGet(conn.getConfiguration().getServiceAddress() + conn.TAXONOMY_TERM + "-1");
        JSONArray entities = conn.callRequest(requestFind);
    }

    @Test
    public void delete4() throws IOException {
//        HttpDelete request = new HttpDelete(conn.getConfiguration().getServiceAddress() + conn.TAXONOMY_TERM + "/" + 317);
//        conn.execute(request);//.close();
//        HttpDelete request2 = new HttpDelete(conn.getConfiguration().getServiceAddress() + conn.TAXONOMY_TERM + "/" + 196);
//        conn.execute(request2);//.close();
//        HttpDelete request3 = new HttpDelete(conn.getConfiguration().getServiceAddress() + conn.TAXONOMY_TERM + "/" + 316);
//        conn.execute(request3);//.close();
//        HttpDelete request4 = new HttpDelete(conn.getConfiguration().getServiceAddress() + conn.TAXONOMY_TERM + "/" + 308);
//        conn.execute(request4);//.close();
        HttpDelete request = new HttpDelete(conn.getConfiguration().getServiceAddress() + conn.TAXONOMY_TERM + "/" + 317);
        conn.callRequest(request, false);//.close();
        HttpDelete request2 = new HttpDelete(conn.getConfiguration().getServiceAddress() + conn.TAXONOMY_TERM + "/" + 196);
        conn.callRequest(request2, false);//.close();
        HttpDelete request3 = new HttpDelete(conn.getConfiguration().getServiceAddress() + conn.TAXONOMY_TERM + "/" + 316);
        conn.callRequest(request3, false);//.close();
        HttpDelete request4 = new HttpDelete(conn.getConfiguration().getServiceAddress() + conn.TAXONOMY_TERM + "/" + 308);
        conn.callRequest(request4, false);//.close();

    }

//
//    @Test
//    public void ignoreCert() throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
//        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
//
//        final BasicCredentialsProvider httpClient = new BasicCredentialsProvider();
//        httpClient.setCredentials(new AuthScope("localhost", 4443), new UsernamePasswordCredentials("rest_user", new String("ThmrqLjVmL")));
//        httpClientBuilder.setDefaultCredentialsProvider(httpClient);
//
//
//        final SSLContext sslContext = new SSLContextBuilder()
//                .loadTrustMaterial(null, new org.apache.http.ssl.TrustStrategy() {
//                    @Override
//                    public boolean isTrusted(X509Certificate[] x509CertChain, String authType) throws CertificateException {
//                        return true;
//                    }
//                })
//                .build();
//
//        CloseableHttpClient client = httpClientBuilder
//                .setSSLContext(sslContext)
//                .setConnectionManager(
//                        new PoolingHttpClientConnectionManager(
//                                RegistryBuilder.<ConnectionSocketFactory>create()
//                                        .register("http", PlainConnectionSocketFactory.INSTANCE)
//                                        .register("https", new SSLConnectionSocketFactory(sslContext,
//                                                NoopHostnameVerifier.INSTANCE))
//                                        .build()
//                        ))
//                .build();
//
//
//        HttpGet req = new HttpGet("https://localhost:4443/rest/user/1");
//        HttpResponse resp = client.execute(req);
//        LOG.info("resp: {0}", resp);
//    }
}
