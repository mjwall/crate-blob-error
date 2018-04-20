package com.mjwall.crate;

import io.crate.testing.CrateTestCluster;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BlobTest extends BaseTest {

    public static String CRATE_VERSION = "3.0.0-SNAPSHOT";
    public static String TABLENAME = "myblob";

    // pre computed sha1 values for string A, B, C and D
    public static String SHA1_A = "6dcd4ce23d88e2ee9568ba546c007c63d9131c1b";
    public static String SHA1_B = "ae4f281df5a5d0ff3cad6371f76d5c29b6d953ec";
    public static String SHA1_C = "32096c2e0eff33d844ee6d675407ace18289357d";
    public static String SHA1_D = "50c9e8d5fc98727b4bbc93cf5d64a68db647f04f";

    private static String URL;
    private static BlobClient client;

    @ClassRule
    public static CrateTestCluster TEST_CLUSTER = CrateTestCluster.fromVersion(CRATE_VERSION)
            .clusterName("with-builder")
            .numberOfNodes(1)
            .build();

    @BeforeClass
    public static void setUpStatic() {
        try {
            // sets up BaseTest and provides url.  See comments on that class for more info
            prepare(TEST_CLUSTER);
            URL = url.toString().replace("/_sql", "/");
        } catch (MalformedURLException e) {
            throw new RuntimeException("Unable to start crate, stopping", e);
        }

        BlobClient staticClient = new BlobClient(URL, TABLENAME); // this client only used to insert data
        staticClient.createBlobTable();
        staticClient.insertBlob(SHA1_A, "A");
        staticClient.insertBlob(SHA1_C, "C");
        staticClient.insertBlob(SHA1_D, "D");

        // initialize the client the tests will use
        client = new BlobClient(URL, TABLENAME);
    }

    @After
    public void tearDown() {
        // keep responses from leaking between tests, otherwise a 404 in one test will affect another test
        client.executor.closeIdleConnections();
    }

    @Test
    // this test passes and shows you the data was inserted
    public void get200s()  {
        assertEquals("A", client.getBlobString(SHA1_A));
        assertEquals("C", client.getBlobString(SHA1_C));
        assertEquals("D", client.getBlobString(SHA1_D));
    }

    @Test
    // this test passes and shows B was not inserted
    public void get404() {
        assertEquals(404, client.getBlob(SHA1_B).getStatusLine().getStatusCode());
    }

    @Test
    // this test fails and shows the bug, the second call also returning a 404
    public void get200After404() throws IOException {
        assertEquals(404, client.getBlob(SHA1_B).getStatusLine().getStatusCode());
        HttpResponse responseA = client.getBlob(SHA1_A);
        assertEquals(200, responseA.getStatusLine().getStatusCode());
        assertEquals("A", EntityUtils.toString(responseA.getEntity(), "UTF-8")); // doesn't get here
    }

    @Test
    // this also fails and shows the bug perists after several calls
    // the 3rd call returns A which was the 2nd calls response
    // this continues, the 4th call returns the 3rd calls response, the 5th returns the 4th and so on
    public void test2CallsAfter404() {
        assertEquals(404, client.getBlob(SHA1_B).getStatusLine().getStatusCode());
        client.getBlobString(SHA1_A); // not asserting because it will be a 404
        assertEquals("C", client.getBlobString(SHA1_C));
    }

    @Test
    // this fails and shows that every 404 make the bug perists after several calls
    // the 3rd call returns A which was the 2nd calls response
    // this continues, the 4th call returns the 3rd calls response, the 5th returns the 4th and so on
    public void testSeveralCallsAfterMultiple404() {
        assertEquals(404, client.getBlob(SHA1_B).getStatusLine().getStatusCode());
        assertEquals("A", client.getBlobString(SHA1_A));
        assertEquals(404, client.getBlob(SHA1_B).getStatusLine().getStatusCode());
        assertEquals("A", client.getBlobString(SHA1_A));
        assertEquals("C", client.getBlobString(SHA1_C));
        assertEquals("D", client.getBlobString(SHA1_D));
    }

    @Test
    // this passes and shows how I am working around the issue,
    // has the same affect as the tearDown, it closes the open connection which clears out for the next response
    public void get200After404WithWorkAround() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Connection", "close");
        assertEquals(404, client.getBlob(SHA1_B, headers).getStatusLine().getStatusCode());
        assertEquals("A", client.getBlobString(SHA1_A));
    }

    // just a sample client, included in the test for easy of reading
    static class BlobClient {

        final private Executor executor;
        final private String url;
        final private String tableName;


        public BlobClient(String url, String tableName) {
            this.url = url;
            this.tableName = tableName;
            this.executor = Executor.newInstance();
        }

        public HttpResponse getBlob(String digest, Map<String, String> headers) {
            Request req = Request.Get(this.url + "/_blobs/"+ this.tableName + "/" + digest);
            for(Map.Entry<String, String> header : headers.entrySet()) {
                req.addHeader(header.getKey(), header.getValue());
            }
            HttpResponse httpResponse = null;
            try {
                httpResponse = executor.execute(req).returnResponse();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return httpResponse;
        }

        public HttpResponse getBlob(String digest) {
            return getBlob(digest, Collections.EMPTY_MAP);
        }

        public String getBlobString(String digest) {
            HttpResponse response = getBlob(digest);
            try {
                return EntityUtils.toString(response.getEntity(), "UTF-8");
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        public void insertBlob(String digest, String content)  {
            Request request = Request.Put(this.url + "_blobs/"+ this.tableName + "/" + digest)
                    .bodyString(content, ContentType.TEXT_PLAIN);
            try {
                Response response = executor.execute(request);
                int code = response.returnResponse().getStatusLine().getStatusCode();
                if (code != 201) {
                    throw new RuntimeException("Response code " + code + " inserting " + content +
                            " with digest of " + digest);
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Could not insert " + content + " with digest of " + digest, e);
            }
        }

        public void createBlobTable() {
            Request request = Request.Post(this.url + "_sql?pretty")
                    .addHeader("Content-Type", "application/json")
                    .bodyString("{\"stmt\":\"create blob table " + this.tableName + "\"}", ContentType.TEXT_PLAIN);
            try {
                Response response = executor.execute(request);
                int code = response.returnResponse().getStatusLine().getStatusCode();
                if (code != 200) {
                    throw new RuntimeException("Response code " + code + " creating table " + this.tableName);
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Could not create table " + this.tableName, e);
            }
        }
    }
}
