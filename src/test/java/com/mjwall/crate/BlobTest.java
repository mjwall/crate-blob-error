package com.mjwall.crate;

import io.crate.testing.CrateTestCluster;
import io.crate.testing.CrateTestServer;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;

import static org.junit.Assert.assertEquals;

public class BlobTest extends BaseTest {

    public static String TABLENAME = "myblob";
    public static String SHA1_A = "6dcd4ce23d88e2ee9568ba546c007c63d9131c1b";
    public static String SHA1_B = "ae4f281df5a5d0ff3cad6371f76d5c29b6d953ec";
    public static String SHA1_C = "32096c2e0eff33d844ee6d675407ace18289357d";
    public static String SHA1_D = "50c9e8d5fc98727b4bbc93cf5d64a68db647f04f";

    private Executor executor = null;
    private String crateUrl = null;

    @ClassRule // wants to run on 4200
    public static final CrateTestCluster TEST_CLUSTER =
            CrateTestCluster.fromVersion("2.3.3")
                    .clusterName("with-builder")
                    .numberOfNodes(1)
                    .build();


    @Before
    public void setUp() {
        try {
            prepare(TEST_CLUSTER); // sets up BaseTest stuff so can run commands
            // also sets url, which was private in the original BaseTest but needs _sql stripped
            crateUrl = url.toString().replace("/_sql", "/");
            System.out.println("URL: " + crateUrl);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        executor = Executor.newInstance();

        // create blob table - ok to run again
        try {
            execute("create blob table " + TABLENAME);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Could not create table, stopping", e);
        }

        // put some blobs, returns 409 if already there
        insertBlob("A", SHA1_A);
        insertBlob("C", SHA1_C);
        insertBlob("D", SHA1_D);
    }

    @After
    public void tearDown() {
        try {
            execute("drop blob table " + TABLENAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
        executor = null;
        crateUrl = null;
    }

    @Test
    public void get200s()  {
        assertEquals("A", getBlobString(SHA1_A));
        assertEquals("C", getBlobString(SHA1_C));
        assertEquals("D", getBlobString(SHA1_D));
    }

    //@Test
    public void get404() {
        assertEquals(404, getBlob(SHA1_B).getStatusLine().getStatusCode());
    }



    //@Test
    public void get200After404() {

    }

    //@Test
    public void test2CallsAfter404() {

    }

    private void insertBlob(String digest, String content)  {
        Request request = Request.Put(crateUrl + "/_blobs/"+ TABLENAME + "/" + digest)
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

    private HttpResponse getBlob(String digest) {
        Request req = Request.Get(crateUrl + "/_blobs/"+ TABLENAME + "/" + digest);
        Response response = null;
        try {
            response = executor.execute(req);
            return response.returnResponse();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String getBlobString(String digest) {
        HttpResponse response = getBlob(digest);
        try {
            return EntityUtils.toString(response.getEntity(), "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
