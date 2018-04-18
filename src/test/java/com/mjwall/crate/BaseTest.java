package com.mjwall.crate;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.crate.testing.CrateTestCluster;
import io.crate.testing.CrateTestServer;
import io.crate.testing.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

// Copied from https://goo.gl/nnYFNP because it doesn't appear to packaged
// as an artifact anywhere.  Couple of NOTEs about what I changed below
public class BaseTest {

    static {
        try {
            Utils.deletePath(CrateTestCluster.TMP_WORKING_DIR);
        } catch (IOException ignored) {
        }
    }

    private static final int DEFAULT_TIMEOUT_MS = 10_000;

    // NOTE: made url protected so I could access it in subclass
    protected static URL url;

    protected static void prepare(CrateTestCluster crateCluster) throws MalformedURLException {
        CrateTestServer server = crateCluster.randomServer();
        url = new URL(String.format("http://%s:%d/_sql", server.crateHost(), server.httpPort()));
    }

    // NOTE: made this static so I could call from a static method on the subclass
    protected static JsonObject execute(String statement) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");

        String query = "{\"stmt\": \"" + statement + "\"}";
        byte[] body = query.getBytes("UTF-8");
        connection.setRequestProperty("Content-Type", "application/text");
        connection.setRequestProperty("Content-Length", String.valueOf(body.length));
        connection.setDoOutput(true);
        connection.getOutputStream().write(body);
        connection.setConnectTimeout(DEFAULT_TIMEOUT_MS);
        return parseResponse(connection.getInputStream());
    }

    private static JsonObject parseResponse(InputStream inputStream) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder res = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            res.append(line);
        }
        br.close();
        return new JsonParser().parse(res.toString()).getAsJsonObject();
    }
}
