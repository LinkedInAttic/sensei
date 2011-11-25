package com.sensei.search.req.json;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.search.req.json.domain.SenseiRequest;
import com.sensei.search.res.json.domain.SenseiResult;

public class SenseiServiceProxy {
    public SenseiResult sendRequest(String urlStr, SenseiRequest request) throws IOException, JSONException {
        HttpURLConnection conn = null;
        try {
        URL url = new URL(urlStr);
         conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
     
        conn.setRequestProperty("Accept-Encoding", "gzip");
        String string = JsonSerializer.serialize(request).toString();
        byte[] requestBytes = string.getBytes("UTF-8");
        conn.setRequestProperty("Content-Length", String.valueOf(requestBytes.length));
        //GZIPOutputStream zippedOutputStream = new GZIPOutputStream(conn.getOutputStream());
        OutputStream os = new BufferedOutputStream( conn.getOutputStream());
        os.write(requestBytes);
        os.flush();
        os.close();
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            throw new RuntimeException("Failed : HTTP error code : "
                + conn.getResponseCode());
        }
        byte[] bytes = drain(new GZIPInputStream(new BufferedInputStream( conn.getInputStream())));
       
 
        String output = new String(bytes, "UTF-8");
        System.out.println("Output from Server = " + output);
        return JsonDeserializer.deserialize(SenseiResult.class, jsonResponse(output));
        } finally {
            if (conn != null) conn.disconnect();
        }
    }
    private JSONObject jsonResponse(String output) throws JSONException {
        return new JSONObject(output);
    }
    byte[] drain(InputStream inputStream) throws IOException {
        try {
        byte[] buf = new byte[1024];
        int len;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                while ((len = inputStream.read(buf)) > 0) {
                    byteArrayOutputStream.write(buf, 0, len);
                    }      
        return byteArrayOutputStream.toByteArray();
        } finally {
            inputStream.close();
        }
    }
}
