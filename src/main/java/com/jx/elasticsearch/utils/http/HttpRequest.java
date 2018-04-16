package com.jx.elasticsearch.utils.http;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HttpRequest {

    private String url;

    private Map<String, String> headers = new ConcurrentHashMap<String, String>();

    private byte[] data;

    private String content;

    private String encode = "UTF-8";

    private boolean enableRedirects = true;


    public HttpRequest() {
        initHeader();
    }

    public HttpRequest(String url) {
        initHeader();
        this.url = url;
    }

    private void initHeader() {
        this.addHeader("Connection", "keep-alive");
        this.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0");
        this.addHeader("Accept-Encoding", "gzip, deflate");
        this.addHeader("Accept", "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2");
    }

    public void cleanHeader() {
        headers.clear();
    }

    public void addHeader(String key, String value) {
        headers.put(key, value);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getContent() throws UnsupportedEncodingException {
        if (content != null) {
            return content;
        } else {
            return new String(data, encode);
        }
    }

    public void setContent(String content) throws UnsupportedEncodingException {
        this.content = content;
        this.data = content.getBytes(encode);
    }

    public String getEncode() {
        return encode;
    }

    public void setEncode(String encode) {
        this.encode = encode;
    }

    public boolean isEnableRedirects() {
        return enableRedirects;
    }

    public void setEnableRedirects(boolean enableRedirects) {
        this.enableRedirects = enableRedirects;
    }
}
