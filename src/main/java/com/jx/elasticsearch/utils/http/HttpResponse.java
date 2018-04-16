package com.jx.elasticsearch.utils.http;

import org.apache.http.cookie.Cookie;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

public class HttpResponse {

    private List<Cookie> cookies;

    private byte[] data;

    private Map<String, String> headers;

    private int statusCode;

    private String url;

    /**
     * 获得指定的http头
     *
     * @param key
     * @return
     */
    public String getHeader(String key) {
        if (headers == null || headers.size() == 0) {
            return null;
        }

        return headers.get(key);
    }

    public String getContent() throws UnsupportedEncodingException {
        return getContent("UTF-8");
    }

    public String getContent(String encode) throws UnsupportedEncodingException {
        return new String(data, encode);
    }

    public List<Cookie> getCookies() {
        return cookies;
    }

    public void setCookies(List<Cookie> cookies) {
        this.cookies = cookies;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
