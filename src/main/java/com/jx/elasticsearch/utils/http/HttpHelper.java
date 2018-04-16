package com.jx.elasticsearch.utils.http;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.ProtocolException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class HttpHelper {

    private static RequestConfig requestConfig = null;

    private static RedirectStrategy noRedirect = null;

    static {
        requestConfig = RequestConfig.custom().setConnectTimeout(1000 * 60 * 30).setConnectionRequestTimeout(1000 * 60 * 30).setSocketTimeout(1000 * 60 * 30).build();

        noRedirect = new RedirectStrategy() {
            public boolean isRedirected(org.apache.http.HttpRequest request, org.apache.http.HttpResponse response, HttpContext context) throws ProtocolException {
                return false;
            }

            public HttpUriRequest getRedirect(org.apache.http.HttpRequest request, org.apache.http.HttpResponse response, HttpContext context) throws ProtocolException {
                return null;
            }
        };
    }

    public static void setRequestConfig(RequestConfig conf) {
        requestConfig = conf;
    }

    public static String cookieToString(List<Cookie> cookies) {
        if (cookies == null || cookies.size() == 0) {
            return "";
        }

        String value = "";
        for (Cookie c : cookies) {
            if (value.length() > 0) {
                value += "; ";
            }

            value += c.getName() + "=" + c.getValue();
        }

        return value;
    }

    public static HttpResponse post(HttpRequest request) throws Exception {
        BasicCookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;
        HttpEntity responseEntity = null;
        HttpResponse httpResponse = new HttpResponse();

        try {
            httpClient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
            HttpPost post = new HttpPost(request.getUrl());
            post.setConfig(requestConfig);

            Map<String, String> headers = request.getHeaders();
            if (headers == null || headers.size() == 0) { // 默认ContentType.URLEncoded编码
                headers = new HashMap<>();
                headers.put("Content-Type", ContentType.URLEncoded.type());
            }

            if (headers.size() > 0) {
                Iterator<Entry<String, String>> itr = headers.entrySet().iterator();
                while (itr.hasNext()) {
                    Entry<String, String> ent = itr.next();
                    post.setHeader(ent.getKey(), ent.getValue());
                }
            }

            if (request.getData() != null) {
                ByteArrayEntity byteEntity = new ByteArrayEntity(request.getData());
                post.setEntity(byteEntity);
            }

            response = httpClient.execute(post);
            responseEntity = response.getEntity();

            Header[] resHeaders = response.getAllHeaders();
            Map<String, String> hmap = new HashMap<String, String>();
            for (Header h : resHeaders) {
                hmap.put(h.getName(), h.getValue());
            }

            httpResponse.setHeaders(hmap);
            httpResponse.setCookies(cookieStore.getCookies());
            httpResponse.setStatusCode(response.getStatusLine().getStatusCode());
            httpResponse.setData(EntityUtils.toByteArray(responseEntity));
            httpResponse.setUrl(request.getUrl());
            return httpResponse;
        } catch (Exception e) {
            throw e;
        } finally {
            if (responseEntity != null) {
                EntityUtils.consume(responseEntity);
            }

            if (response != null) {
                response.close();
            }

            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public static String get(String url) throws Exception {
        HttpRequest request = new HttpRequest();
        request.setUrl(url);
        HttpResponse response = get(request);
        return response.getContent();
    }

    public static HttpResponse get(HttpRequest request) throws Exception {
        BasicCookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;
        HttpEntity responseEntity = null;
        HttpResponse httpResponse = new HttpResponse();

        try {
            if (request.isEnableRedirects()) {
                httpClient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
            } else {
                httpClient = HttpClients.custom().setDefaultCookieStore(cookieStore).setRedirectStrategy(noRedirect)
                        .build();
            }

            HttpGet get = new HttpGet(request.getUrl());
            get.setConfig(requestConfig);

            if (request.getHeaders() != null && request.getHeaders().size() > 0) {
                Iterator<Entry<String, String>> itr = request.getHeaders().entrySet().iterator();
                while (itr.hasNext()) {
                    Entry<String, String> ent = itr.next();
                    get.setHeader(ent.getKey(), ent.getValue());
                }
            }

            response = httpClient.execute(get);
            responseEntity = response.getEntity();
            Header[] headers = response.getAllHeaders();
            Map<String, String> hmap = new HashMap<String, String>();
            for (Header h : headers) {
                hmap.put(h.getName(), h.getValue());
            }

            httpResponse.setHeaders(hmap);
            httpResponse.setCookies(cookieStore.getCookies());
            httpResponse.setStatusCode(response.getStatusLine().getStatusCode());
            httpResponse.setData(EntityUtils.toByteArray(responseEntity));
            httpResponse.setUrl(request.getUrl());
            return httpResponse;
        } catch (Exception e) {
            throw e;
        } finally {
            if (responseEntity != null) {
                EntityUtils.consume(responseEntity);
            }

            if (response != null) {
                response.close();
            }

            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

}
