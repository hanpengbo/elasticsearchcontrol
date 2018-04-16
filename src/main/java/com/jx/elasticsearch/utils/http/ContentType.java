package com.jx.elasticsearch.utils.http;

public enum ContentType {

    URLEncoded("application/x-www-form-urlencoded"),

    MultipartData("multipart/form-data"),

    JSON("application/json"),

    XML("text/xml");


    private String type;

    private ContentType(String type) {
        this.type = type;
    }

    public String type() {
        return this.type;
    }
}
