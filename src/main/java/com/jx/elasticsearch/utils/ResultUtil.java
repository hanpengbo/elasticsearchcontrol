package com.jx.elasticsearch.utils;

public class ResultUtil {

    /**
     * 请求成功返回
     * @param data 返回的数据
     * @return
     */
    public static <T> JSONResult<T> success(T data) {
        JSONResult<T> result = new JSONResult<>();
        result.setCode(200);
        result.setMsg("");
        result.setData(data);

        return result;
    }

    /**
     * 成功
     * @return
     */
    public static JSONResult success() {
        return success(null);
    }

    /**
     * 异常
     * @param code 错误码
     * @param msg 错误消息
     * @return
     */
    public static JSONResult error(int code, String msg) {
        JSONResult result = new JSONResult();
        result.setCode(code);
        result.setMsg(msg);

        return result;
    }
}
