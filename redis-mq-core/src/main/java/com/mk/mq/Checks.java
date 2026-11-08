package com.mk.mq;

import java.time.Duration;
import java.util.regex.Pattern;

final class Checks {
    private static final Pattern NAME = Pattern.compile("[a-zA-Z0-9._:-]{1,160}");
    private Checks() { }
    static String name(String value, String label) {
        if (value == null || !NAME.matcher(value).matches()) {
            throw new IllegalArgumentException(label + "只能包含字母、数字、点、下划线、冒号、横线，长度为 1 至 160");
        }
        return value;
    }
    static String queue(String value) {
        name(value, "队列名称");
        if (value.indexOf(':') >= 0) throw new IllegalArgumentException("队列名称不能包含冒号，以避免命名空间与队列名称组合冲突");
        return value;
    }
    static long millis(Duration value, boolean zero, String label) {
        if (value == null) throw new IllegalArgumentException(label + "不能为空");
        long result;
        try { result = value.toMillis(); }
        catch (ArithmeticException e) { throw new IllegalArgumentException(label + "超出支持范围", e); }
        if (value.getNano() % 1000000 != 0 || value.isNegative() || result < (zero ? 0 : 1) || result > 315360000000L) {
            throw new IllegalArgumentException(label + "必须为毫秒精度，且在支持范围内（最长十年）");
        }
        return result;
    }
}
