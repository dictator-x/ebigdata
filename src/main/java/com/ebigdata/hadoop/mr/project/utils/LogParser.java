package com.ebigdata.hadoop.mr.project.utils;

import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.HashMap;

public class LogParser {

    public Map<String, String> parse(String log) {

        Map<String, String> info = new HashMap<String, String>();

        IPParser ipParser = IPParser.getInstance();

        if ( StringUtils.isNotBlank(log) ) {
            String[] splits = log.split("\001");

            String ip = splits[13];
            String province = "-";
            String country = "-";
            String city = "-";
            IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);

            if ( regionInfo != null ) {
                country = regionInfo.getCountry();
                province = regionInfo.getProvince();
                city = regionInfo.getCity();
            }

            info.put("ip", ip);
            info.put("country", country);
            info.put("province", province);
            info.put("city", city);

            String url = splits[1];
            info.put("url", url);
        }

        return info;
    }
}
