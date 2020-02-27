package com.ebigdata.hadoop.mr.project.utils;

import org.junit.Test;

public class IPTest {

    @Test
    public void testIp() {

        IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("123.116.60.97");

        System.out.println(regionInfo.getCountry());
        System.out.println(regionInfo.getProvince());
        System.out.println(regionInfo.getCity());
    }
}
