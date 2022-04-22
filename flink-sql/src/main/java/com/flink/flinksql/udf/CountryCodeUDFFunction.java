package com.flink.flinksql.udf;

import com.google.common.collect.Maps;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class CountryCodeUDFFunction extends ScalarFunction {
    private static Map<String,String> countryCodes = Maps.newHashMap();
    static {
        File file = new File("data/country_codes");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line = null;
            // 一次读入一行，直到读入null为文件结束
            while ((line = reader.readLine()) != null) {
                // 显示行号
                String[] fileds = line.split("\t");
                countryCodes.put(fileds[0],fileds[1] + "-" + fileds[2]);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    public String eval(String code){
        return countryCodes.get(code);
    }
}
