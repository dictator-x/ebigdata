package com.ebigdata.hadoop.hdfs;

public class WordCountMapper implements Mapper {

    public void map(String line, MapContext context) {
        String[] words = line.split(" ");

        for (String word : words) {
            Object value = context.get(word);
            if ( value == null ) {
                context.write(word, 1);
            } else {
                int v = Integer.parseInt(value.toString());
                context.write(word, v+1);
            }
        }
    }
}
