package it.uniroma2.ing.dicii.sabd.utils;

import org.apache.commons.cli.MissingArgumentException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;

public class ClusterConfig {
    static Integer hdfsDefaultPort = 54310;


    String hdfsHost;
    Integer hdfsPort;

    public ClusterConfig(String hdfsHost, Integer hdfsPort) {
        this.hdfsHost = hdfsHost;
        this.hdfsPort = hdfsPort;
    }


    public ClusterConfig(String hdfsHost) {
        this(hdfsHost, hdfsDefaultPort);
    }


    public String getHdfsURL() {
        return "hdfs://" + this.hdfsHost + ":" + this.hdfsPort;
    }

    public static ClusterConfig ParseConfig(String filepath) throws ParseException, IOException, MissingArgumentException {
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(filepath));


        String hdfsHost = (String) json.get("hdfsHost");
        if (hdfsHost == null) throw new MissingArgumentException("Missing hdfsHost field");


        Integer hdfsPort = (Integer) json.get("hdfsPort");
        hdfsPort = (hdfsPort != null) ? hdfsPort : hdfsDefaultPort;

        return new ClusterConfig(hdfsHost, hdfsPort);
    }

}
