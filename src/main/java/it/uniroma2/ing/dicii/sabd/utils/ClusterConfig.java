package it.uniroma2.ing.dicii.sabd.utils;

import org.apache.commons.cli.MissingArgumentException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;

public class ClusterConfig {
    static Integer sparkDefaultPort = 7077;
    static Integer hdfsDefaultPort = 54310;

    String sparkHost;
    Integer sparkPort;

    String hdfsHost;
    Integer hdfsPort;

    public ClusterConfig(String sparkHost, Integer sparkPort, String hdfsHost, Integer hdfsPort) {
        this.sparkHost = sparkHost;
        this.sparkPort = sparkPort;
        this.hdfsHost = hdfsHost;
        this.hdfsPort = hdfsPort;
    }

    public ClusterConfig(String sparkHost, String hdfsHost, Integer hdfsPort) {
        this(sparkHost, sparkDefaultPort, hdfsHost, hdfsPort);
    }

    public ClusterConfig(String sparkHost, Integer sparkPort, String hdfsHost) {
        this(sparkHost, sparkPort, hdfsHost, hdfsDefaultPort);
    }

    public ClusterConfig(String sparkHost, String hdfsHost) {
        this(sparkHost, sparkDefaultPort, hdfsHost, hdfsDefaultPort);
    }

    public String getSparkURL() {
        return "spark://" + this.sparkHost + ":" + this.sparkPort;
    }

    public String getHdfsURL() {
        return "hdfs://" + this.hdfsHost + ":" + this.hdfsPort;
    }

    public static ClusterConfig ParseConfig(String filepath) throws ParseException, IOException, MissingArgumentException {
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(filepath));

        String sparkHost = (String) json.get("sparkHost");
        if (sparkHost == null) throw new MissingArgumentException("Missing sparkHost field");

        String hdfsHost = (String) json.get("hdfsHost");
        if (hdfsHost == null) throw new MissingArgumentException("Missing hdfsHost field");

        Integer sparkPort = (Integer) json.get("sparkPort");
        sparkPort = (sparkPort != null) ? sparkPort : sparkDefaultPort;

        Integer hdfsPort = (Integer) json.get("hdfsPort");
        hdfsPort = (hdfsPort != null) ? hdfsPort : hdfsDefaultPort;

        return new ClusterConfig(sparkHost, sparkPort, hdfsHost, hdfsPort);
    }

}
