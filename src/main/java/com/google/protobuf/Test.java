package com.google.protobuf;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.xerial.snappy.Snappy;
import prometheus.Remote;
import prometheus.Types;

import java.io.BufferedReader;
import java.io.UnsupportedEncodingException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    private Types.TimeSeries.Builder timeSeriesBuilder = Types.TimeSeries.newBuilder();
    private Types.Sample.Builder sampleBuilder = Types.Sample.newBuilder();

    private Remote.WriteRequest.Builder writeRequestBuilder = Remote.WriteRequest.newBuilder();
    private final CloseableHttpClient httpClient = HttpClients.createSystem();

    public static void main(String[] args) {
        Test test = new Test();
        String url = "http://10.68.248.115:9090/api/v1/write";
        String filePath = "metricsInput.txt";
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime time = now.minusHours(10);
        long timestamp = time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        try {
            List<String> lines = test.readFile(filePath);
            for (String jsonLine: lines) {
               if (jsonLine.startsWith("#")) {
                   continue;
               }else  {
                    String[] parts = jsonLine.split(" ",2);
                    if (parts.length!=2) {
                        System.err.println("解析失败");
                        return;
                    }
                   String metricNameWithLabels = parts[0];
                   String value = parts[1];

                   // 如果需要进一步解析指标名称和标签，可以使用以下代码：
                   String metricName = metricNameWithLabels.split("\\{")[0]; // 假设指标名称在第一个括号之前
//                   String labelPart = metricNameWithLabels.substring(metricName.length());

                   Map<String,Object> map = new HashMap<>();
                   map.put("__name__",metricName);


                   Pattern pattern = Pattern.compile("\\{([^=]+)=\"([^\"]+)\"}");
                   Matcher matcher = pattern.matcher(metricNameWithLabels);
                   while (matcher.find()) {
                       String labelName = matcher.group(1);
                       String labelValue = matcher.group(2);
                       map.put(labelName, labelValue);
                   }
                   map.put("value",value);
                   List<Types.TimeSeries> testJob = test.createTimeSeries(map, "10.0.0.1:9100", "testJob", timestamp);
                   test.write(testJob,url);
               }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // timeseries
//    private void remoteWrite(Remote.WriteRequest.Builder writeRequestBuilder) throws IOException {
//        Remote.WriteRequest writeRequest = writeRequestBuilder.build();
//        byte[] compressed = Snappy.compress(writeRequest.toByteArray());
//        String url = "http://10.68.248.115:9090/api/v1/write";
//
//        HttpPost httpPost = new HttpPost(url);
//
//        httpPost.setHeader("Content-type", "application/x-protobuf");
//        httpPost.setHeader("Content-Encoding", "snappy");
//        httpPost.setHeader("X-Prometheus-Remote-Write-Version", "0.1.0");
//
//        ByteArrayEntity byteArrayEntity = new ByteArrayEntity(compressed);
//        httpPost.getRequestLine();
//        httpPost.setEntity(byteArrayEntity);
//        CloseableHttpClient httpClient = HttpClients.createSystem();
//        for (int i = 0; i < 3; i++) {
//            try {
//                CloseableHttpResponse response = httpClient.execute(httpPost);
//                System.out.println("远程写入prometheus数据结果" + response);
//                break;
//            } catch (Exception e) {
//                System.err.println("error");
//            }
//        }
//    }
    public List<Types.TimeSeries> createTimeSeries(Map<String, Object> map, String instance, String job, long timestamp) {
        List<Types.TimeSeries> timeSeriesList = new ArrayList<>();
        timeSeriesBuilder.clear();
        Types.Label instanceLabel = Types.Label.newBuilder().setName("instance").setValue(instance).build();
        Types.Label jobLabel = Types.Label.newBuilder().setName("job").setValue(job).build();

        timeSeriesBuilder.addLabels(instanceLabel);
        timeSeriesBuilder.addLabels(jobLabel);

        for (Map.Entry<String, Object> entry : map.entrySet()) {
//            timeSeriesBuilder.clear();
            sampleBuilder.clear();


            String name = entry.getKey().replace(".","_");
            String value = entry.getValue().toString();
            if (name .equals("value")) {
                sampleBuilder.setValue(Double.parseDouble(value));
                sampleBuilder.setTimestamp(timestamp);
                timeSeriesBuilder.addSamples(sampleBuilder.build());
            }else {
                Types.Label metricNameLabel = Types.Label.newBuilder().setName(name).setValue(value).build();
                timeSeriesBuilder.addLabels(metricNameLabel);
            }

//            Types.Label instanceLabel = Types.Label.newBuilder().setName("instance").setValue(instance).build();
//            Types.Label jobLabel = Types.Label.newBuilder().setName("job").setValue(job).build();
//
//            timeSeriesBuilder.addLabels(instanceLabel);
//            timeSeriesBuilder.addLabels(jobLabel);

//            Types.Label appLabel3 = Types.Label.newBuilder().setName("env").setValue(env).build();
//            timeSeriesBuilder.addLabels(appLabel3);


        }
        timeSeriesList.add(timeSeriesBuilder.build());
        System.out.println(timeSeriesList);
        return timeSeriesList;
    }


    public void write( List<Types.TimeSeries> timeSeriesList, String url ) throws Exception{
        try{
            writeRequestBuilder.clear();
            Remote.WriteRequest writeRequest= writeRequestBuilder.addAllTimeseries(timeSeriesList).build();
            byte[] compressed = Snappy.compress(writeRequest.toByteArray());
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader("Content-type","application/x-www-form-urlencoded");
            httpPost.setHeader("Content-Encoding", "snappy");
            httpPost.setHeader("X-Prometheus-Remote-Write-Version", "0.1.0");

            ByteArrayEntity byteArrayEntity = new ByteArrayEntity(compressed);

            httpPost.getRequestLine();
            httpPost.setEntity(byteArrayEntity);
            httpClient.execute(httpPost);
        }catch(UnsupportedEncodingException uee){
            throw uee;
        }catch (IOException ioe){
            throw ioe;
        }catch (Exception ex) {
            throw ex;
        }
    }
    public List<String> readFile(String filePath ) throws Exception{
        List<String> lines = new ArrayList<String>();
        java.io.FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        try {
            fileReader = new java.io.FileReader(filePath);
            bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                lines.add(line);
            }
        }catch (Exception ex){
            throw ex;
        }finally {
            if(bufferedReader != null) bufferedReader.close();
            if(fileReader != null) fileReader.close();
        }
        return lines;
    }

}
