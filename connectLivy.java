package sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

public class connectLivy {

        public static void main(String[] args) {

          try {

                DefaultHttpClient httpClient = new DefaultHttpClient();
                HttpPost postRequest = new HttpPost(
                        "http://livy-server/batches");

                StringEntity input = new StringEntity("{\"file\" : \"###.jar\" ,\"driverCores\" : 4,\"driverMemory\" : \"4G\",\"executorCores\" : 4,\"executorMemory\" : \"4G\",\"conf\" : {    \"spark.rdd.compress\" : \"true\",      \"spark.io.compression.codec\" : \"lz4\",       \"spark.sql.tungsten.enabled\" : \"false\",     \"spark.kryoserializer.buffer.max\" : \"1536M\",        \"spark.local.dir\" : \"tmp\"}, \"files\": [\"###.properties\"], \"args\": [\"####\"],\"className\" : \"###\" }");
                input.setContentType("application/json");
                postRequest.setEntity(input);

                HttpResponse response = httpClient.execute(postRequest);

                if (response.getStatusLine().getStatusCode() != 201) {
                        throw new RuntimeException("Failed : HTTP error code : "
                                + response.getStatusLine().getStatusCode());
                }

                BufferedReader br = new BufferedReader(
                        new InputStreamReader((response.getEntity().getContent())));

                String output;
                System.out.println("Output from Server .... \n");
                while ((output = br.readLine()) != null) {
                        System.out.println(output);
                }

                httpClient.getConnectionManager().shutdown();

          } catch (MalformedURLException e) {

                e.printStackTrace();

          } catch (IOException e) {

                e.printStackTrace();

          }

        }

}