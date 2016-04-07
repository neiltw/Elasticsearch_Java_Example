package DBConnect;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Neil on 4/6/16.
 */
public class ESConnect {

    private Settings settings;
    private Client client;

    public Client getClient() { return client; }

    public void CloseClient(){ System.out.println("Close elasticsearch database........"); client.close();}

    /**
     * Connect ES Server
     * @param clusterName
     * @param host
     * **/
    public void EsConnect(String clusterName , String host) {
        try {
            settings = Settings.settingsBuilder()
                    .put("cluster.name", clusterName)
                    .put("client.transport.ping_timeout", "30s") // default 5s
                    .put("client.transport.sniff", false).build();
            System.out.println("Connect elasticsearch database..........");
            client = TransportClient
                    .builder()
                    .settings(settings)
                    .build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), 9300));
        } catch (UnknownHostException e) {
            System.err.println("Connect elasticsearch error ");
            e.printStackTrace();
        }
    }
}
