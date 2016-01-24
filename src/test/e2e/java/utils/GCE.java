package utils;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Lists;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.Zone;
import com.google.api.services.compute.model.ZoneList;

import java.io.IOException;
import java.util.List;

/**
 * Created by ppastuszka on 14.12.15.
 */
public class GCE {
    private final Compute compute;

    GCE() {
        try {
            JacksonFactory jaksonFactory = JacksonFactory.getDefaultInstance();
            NetHttpTransport httpTransport = newTrustedTransport();
            GoogleCredential credential = GoogleCredential.getApplicationDefault
                    (httpTransport, jaksonFactory).createScoped(BigqueryScopes.all());
            this.compute = new Compute.Builder(httpTransport, jaksonFactory, credential).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static GCE get() {
        return Holder.INSTANCE;
    }

    public List<Zone> listZones(String project) throws IOException {
        List<Zone> zones = Lists.newArrayList();
        String pageToken = null;

        do {
            ZoneList result = compute.zones().list(project).setPageToken(pageToken).execute();
            zones.addAll(result.getItems());
            pageToken = result.getNextPageToken();
        } while (pageToken != null);
        return zones;
    }

    public void stopInstance(Instance instance) throws IOException {
        String[] zoneNameParts = instance.getZone().split("/");
        String zone = zoneNameParts[zoneNameParts.length - 1];
        String project = zoneNameParts[zoneNameParts.length - 3];
        compute.instances().stop(project, zone, instance.getName()).execute();
    }

    public void startInstance(Instance instance) throws IOException {
        String[] zoneNameParts = instance.getZone().split("/");
        String zone = zoneNameParts[zoneNameParts.length - 1];
        String project = zoneNameParts[zoneNameParts.length - 3];
        compute.instances().start(project, zone, instance.getName()).execute();
    }

    public List<Instance> listInstances(String project) throws IOException {
        List<Instance> instances = Lists.newArrayList();
        for (Zone zone : listZones(project)) {
            String pageToken = null;
            do {
                InstanceList result = compute.instances().list(project, zone.getName())
                        .setPageToken(pageToken).execute();
                pageToken = result.getNextPageToken();
                if (result.getItems() != null) {
                    instances.addAll(result.getItems());
                }
            } while (pageToken != null);
        }
        return instances;
    }

    private static class Holder {
        private static final GCE INSTANCE = new GCE();
    }
}
