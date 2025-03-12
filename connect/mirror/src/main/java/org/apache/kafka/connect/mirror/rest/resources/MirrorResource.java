package org.apache.kafka.connect.mirror.rest.resources;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.apache.kafka.connect.mirror.MirrorHerder;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestRequestTimeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MirrorResource {

    private static final Logger log = LoggerFactory.getLogger(MirrorResource.class);

    @Context
    private UriInfo uriInfo;

    private final Map<SourceAndTarget, MirrorHerder> herders;

    private final Map<String, Object> props;

    @Inject
    public MirrorResource(
            Map<SourceAndTarget, MirrorHerder> herders,
            RestClient restClient,
            RestRequestTimeout requestTimeout,
            Map<String, Object> props
    ) {
        this.props = props;
        this.herders = herders;
    }

    @GET
    @Operation(summary = "List all herders")
    public Response listConnectors(
            final @Context UriInfo uriInfo,
            final @Context HttpHeaders headers
    ) {
        Map<String, Map<String, Object>> out = new HashMap<>();

        HashMap<String, Object> clusters = new HashMap<>();
        HashMap<String, Object> flows = new HashMap<>();

        herders.forEach((st, h)-> {
            HashMap<String, Object> links = new HashMap<>();
            links.put("herder", "/"+st.source()+"/"+st.target());
            links.put("connectors", "/"+st.source()+"/"+st.target()+"/connectors");
            clusters.computeIfAbsent(st.source(), (id)->getBootstrapServersForCluster(id));
            clusters.computeIfAbsent(st.target(), (id)->getBootstrapServersForCluster(id));
            Map<String, Object> hd = h.herderDetails(st);
            hd.put("links", links);
            flows.put(st.source() + "->" + st.target(), hd);
        });

        out.put("clusters", clusters);
        out.put("flows", flows);

        return Response.ok(out).build();
    }

    private String getBootstrapServersForCluster(String id) {
        return (String) props.get(id+".bootstrap.servers");
    }
}
