package org.apache.kafka.connect.mirror.rest.resources;

import com.fasterxml.jackson.core.type.TypeReference;
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
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.mirror.MirrorHerder;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.HerderRequestHandler;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestRequestTimeout;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@Path("/{source}/{target}")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MirrorHerderResource {

    private static final Logger log = LoggerFactory.getLogger(MirrorHerderResource.class);

    @Context
    private UriInfo uriInfo;

    private final Map<SourceAndTarget, MirrorHerder> herders;

    private final HerderRequestHandler requestHandler;

    @Inject
    public MirrorHerderResource(
            Map<SourceAndTarget, MirrorHerder> herders,
            RestClient restClient,
            RestRequestTimeout requestTimeout
    ) {
        this.requestHandler = new HerderRequestHandler(restClient, requestTimeout);
        this.herders = herders;
    }

    protected MirrorHerder herderForRequest() {
        String source = pathParam("source");
        String target = pathParam("target");
        MirrorHerder result = herders.get(new SourceAndTarget(source, target));
        if (result == null) {
            throw new jakarta.ws.rs.NotFoundException("No replication flow found for source '" + source + "' and target '" + target + "'");
        }
        return result;
    }

    @GET
    @Operation(summary = "Describe herder")
    public Response describeHerder(
            final @Context UriInfo uriInfo,
            final @Context HttpHeaders headers
    ) throws Throwable {
        MirrorHerder herder = herderForRequest();


        FutureCallback<Map<String, Object>> cb = new FutureCallback<>();
        herder.getHerderStat(cb);
        String forwardingPath = "/"+pathParam("source")+"/" + pathParam("target");
        Map<String, Object> replicatedTopics = requestHandler.completeOrForwardRequest(cb, forwardingPath, "GET", headers, null, new TypeReference<Map<String, Object>>() {}, true);

        return Response.ok(replicatedTopics).build();
    }

    @GET
    @Path("/connectors")
    @Operation(summary = "List all active connectors")
    public Response listConnectors(
            final @Context UriInfo uriInfo,
            final @Context HttpHeaders headers
    ) {
        Herder herder = herderForRequest();
        if (uriInfo.getQueryParameters().containsKey("expand")) {
            Map<String, Map<String, Object>> out = new HashMap<>();
            for (String connector : herder.connectors()) {
                try {
                    Map<String, Object> connectorExpansions = new HashMap<>();
                    for (String expansion : uriInfo.getQueryParameters().get("expand")) {
                        switch (expansion) {
                            case "status":
                                connectorExpansions.put("status", herder.connectorStatus(connector));
                                break;
                            case "info":
                                connectorExpansions.put("info", herder.connectorInfo(connector));
                                break;
                            default:
                                log.info("Ignoring unknown expansion type {}", expansion);
                        }
                    }
                    out.put(connector, connectorExpansions);
                } catch (NotFoundException e) {
                    // this likely means that a connector has been removed while we look its info up
                    // we can just not include this connector in the return entity
                    log.debug("Unable to get connector info for {} on this worker", connector);
                }

            }
            return Response.ok(out).build();
        } else {
            return Response.ok(herder.connectors()).build();
        }
    }

    private String pathParam(String name) {
        String result = uriInfo.getPathParameters().getFirst(name);
        if (result == null)
            throw new jakarta.ws.rs.NotFoundException("Could not parse " + name + " cluster from request path");
        return result;
    }


}
