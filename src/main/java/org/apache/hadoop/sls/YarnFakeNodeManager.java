package org.apache.hadoop.sls;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.nodemanager.webapp.ContainerShellWebSocketServlet;
import org.apache.hadoop.yarn.server.nodemanager.webapp.TerminalServlet;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * fake NodeManager
 */
public class YarnFakeNodeManager implements ContainerManagementProtocol {


    private static final Logger LOG = LoggerFactory.getLogger(YarnFakeNodeManager.class);
    private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

    final private String containerManagerAddress;
    final private String nodeHttpAddress;
    final private String rackName;
    final private NodeId nodeId;
    final private Resource capability;
    Resource available = recordFactory.newRecordInstance(Resource.class);
    Resource used = recordFactory.newRecordInstance(Resource.class);

    final ResourceTracker resourceTracker;
    final Map<ApplicationId, List<Container>> containers = new HashMap<ApplicationId, List<Container>>();

    final Map<Container, ContainerStatus> containerStatusMap = new HashMap<Container, ContainerStatus>();


    private int responseID = 0;

    private final MasterKey nmTokenMasterKey;

    private long tokenSequenceNo;

    private ResourceUtilization nodeUtilization = null;

    private WebApp webApp;

    private final ResourceUtilization containersUtilization = ResourceUtilization.newInstance(0, 0, 0.0f);

    public YarnFakeNodeManager(String hostName, int containerManagerPort, int httpPort,
                               String rackName, Resource capability, YarnConfiguration config) throws IOException, YarnException {
        this.containerManagerAddress = hostName + ":" + containerManagerPort;
        this.nodeHttpAddress = hostName + ":" + httpPort;
        this.rackName = rackName;
        this.resourceTracker = ServerRMProxy.createRMProxy(config, ResourceTracker.class);
        this.capability = capability;
        Resources.addTo(available, capability);
        this.nodeId = NodeId.newInstance(hostName, containerManagerPort);
        Map<String, Float> customResources = new HashMap<>();
        customResources.put("yarn.io/gpu", 0f);
        nodeUtilization = ResourceUtilization.newInstance(0, 0, 0f, customResources);
        RegisterNodeManagerRequest request = recordFactory
                .newRecordInstance(RegisterNodeManagerRequest.class);
        request.setHttpPort(httpPort);
        request.setResource(capability);
        request.setPhysicalResource(capability);
        request.setNodeId(this.nodeId);
        request.setNMVersion(YarnVersionInfo.getVersion());
        LOG.info("begin register NodeManager {}", nodeId);
        RegisterNodeManagerResponse response = resourceTracker.registerNodeManager(request);
        nmTokenMasterKey = response.getNMTokenMasterKey();
        LOG.info("Register NodeManager {} success", nodeId);
        initRpcServer(config, containerManagerPort, hostName);
        // initHttpServer(config, httpPort, hostName);
    }

    private void initHttpServer(YarnConfiguration config, int port, String hostName) {
        String bindAddress = hostName + ":" + port;
        Map<String, String> params = new HashMap<String, String>();
        Map<String, String> terminalParams = new HashMap<String, String>();
        terminalParams.put("resourceBase", WebServer.class.getClassLoader().getResource("TERMINAL").toExternalForm());
        terminalParams.put("dirAllowed", "false");
        terminalParams.put("pathInfoOnly", "true");
        Context nmContext = new NodeManager.NMContext(null, null, null, null,
                null, false, config);
        ResourceView resourceView = new FakeResourceView();
        NodeHealthCheckerService healthChecker = createNodeHealthCheckerService();
        healthChecker.init(config);
        LocalDirsHandlerService dirsHandler = healthChecker.getDiskHandler();
                WebServer.NMWebApp nmWebApp = new WebServer.NMWebApp(resourceView, new ApplicationACLsManager(config), dirsHandler);
        webApp = WebApps.$for("node", Context.class, nmContext, "ws")
                .at(bindAddress)
                .withServlet("ContainerShellWebSocket", "/container/*",
                        ContainerShellWebSocketServlet.class, params, false)
                .withServlet("Terminal", "/terminal/*",
                        TerminalServlet.class, terminalParams, false)
                .with(config)
                .withHttpSpnegoPrincipalKey(
                        YarnConfiguration.NM_WEBAPP_SPNEGO_USER_NAME_KEY)
                .withHttpSpnegoKeytabKey(
                        YarnConfiguration.NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
                .start(nmWebApp);
    }

    private NodeHealthCheckerService createNodeHealthCheckerService() {
        LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
        return new NodeHealthCheckerService(dirsHandler);
    }

    private void initRpcServer(YarnConfiguration config, int port, String hostName) {
        YarnRPC rpc = YarnRPC.create(config);
        InetSocketAddress addr = NetUtils.createSocketAddr(hostName + ":" + port);
        NMTokenSecretManagerInNM tokenSecretManager = new NMTokenSecretManagerInNM();
        tokenSecretManager.setMasterKey(nmTokenMasterKey);
        Server server = rpc.getServer(ContainerManagementProtocol.class,
                        this, addr, config, tokenSecretManager, 10);
        server.start();
        LOG.info("Init rpc {}:{} success", hostName, port);
    }

    private org.apache.hadoop.yarn.server.api.records.NodeStatus createNodeStatus(NodeId nodeId, List<ContainerStatus> containers) {
        RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
        org.apache.hadoop.yarn.server.api.records.NodeStatus nodeStatus =
                recordFactory.newRecordInstance(org.apache.hadoop.yarn.server.api.records.NodeStatus.class);
        nodeStatus.setNodeId(nodeId);
        nodeStatus.setNodeUtilization(nodeUtilization);
        nodeStatus.setContainersUtilization(containersUtilization);
        nodeStatus.setContainersStatuses(containers);
        NodeHealthStatus nodeHealthStatus =
                recordFactory.newRecordInstance(NodeHealthStatus.class);
        nodeHealthStatus.setIsNodeHealthy(true);
        nodeHealthStatus.setHealthReport("Healthy");
        nodeHealthStatus.setLastHealthReportTime(System.currentTimeMillis());
        nodeStatus.setNodeHealthStatus(nodeHealthStatus);
        return nodeStatus;
    }

    public void heartbeat() throws IOException, YarnException {
        NodeStatus nodeStatus = createNodeStatus(nodeId, getContainerStatuses(containers));
        nodeStatus.setResponseId(responseID);
        NodeHeartbeatRequest request =
                NodeHeartbeatRequest.newInstance(nodeStatus, nmTokenMasterKey, nmTokenMasterKey,
                        CommonNodeLabelsManager.EMPTY_NODELABEL_SET, null, null);
        request.setNodeStatus(nodeStatus);
        request.setTokenSequenceNo(tokenSequenceNo);
        request.setLastKnownNMTokenMasterKey(nmTokenMasterKey);
        request.setLastKnownContainerTokenMasterKey(nmTokenMasterKey);
        NodeHeartbeatResponse response = resourceTracker.nodeHeartbeat(request);
        responseID = response.getResponseId();
        tokenSequenceNo = response.getTokenSequenceNo();
        LOG.debug("response, responseID={}, nmTokenMasterKey={}, tokenSequenceNo={}", responseID, request.getLastKnownNMTokenMasterKey(), response.getTokenSequenceNo());
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    private List<ContainerStatus> getContainerStatuses(Map<ApplicationId, List<Container>> containers) {
        List<ContainerStatus> containerStatuses = new ArrayList<ContainerStatus>();
        for (List<Container> appContainers : containers.values()) {
            for (Container container : appContainers) {
                containerStatuses.add(containerStatusMap.get(container));
            }
        }
        return containerStatuses;
    }

    @Override
    public StartContainersResponse startContainers(StartContainersRequest requests) throws YarnException, IOException {
        for (StartContainerRequest request : requests.getStartContainerRequests()) {
            Token containerToken = request.getContainerToken();
            ContainerTokenIdentifier tokenId = null;

            try {
                tokenId = BuilderUtils.newContainerTokenIdentifier(containerToken);
            } catch (IOException e) {
                throw RPCUtil.getRemoteException(e);
            }

            ContainerId containerID = tokenId.getContainerID();
            ApplicationId applicationId =
                    containerID.getApplicationAttemptId().getApplicationId();

            List<Container> applicationContainers = containers.computeIfAbsent(applicationId, k -> new ArrayList<Container>());

            // Sanity check
            for (Container container : applicationContainers) {
                if (container.getId().compareTo(containerID) == 0) {
                    throw new IllegalStateException("Container " + containerID
                            + " already setup on node " + containerManagerAddress);
                }
            }

            Container container =
                    BuilderUtils.newContainer(containerID, this.nodeId, nodeHttpAddress,
                            tokenId.getResource(), null, null // DKDC - Doesn't matter
                    );

            ContainerStatus containerStatus =
                    BuilderUtils.newContainerStatus(container.getId(),
                            ContainerState.NEW, "", -1000, container.getResource());
            applicationContainers.add(container);
            containerStatusMap.put(container, containerStatus);
            Resources.subtractFrom(available, tokenId.getResource());
            Resources.addTo(used, tokenId.getResource());

            LOG.debug("startContainer: node={} application={} container={}"
                            + " available={} used={}", containerManagerAddress, applicationId,
                    container, available, used);

        }
        StartContainersResponse response =
                StartContainersResponse.newInstance(null, null, null);
        return response;
    }

    @Override
    public StopContainersResponse stopContainers(StopContainersRequest request) throws YarnException, IOException {
        for (ContainerId containerID : request.getContainerIds()) {
            String applicationId =
                    String.valueOf(containerID.getApplicationAttemptId()
                            .getApplicationId().getId());
            // Mark the container as COMPLETE
            List<Container> applicationContainers = containers.get(containerID.getApplicationAttemptId()
                    .getApplicationId());
            for (Container c : applicationContainers) {
                if (c.getId().compareTo(containerID) == 0) {
                    ContainerStatus containerStatus = containerStatusMap.get(c);
                    containerStatus.setState(ContainerState.COMPLETE);
                    containerStatusMap.put(c, containerStatus);
                }
            }

            // Remove container and update status
            int ctr = 0;
            Container container = null;
            for (Iterator<Container> i = applicationContainers.iterator(); i
                    .hasNext(); ) {
                container = i.next();
                if (container.getId().compareTo(containerID) == 0) {
                    i.remove();
                    ++ctr;
                }
            }

            if (ctr != 1) {
                throw new IllegalStateException("Container " + containerID
                        + " stopped " + ctr + " times!");
            }

            Resources.addTo(available, container.getResource());
            Resources.subtractFrom(used, container.getResource());

            LOG.debug("stopContainer: node={} application={} container={}"
                            + " available={} used={}", containerManagerAddress, applicationId,
                    containerID, available, used);
        }
        return StopContainersResponse.newInstance(null, null);
    }

    @Override
    public GetContainerStatusesResponse getContainerStatuses(GetContainerStatusesRequest request) {
        List<ContainerStatus> statuses = new ArrayList<ContainerStatus>();
        for (ContainerId containerId : request.getContainerIds()) {
            List<Container> appContainers =
                    containers.get(containerId.getApplicationAttemptId()
                            .getApplicationId());
            Container container = null;
            for (Container c : appContainers) {
                if (c.getId().equals(containerId)) {
                    container = c;
                }
            }
            if (container != null
                    && containerStatusMap.get(container).getState() != null) {
                statuses.add(containerStatusMap.get(container));
            }
        }
        return GetContainerStatusesResponse.newInstance(statuses, null);
    }

    @Override
    public IncreaseContainersResourceResponse increaseContainersResource(IncreaseContainersResourceRequest request) {

        return null;
    }

    @Override
    public ContainerUpdateResponse updateContainer(ContainerUpdateRequest request) {
        return null;
    }

    @Override
    public SignalContainerResponse signalToContainer(SignalContainerRequest request) throws YarnException, IOException {
        throw new YarnException("Not supported yet!");
    }

    @Override
    public ResourceLocalizationResponse localize(ResourceLocalizationRequest request) throws YarnException, IOException {
        return null;
    }

    @Override
    public ReInitializeContainerResponse reInitializeContainer(ReInitializeContainerRequest request) throws YarnException, IOException {
        return null;
    }

    @Override
    public RestartContainerResponse restartContainer(ContainerId containerId) throws YarnException, IOException {
        return null;
    }

    @Override
    public RollbackResponse rollbackLastReInitialization(ContainerId containerId) throws YarnException, IOException {
        return null;
    }

    @Override
    public CommitResponse commitLastReInitialization(ContainerId containerId) throws YarnException, IOException {
        return null;
    }

    @Override
    public GetLocalizationStatusesResponse getLocalizationStatuses(GetLocalizationStatusesRequest request) throws YarnException, IOException {
        return null;
    }
}
