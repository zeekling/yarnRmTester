package org.apache.hadoop.sls.nm;

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.sls.job.FakeAppMaster;
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
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;

import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;

import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * fake NodeManager
 */
public class YarnFakeNodeManager implements ContainerManagementProtocol {


    private static final Logger LOG = LoggerFactory.getLogger(YarnFakeNodeManager.class);
    private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

    private final String containerManagerAddress;
    private final String nodeHttpAddress;
    private final String rackName;
    private final NodeId nodeId;
    private final Resource capability;
    private final Resource available;
    private final Resource used;

    private final ResourceTracker resourceTracker;

    private final Map<ApplicationId, List<Container>> containers = new ConcurrentHashMap<>();
    private final Map<Container, ContainerStatus> containerStatusMap = new ConcurrentHashMap<>();
    private final Map<ApplicationId, FakeAppMaster> appMasterMap = new ConcurrentHashMap<>();

    private int responseID = 0;

    private MasterKey nmTokenMasterKey= null;

    private long tokenSequenceNo;

    private final ResourceUtilization nodeUtilization;

    private final ResourceUtilization containersUtilization = ResourceUtilization.newInstance(0, 0, 0.0f);

    private final SLSConfig slsConfig;

    private final YarnConfiguration config;

    private final int httpPort;

    public YarnFakeNodeManager(String hostName, int containerManagerPort, int httpPort,
                               String rackName, Resource capability, YarnConfiguration config, SLSConfig slsConfig) throws IOException, YarnException {
        this.containerManagerAddress = hostName + ":" + containerManagerPort;
        this.nodeHttpAddress = hostName + ":" + httpPort;
        this.rackName = rackName;
        this.resourceTracker = ServerRMProxy.createRMProxy(config, ResourceTracker.class);
        this.capability = capability;
        this.httpPort = httpPort;
        this.available = Resource.newInstance(capability);
        this.used = Resource.newInstance(0, 0);
        this.nodeId = NodeId.newInstance(hostName, containerManagerPort);
        this.config = config;
        this.slsConfig = slsConfig;
        Map<String, Float> customResources = new HashMap<>();
        nodeUtilization = ResourceUtilization.newInstance(0, 0, 0f, customResources);
        customResources.put("yarn.io/gpu", 0f);
        registerNodeManager();
        initRpcServer(config, containerManagerPort, hostName);
        initHttpServer(httpPort, hostName);
    }

    private void registerNodeManager() throws YarnException, IOException {
        RegisterNodeManagerRequest request = recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
        request.setHttpPort(httpPort);
        request.setResource(capability);
        request.setPhysicalResource(capability);
        request.setNodeId(this.nodeId);
        request.setNMVersion(YarnVersionInfo.getVersion());
        LOG.info("begin register NodeManager {} capacity={}, available={}, used={}", nodeId, this.capability, this.available, this.used);
        RegisterNodeManagerResponse response = resourceTracker.registerNodeManager(request);
        nmTokenMasterKey = response.getNMTokenMasterKey();
        LOG.info("Register NodeManager {}  success", nodeId);
    }

    private void initHttpServer(int port, String hostName) throws IOException {
        InetSocketAddress addr = NetUtils.createSocketAddr(hostName + ":" + port);
        HttpServer httpServer = HttpServer.create(addr, 0);
        httpServer.createContext("/", new NMHttpHandler(this));
        httpServer.setExecutor(Executors.newFixedThreadPool(2));
        httpServer.start();
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
        NodeHealthStatus nodeHealthStatus = recordFactory.newRecordInstance(NodeHealthStatus.class);
        nodeHealthStatus.setIsNodeHealthy(true);
        nodeHealthStatus.setHealthReport("Healthy");
        nodeHealthStatus.setLastHealthReportTime(System.currentTimeMillis());
        nodeStatus.setNodeHealthStatus(nodeHealthStatus);
        return nodeStatus;
    }

    public void heartbeat() throws IOException, YarnException {
        NodeStatus nodeStatus = createNodeStatus(nodeId, getContainerStatuses(containers));
        nodeStatus.setResponseId(responseID);
        NodeHeartbeatRequest request = NodeHeartbeatRequest.newInstance(nodeStatus, nmTokenMasterKey, nmTokenMasterKey,
                CommonNodeLabelsManager.EMPTY_NODELABEL_SET, null, null);
        request.setNodeStatus(nodeStatus);
        request.setTokenSequenceNo(tokenSequenceNo);
        request.setLastKnownNMTokenMasterKey(nmTokenMasterKey);
        request.setLastKnownContainerTokenMasterKey(nmTokenMasterKey);
        NodeHeartbeatResponse response = resourceTracker.nodeHeartbeat(request);
        NodeAction nodeAction = response.getNodeAction();
        if (nodeAction == NodeAction.RESYNC) {
            registerNodeManager();
            responseID = 0;
            tokenSequenceNo = 0;
            return;
        }
        responseID = response.getResponseId();
        tokenSequenceNo = response.getTokenSequenceNo();
        List<ContainerId> toBeRemovedFromNM = response.getContainersToBeRemovedFromNM();
        removeContainer(toBeRemovedFromNM);
        LOG.debug("response, responseID={}, nmTokenMasterKey={}, tokenSequenceNo={}", responseID, request.getLastKnownNMTokenMasterKey(), response.getTokenSequenceNo());
    }

    private void removeContainer(List<ContainerId> toBeRemovedFromNM) {
        if (toBeRemovedFromNM == null || toBeRemovedFromNM.isEmpty()) {
            return;
        }
        for (ContainerId containerId : toBeRemovedFromNM) {
            ApplicationId applicationId = containerId.getApplicationAttemptId().getApplicationId();
            List<Container> containersAll = containers.get(applicationId);
            if (containersAll == null) {
                continue;
            }
            Iterator<Container> it = containersAll.iterator();
            while (it.hasNext()) {
                Container container = it.next();
                if (containerId.equals(container.getId())) {
                    it.remove();
                    containerStatusMap.remove(container);
                }
            }
            if (containersAll.isEmpty()) {
                containers.remove(applicationId);
            }
        }
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public Resource getCapability() {
        return capability;
    }

    public String getRackName() {
        return rackName;
    }

    public Resource getAvailable() {
        return available;
    }

    public Resource getUsed() {
        return used;
    }

    public Map<ApplicationId, FakeAppMaster> getAppMasterMap() {
        return appMasterMap;
    }

    public YarnConfiguration getConfig() {
        return config;
    }

    public SLSConfig getSlsConfig() {
        return slsConfig;
    }

    private List<ContainerStatus> getContainerStatuses(Map<ApplicationId, List<Container>> containers) {
        List<ContainerStatus> containerStatuses = new ArrayList<>();
        for (List<Container> appContainers : containers.values()) {
            for (Container container : appContainers) {
                containerStatuses.add(containerStatusMap.get(container));
            }
        }
        return containerStatuses;
    }

    @Override
    public StartContainersResponse startContainers(StartContainersRequest requests) throws YarnException, IOException {
        List<ContainerId> succeededContainers = new ArrayList<>();
        Map<ContainerId, SerializedException> failedContainers = new HashMap<>();
        for (StartContainerRequest request : requests.getStartContainerRequests()) {
            Token containerToken = request.getContainerToken();
            ContainerTokenIdentifier tokenId;

            try {
                tokenId = BuilderUtils.newContainerTokenIdentifier(containerToken);
            } catch (IOException e) {
                throw RPCUtil.getRemoteException(e);
            }

            ContainerId containerID = tokenId.getContainerID();
            ApplicationId applicationId = containerID.getApplicationAttemptId().getApplicationId();

            List<Container> applicationContainers = containers.computeIfAbsent(applicationId, k -> new ArrayList<>());

            // Sanity check
            for (Container container : applicationContainers) {
                if (container.getId().compareTo(containerID) == 0) {
                    throw new IllegalStateException("Container " + containerID
                            + " already setup on node " + containerManagerAddress);
                }
            }

            Container container = BuilderUtils.newContainer(containerID, this.nodeId, nodeHttpAddress,
                    tokenId.getResource(), null, null);
            Credentials credentials = YarnServerSecurityUtils.parseCredentials(request.getContainerLaunchContext());
            ContainerTokenIdentifier tokenIdentifier = BuilderUtils.newContainerTokenIdentifier(request.getContainerToken());


            ContainerStatus containerStatus = BuilderUtils.newContainerStatus(container.getId(),
                    ContainerState.RUNNING, "running", -1000, container.getResource());
            applicationContainers.add(container);
            FakeAppMaster appMaster = null;
            if (tokenIdentifier.getContainerType().equals(ContainerType.APPLICATION_MASTER)) {
                appMaster = new FakeAppMaster(applicationId, this, credentials);
                appMaster.addContainer(container);
                appMasterMap.put(applicationId, appMaster);
            } else {
                appMaster = appMasterMap.get(applicationId);
                appMaster.addContainer(container);
            }
            containerStatusMap.put(container, containerStatus);
            Resources.subtractFrom(available, tokenId.getResource());
            Resources.addTo(used, tokenId.getResource());

            LOG.debug("startContainer: node={} application={} container={}"
                            + " available={} used={}", containerManagerAddress, applicationId,
                    container, available, used);
            succeededContainers.add(containerID);

        }
        return StartContainersResponse.newInstance(null, succeededContainers, failedContainers);
    }

    public void updateContainerStatus() throws IOException, YarnException {
        if (appMasterMap.isEmpty()) {
            return;
        }
        for (Map.Entry<ApplicationId, FakeAppMaster> entry : appMasterMap.entrySet()) {
            FakeAppMaster appMaster = entry.getValue();
            if (appMaster.isRegistered()) {
                try {
                    appMaster.updateContainer();
                } catch (Exception e) {
                    LOG.warn("update Container failed", e);
                    appMaster.failedApp(e.getMessage());
                }
            } else {
                try {
                    appMaster.registerToRm();
                } catch (Exception e) {
                    LOG.warn("register failed", e);
                    appMaster.failedApp(e.getMessage());
                }
            }
        }
    }

    @Override
    public StopContainersResponse stopContainers(StopContainersRequest request) {
        for (ContainerId containerID : request.getContainerIds()) {
            ApplicationId applicationId = containerID.getApplicationAttemptId().getApplicationId();
            // Mark the container as COMPLETE
            List<Container> applicationContainers = containers.get(applicationId);
            if (applicationContainers == null) {
                continue;
            }
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
            if (applicationContainers.isEmpty()) {
                containers.remove(applicationId);
                appMasterMap.remove(applicationId);
            }

            if (ctr != 1) {
                continue;
            }

            Resources.addTo(available, container.getResource());
            Resources.subtractFrom(used, container.getResource());

            LOG.debug("stopContainer: node={} application={} container={}"
                            + " available={} used={}", containerManagerAddress, applicationId,
                    containerID, available, used);
        }
        return StopContainersResponse.newInstance(null, null);
    }

    public void stopApplication(ApplicationId applicationId) {
        appMasterMap.remove(applicationId);
        // Mark the container as COMPLETE
        List<Container> applicationContainers = containers.get(applicationId);
        if (applicationContainers == null) {
            return;
        }
        for (Container c : applicationContainers) {
            ContainerStatus containerStatus = containerStatusMap.get(c);
            containerStatus.setState(ContainerState.COMPLETE);
            containerStatusMap.put(c, containerStatus);

        }

        Container container = null;
        for (Iterator<Container> i = applicationContainers.iterator(); i
                .hasNext(); ) {
            container = i.next();
            i.remove();
            Resources.addTo(available, container.getResource());
            Resources.subtractFrom(used, container.getResource());
            LOG.debug("stopContainer: node={} application={} container={}"
                            + " available={} used={}", containerManagerAddress, applicationId,
                    container.getId(), available, used);
        }

    }

    @Override
    public GetContainerStatusesResponse getContainerStatuses(GetContainerStatusesRequest request) {
        List<ContainerStatus> statuses = new ArrayList<>();
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
    public SignalContainerResponse signalToContainer(SignalContainerRequest request) throws YarnException {
        throw new YarnException("Not supported yet!");
    }

    @Override
    public ResourceLocalizationResponse localize(ResourceLocalizationRequest request) {
        return null;
    }

    @Override
    public ReInitializeContainerResponse reInitializeContainer(ReInitializeContainerRequest request) {
        return null;
    }

    @Override
    public RestartContainerResponse restartContainer(ContainerId containerId) {
        return null;
    }

    @Override
    public RollbackResponse rollbackLastReInitialization(ContainerId containerId) {
        return null;
    }

    @Override
    public CommitResponse commitLastReInitialization(ContainerId containerId) {
        return null;
    }

    @Override
    public GetLocalizationStatusesResponse getLocalizationStatuses(GetLocalizationStatusesRequest request) {
        return null;
    }
}
