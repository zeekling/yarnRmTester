package org.apache.hadoop.sls.job;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.sls.nm.YarnFakeNodeManager;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.sls.nm.NodeManagerCommon.FAKE_NODE_MANAGER_MAP;

public class FakeApplication {

    private static final Logger LOG = LoggerFactory.getLogger(FakeApplication.class);

    private final ApplicationId applicationId;

    private final Map<Container, Long> containers = new ConcurrentHashMap<>();

    private final long appStartTime;

    private boolean isRegistered = false;

    private final YarnFakeNodeManager nodeManager;

    private final SLSConfig slsConfig;

    private final Credentials credentials;

    private ApplicationMasterProtocol appMasterClient;

    private int lastResponseID = 0;

    private boolean containerAllocated = false;

    private Container appMaster = null;

    private int allocatedCount = 0;

    private final YarnConfiguration config;

    private Map<NodeId, ContainerManagementProtocol> nodeManagerConnections = new HashMap<>();

    public FakeApplication(ApplicationId applicationId, YarnFakeNodeManager nodeManager, Credentials credentials, YarnConfiguration config) {
        this.applicationId = applicationId;
        appStartTime = System.currentTimeMillis();
        this.nodeManager = nodeManager;
        this.slsConfig = nodeManager.getSlsConfig();
        this.credentials = credentials;
        this.config = config;
        try {
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            Token<AMRMTokenIdentifier> amrmToken = getFirstAMRMToken(credentials.getAllTokens());
            LOG.debug("amrmToken={}", amrmToken);
            currentUser.addToken(amrmToken);
            currentUser.addCredentials(credentials);
            appMasterClient = AMRMClientUtils.createRMProxy(nodeManager.getConfig(), ApplicationMasterProtocol.class, currentUser, amrmToken);
        } catch (IOException e) {
            LOG.warn("init faiked", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Token<AMRMTokenIdentifier> getFirstAMRMToken(
            Collection<Token<? extends TokenIdentifier>> allTokens) {
        for (Token<? extends TokenIdentifier> token : allTokens) {
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                return (Token<AMRMTokenIdentifier>) token;
            }
        }

        return null;
    }

    public synchronized void addMasterContainer(Container container) {
        containers.put(container, System.currentTimeMillis());
        if (appMaster == null) {
            appMaster = container;
        }
    }

    public synchronized void addContainer(Container container) {
        containers.put(container, System.currentTimeMillis());
    }

    public boolean isRegistered() {
        return appMaster == null || isRegistered;
    }

    public Container getAppMaster() {
        return appMaster;
    }

    public int getAllocatedCount() {
        return allocatedCount;
    }

    public long getAppStartTime() {
        return appStartTime;
    }

    public void registerToRm() throws IOException, YarnException {
        if (appMaster == null) {
            return;
        }
        String appHttpAddress = appMaster.getNodeHttpAddress() + "/" + appMaster.getId() + "?nodeId=" + appMaster.getNodeId();
        RegisterApplicationMasterRequest request = RegisterApplicationMasterRequest.newInstance(nodeManager.getNodeId().getHost(), nodeManager.getNodeId().getPort(), appHttpAddress);
        appMasterClient.registerApplicationMaster(request);
        isRegistered = true;
        LOG.info("AM {} register success", appMaster.getId());
    }

    public void updateContainer() throws IOException, YarnException {

        if (containers.isEmpty()) {
            return;
        }

        checkFinished();
        if (allocatedCount < slsConfig.getJobContainerNums() && appMaster != null) {
            // 申请allocation
            allocateContainer();
            return;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - appStartTime < slsConfig.getJobDuration()) {
            return;
        }

        if (appMaster == null || !isRegistered) {
            stopContainers();
            return;
        }

        FinishApplicationMasterRequest request = FinishApplicationMasterRequest.newInstance(FinalApplicationStatus.SUCCEEDED, "run success", "");
        try {
            stopContainers();
            appMasterClient.finishApplicationMaster(request);
            LOG.info("app {} finished", appMaster.getId().getApplicationAttemptId().getApplicationId());
        } catch (InvalidApplicationMasterRequestException e) {
            LOG.debug("ignore error {}", e.getMessage());
        }
    }

    private void checkFinished() {
        long currentTime = System.currentTimeMillis();
        for (Map.Entry<Container, Long> entry : containers.entrySet()) {
            Long time = entry.getValue();
            Container container = entry.getKey();
            if (currentTime - time < slsConfig.getJobDuration()) {
                continue;
            }
            if (appMaster == null || appMaster == container) {
                continue;
            }
            ContainerStatus containerStatus = nodeManager.getContainerStatusMap().get(container);
            if (containerStatus == null) {
                continue;
            }
            containerStatus.setDiagnostics("stoped");
            containerStatus.setExitStatus(0);
            containerStatus.setState(ContainerState.COMPLETE);
        }
    }

    public void allocateContainer() throws IOException, YarnException {
        ResourceBlacklistRequest blacklistRequest = ResourceBlacklistRequest.newInstance(new ArrayList<>(), new ArrayList<>());
        float process = (float) containers.size() / slsConfig.getJobContainerNums();
        List<ResourceRequest> askList = new ArrayList<>();
        if (!containerAllocated) {
            Resource resource = slsConfig.getJobContainerResource();
            ResourceRequest ask = ResourceRequest.newInstance(Priority.newInstance(0), ResourceRequest.ANY, resource, slsConfig.getJobContainerNums(), true);
            askList.add(ask);
        }
        AllocateRequest request = AllocateRequest.newInstance(lastResponseID, process, askList, new ArrayList<>(), blacklistRequest);
        AllocateResponse response = appMasterClient.allocate(request);
        lastResponseID = response.getResponseId();
        containerAllocated = true;

        List<Container> allocatedContainers = response.getAllocatedContainers();
        if (allocatedContainers.isEmpty()) {
            return;
        }
        LOG.debug("allocated container {} ", Arrays.toString(allocatedContainers.toArray()));
        for (Container container : allocatedContainers) {
            org.apache.hadoop.yarn.api.records.Token containerToken = container.getContainerToken();
            List<StartContainerRequest> requests = new ArrayList<>();
            ContainerLaunchContext launchContext = setupContainerLaunchContext();
            StartContainerRequest startContainerRequest = StartContainerRequest.newInstance(launchContext, container.getContainerToken());
            requests.add(startContainerRequest);
            StartContainersRequest startContainersRequest = StartContainersRequest.newInstance(requests);
            YarnFakeNodeManager realNodeManager = FAKE_NODE_MANAGER_MAP.get(container.getNodeId());
            startContainer(container, realNodeManager, startContainersRequest);
        }
        allocatedCount += allocatedContainers.size();
    }

    private void startContainer(Container container, YarnFakeNodeManager realNodeManager, StartContainersRequest startContainersRequest) throws YarnException, IOException {
        if (realNodeManager != null) {
            realNodeManager.startContainers(startContainersRequest);
        } else {
            ContainerManagementProtocol nmConnection = getNmConnection(container);
            if (nmConnection != null) {
                try {
                    nmConnection.startContainers(startContainersRequest);
                    addContainer(container);
                } catch (Exception e) {
                    addContainer(container);
                }
            } else {
                addContainer(container);
            }
        }
    }

    public ContainerManagementProtocol getNmConnection(Container container) {
        YarnRPC yarnRPC = YarnRPC.create(config);
        try {
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            InetSocketAddress cmAddr =
                    NetUtils.createSocketAddr(container.getNodeId().toString());
            org.apache.hadoop.security.token.Token<NMTokenIdentifier> nmToken =
                    ConverterUtils.convertFromYarn(container.getContainerToken(), cmAddr);
            currentUser.addToken(nmToken);
            ContainerManagementProtocol nmProxy = NMProxy.createNMProxy(config, ContainerManagementProtocol.class,
                    currentUser, yarnRPC, cmAddr);
            nodeManagerConnections.putIfAbsent(container.getNodeId(), nmProxy);
            return nmProxy;
        } catch (IOException e) {
            LOG.warn("exception ", e);
            return null;
        }
    }

    private ContainerLaunchContext setupContainerLaunchContext() throws IOException {
        Map<String, LocalResource> localResources = new HashMap<>();
        Map<String, String> environment = new HashMap<>();
        environment.put(ApplicationConstants.Environment.SHELL.name(), "/bin/bash");

        Vector<String> vargsFinal = new Vector<>(8);
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        Map<ApplicationAccessType, String> acls = new HashMap<>(2);
        acls.put(ApplicationAccessType.VIEW_APP, " ");
        acls.put(ApplicationAccessType.MODIFY_APP, " ");
        return ContainerLaunchContext.newInstance(localResources, environment,
                vargsFinal, null, securityTokens, acls);
    }

    public void failedApp(String msg) throws IOException, YarnException {
        if (appMaster == null) {
            stopContainers();
            return;
        }
        FinishApplicationMasterRequest request = FinishApplicationMasterRequest.newInstance(FinalApplicationStatus.FAILED, msg, "");
        try {
            stopContainers();
            appMasterClient.finishApplicationMaster(request);
        } catch (InvalidApplicationMasterRequestException e) {
            LOG.debug("ignore error {}", e.getMessage());
        }
    }

    private void stopContainers() {
        nodeManager.stopContainers(applicationId);
        Iterator<Map.Entry<Container, Long>> it = containers.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Container, Long> entry = it.next();
            Container container = entry.getKey();
            YarnFakeNodeManager fakeNodeManager = FAKE_NODE_MANAGER_MAP.get(container.getNodeId());
            it.remove();
            if (fakeNodeManager != null) {
                continue;
            }
            ContainerManagementProtocol nmConnection = getNmConnection(container);
            try {
                List<ContainerId> containerIds = new ArrayList<>();
                containerIds.add(container.getId());
                StopContainersRequest stopContainersRequest = StopContainersRequest.newInstance(containerIds);
                nmConnection.stopContainers(stopContainersRequest);
            } catch (Exception e) {
                LOG.warn("remove Container failed", e);
            }
        }
    }
}
