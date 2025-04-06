package org.apache.hadoop.sls.job;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.sls.nm.YarnFakeNodeManager;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FakeAppMaster {

    private static final Logger LOG = LoggerFactory.getLogger(FakeAppMaster.class);

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

    private int containerCount = 0;

    private Container appMaster = null;

    public FakeAppMaster(ApplicationId applicationId, YarnFakeNodeManager nodeManager, Credentials credentials) {
        this.applicationId = applicationId;
        appStartTime = System.currentTimeMillis();
        this.nodeManager = nodeManager;
        this.slsConfig = nodeManager.getSlsConfig();
        this.credentials = credentials;
        try {
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            Token<AMRMTokenIdentifier> amrmToken = getFirstAMRMToken(credentials.getAllTokens());
            LOG.debug("amrmToken={}", amrmToken);
            currentUser.addToken(amrmToken);
            currentUser.addCredentials(credentials);
            appMasterClient = AMRMClientUtils.createRMProxy(nodeManager.getConfig(), ApplicationMasterProtocol.class, currentUser, amrmToken);
        } catch (IOException e) {
            LOG.info("init faiked", e);
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
        if (appMaster != null) {
            appMaster = container;
        }
        containerCount++;
    }

    public synchronized void addContainer(Container container) {
        containers.put(container, System.currentTimeMillis());
        containerCount++;
    }

    public boolean isRegistered() {
        return isRegistered;
    }

    public void registerToRm() throws IOException, YarnException {
        RegisterApplicationMasterRequest request = RegisterApplicationMasterRequest.newInstance(nodeManager.getNodeId().getHost(), nodeManager.getNodeId().getPort(), "");
        appMasterClient.registerApplicationMaster(request);
        isRegistered = true;
    }

    public void updateContainer() throws IOException, YarnException {

        if (containers.isEmpty()) {
            return;
        }

        checkFinished();
        if (containerCount <= slsConfig.getJobContainerNums()) {
            // 申请allocation
            allocateContainer();
            return;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - appStartTime < slsConfig.getJobDuration()) {
            return;
        }
        nodeManager.stopApplication(applicationId);
        FinishApplicationMasterRequest request = FinishApplicationMasterRequest.newInstance(FinalApplicationStatus.SUCCEEDED, "run success", "");
        try {
            appMasterClient.finishApplicationMaster(request);
        } catch (InvalidApplicationMasterRequestException e) {
            LOG.debug("ignore error {}", e.getMessage());
        }
    }

    private void checkFinished() {
        long currentTime = System.currentTimeMillis();
        for (Map.Entry<Container, Long> entry: containers.entrySet()) {
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
            ResourceRequest ask = ResourceRequest.newInstance(Priority.newInstance(0), ResourceRequest.ANY, resource, slsConfig.getJobContainerNums());
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
        List<StartContainerRequest> requests = new ArrayList<>();
        for (Container container: allocatedContainers) {
            ContainerLaunchContext launchContext = setupContainerLaunchContext();
            StartContainerRequest startContainerRequest = StartContainerRequest.newInstance(launchContext, container.getContainerToken());
            requests.add(startContainerRequest);
        }
        StartContainersRequest startContainersRequest = StartContainersRequest.newInstance(requests);
        nodeManager.startContainers(startContainersRequest);
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
        nodeManager.stopApplication(applicationId);
        FinishApplicationMasterRequest request = FinishApplicationMasterRequest.newInstance(FinalApplicationStatus.FAILED, msg, "");
        try {
            appMasterClient.finishApplicationMaster(request);
        } catch (InvalidApplicationMasterRequestException e) {
            LOG.debug("ignore error {}", e.getMessage());
        }
    }
}
