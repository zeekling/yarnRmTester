package org.apache.hadoop.sls.job;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.sls.nm.JobStatUpdater;
import org.apache.hadoop.sls.nm.YarnFakeNodeManager;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class FakeAppMaster {

    private static final Logger LOG = LoggerFactory.getLogger(FakeAppMaster.class);

    private final ApplicationId applicationId;

    private final List<Container> containers = new ArrayList<>();

    private final long appStartTime;

    private boolean isRegistered = false;

    private final YarnFakeNodeManager nodeManager;

    private final SLSConfig slsConfig;

    private final Credentials credentials;

    private ApplicationMasterProtocol appMasterClient;

    private int lastResponseID = 0;


    private boolean containerAllocated = false;

    public FakeAppMaster(ApplicationId applicationId, YarnFakeNodeManager nodeManager, Credentials credentials) {
        this.applicationId = applicationId;
        appStartTime = System.currentTimeMillis();
        this.nodeManager = nodeManager;
        this.slsConfig = nodeManager.getSlsConfig();
        this.credentials = credentials;
        try {
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            Token<AMRMTokenIdentifier> amrmToken = getFirstAMRMToken(credentials.getAllTokens());
            LOG.info("amrmToken={}", amrmToken);
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

    public void addContainer(Container container) {
        containers.add(container);
    }

    public ApplicationId getApplicationId() {
        return applicationId;
    }

    public List<Container> getContainers() {
        return containers;
    }

    public long getAppStartTime() {
        return appStartTime;
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
        long currentTime = System.currentTimeMillis();
        if (containers.isEmpty()) {
            return;
        }
        if (containers.size() <= slsConfig.getJobContainerNums() && currentTime - appStartTime < slsConfig.getJobDuration()) {
            // 申请allocation
            allocateContainer();
            return;
        }

        if (currentTime - appStartTime < slsConfig.getJobDuration()) {
            return;
        }
        nodeManager.stopApplication(applicationId);
        FinishApplicationMasterRequest request = FinishApplicationMasterRequest.newInstance(FinalApplicationStatus.SUCCEEDED, "run success", "");
        appMasterClient.finishApplicationMaster(request);
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
        LOG.info("allocated container {} ", Arrays.toString(allocatedContainers.toArray()));
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
        appMasterClient.finishApplicationMaster(request);
    }
}
