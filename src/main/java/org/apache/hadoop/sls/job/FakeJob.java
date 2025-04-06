package org.apache.hadoop.sls.job;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl.AM_CONTAINER_PRIORITY;


public class FakeJob {

    private static final Logger LOG = LoggerFactory.getLogger(FakeJob.class);

    private final static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

    private final YarnConfiguration config;

    private final SLSConfig slsConfig;

    private final ApplicationClientProtocol rmClient;

    private YarnClientApplication clientApplication;

    private final Credentials credentials = new Credentials();

    private final String jobName;

    private final String AM_RESOURCE_PREFIX = "yarn.app.fake.am.";

    private final String AM_RESOURCE_MEMORY = AM_RESOURCE_PREFIX + "memory-mb";

    private final String AM_RESOURCE_VCORE = AM_RESOURCE_PREFIX + "vcores";



    public FakeJob(YarnConfiguration config, SLSConfig slsConfig, String jobName) throws IOException {
        this.config = config;
        this.rmClient = ClientRMProxy.createRMProxy(config, ApplicationClientProtocol.class);
        this.slsConfig = slsConfig;
        this.jobName = jobName;
    }

    private GetNewApplicationResponse getNewApplication()
            throws YarnException, IOException {
        GetNewApplicationRequest request =
                Records.newRecord(GetNewApplicationRequest.class);
        return rmClient.getNewApplication(request);
    }

    public YarnClientApplication createApplication()
            throws YarnException, IOException {
        ApplicationSubmissionContext context = Records.newRecord
                (ApplicationSubmissionContext.class);
        GetNewApplicationResponse newApp = getNewApplication();
        ApplicationId appId = newApp.getApplicationId();
        context.setApplicationId(appId);
        return new YarnClientApplication(newApp, context);
    }

    private void generalToken() throws IOException {
        if (!UserGroupInformation.isSecurityEnabled()) {
            return;
        }
        Path[] jobTokenServers = slsConfig.getJobTokenServers();
        for (Path p : jobTokenServers) {
            FileSystem fs = p.getFileSystem(config);
            // todo get kerberos principal for use as delegation token renewer
            fs.addDelegationTokens("", credentials);
        }
    }

    private void printTokens() {
        if (!UserGroupInformation.isSecurityEnabled()) {
            return;
        }
        LOG.info("Submitting tokens for job: {}", clientApplication.getApplicationSubmissionContext().getApplicationId());
        LOG.info("Executing with tokens: {}", credentials.getAllTokens());
    }

    private ContainerLaunchContext setupContainerLaunchContextForAM() throws IOException {
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

    private List<ResourceRequest> generateResourceRequests() throws IOException {
        Resource capability = recordFactory.newRecordInstance(Resource.class);
        List<ResourceInformation> resourceRequests = ResourceUtils.getRequestedResourcesFromConfig(config, AM_RESOURCE_PREFIX);
        boolean memorySet = false;
        boolean cpuVcoresSet = false;
        for (ResourceInformation resourceReq : resourceRequests) {
            String resourceName = resourceReq.getName();
            LOG.info("resourceName={}", resourceName);
            if (AM_RESOURCE_MEMORY.equals(resourceName)) {
                LOG.info("resourceReq.getUnits()={}", resourceReq.getUnits());
                capability.setMemorySize(UnitsConversionUtil.convert(resourceReq.getUnits(), "Mi", resourceReq.getValue()));
                memorySet = true;
            } else if (AM_RESOURCE_VCORE.equals(resourceName)) {
                capability.setVirtualCores((int) UnitsConversionUtil.convert(resourceReq.getUnits(), "", resourceReq.getValue()));
                cpuVcoresSet = true;
            } else {
                LOG.debug("invalid resource");
            }
        }
        if (!memorySet) {
            capability.setMemorySize(1536);
        }
        if (!cpuVcoresSet) {
            capability.setVirtualCores(1);
        }
        List<ResourceRequest> amResourceRequests = new ArrayList<>();
        ResourceRequest amAnyResourceRequest = createAMResourceRequest(capability);
        amResourceRequests.add(amAnyResourceRequest);
        return amResourceRequests;
    }

    private ResourceRequest createAMResourceRequest(Resource capability) {
        ResourceRequest amRequest = recordFactory.newRecordInstance(ResourceRequest.class);
        amRequest.setPriority(AM_CONTAINER_PRIORITY);
        amRequest.setResourceName(ResourceRequest.ANY);
        amRequest.setCapability(capability);
        amRequest.setNumContainers(1);
        amRequest.setRelaxLocality(true);
        return amRequest;
    }

    private ApplicationSubmissionContext createApplicationSubmissionContext() throws IOException {
        ApplicationSubmissionContext appContext = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
        ContainerLaunchContext amContainer = setupContainerLaunchContextForAM();
        appContext.setApplicationId(clientApplication.getApplicationSubmissionContext().getApplicationId());
        appContext.setQueue(slsConfig.getJobQueue());
        appContext.setApplicationName(jobName);
        appContext.setCancelTokensWhenComplete(true);
        appContext.setAMContainerSpec(amContainer);
        appContext.setMaxAppAttempts(3);
        appContext.setApplicationType("Fake Job");
        List<ResourceRequest> amResourceRequests = generateResourceRequests();
        appContext.setAMContainerResourceRequests(amResourceRequests);
        return appContext;
    }

    public void submit() throws IOException, YarnException {
        generalToken();
        this.clientApplication = createApplication();
        printTokens();
        ApplicationSubmissionContext appContext = createApplicationSubmissionContext();
        SubmitApplicationRequest request = Records.newRecord(SubmitApplicationRequest.class);
        request.setApplicationSubmissionContext(appContext);
        rmClient.submitApplication(request);
        LOG.info("Job {} submited", appContext.getApplicationId());

    }

}
