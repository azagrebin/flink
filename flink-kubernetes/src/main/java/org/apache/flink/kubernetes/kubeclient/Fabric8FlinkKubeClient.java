/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPodsWatcher;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The implementation of {@link FlinkKubeClient}.
 */
public class Fabric8FlinkKubeClient implements FlinkKubeClient {

	private static final Logger LOG = LoggerFactory.getLogger(Fabric8FlinkKubeClient.class);

	private final Configuration flinkConfig;
	private final KubernetesClient internalClient;
	private final String clusterId;
	private final String nameSpace;

	private final ExecutorWrapper executorWrapper;

	public Fabric8FlinkKubeClient(Configuration flinkConfig, KubernetesClient client, ExecutorWrapper executorWrapper) {
		this.flinkConfig = checkNotNull(flinkConfig);
		this.internalClient = checkNotNull(client);
		this.clusterId = checkNotNull(flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID));

		this.nameSpace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);

		this.executorWrapper = executorWrapper;
	}

	@Override
	public CompletableFuture<Void> createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec) {
		final Deployment deployment = kubernetesJMSpec.getDeployment();
		final List<HasMetadata> accompanyingResources = kubernetesJMSpec.getAccompanyingResources();

		// create Deployment
		LOG.debug("Start to create deployment with spec {}", deployment.getSpec().toString());

		return CompletableFuture.runAsync(() -> {
			final Deployment createdDeployment = this.internalClient
				.apps()
				.deployments()
				.inNamespace(this.nameSpace)
				.create(deployment);

			// Note that we should use the uid of the created Deployment for the OwnerReference.
			setOwnerReference(createdDeployment, accompanyingResources);

			this.internalClient
				.resourceList(accompanyingResources)
				.inNamespace(this.nameSpace)
				.createOrReplace();
		}, executorWrapper.getExecutor());
	}

	@Override
	public void createTaskManagerPod(KubernetesPod kubernetesPod) {
		CompletableFuture.runAsync(() -> {
			final Deployment masterDeployment = this.internalClient
				.apps()
				.deployments()
				.inNamespace(this.nameSpace)
				.withName(KubernetesUtils.getDeploymentName(clusterId))
				.get();

			if (masterDeployment == null) {
				throw new RuntimeException(
					"Failed to find Deployment named " + clusterId + " in namespace " + this.nameSpace);
			}

			// Note that we should use the uid of the master Deployment for the OwnerReference.
			setOwnerReference(masterDeployment, Collections.singletonList(kubernetesPod.getInternalResource()));

			LOG.debug("Start to create pod with metadata {}, spec {}",
				kubernetesPod.getInternalResource().getMetadata(),
				kubernetesPod.getInternalResource().getSpec());

			this.internalClient
				.pods()
				.inNamespace(this.nameSpace)
				.create(kubernetesPod.getInternalResource());
		}, executorWrapper.getExecutor());
	}

	@Override
	public void stopPod(String podName) {
		CompletableFuture.runAsync(
			() -> this.internalClient.pods().withName(podName).delete(),
			executorWrapper.getExecutor());
	}

	@Override
	public CompletableFuture<Optional<Endpoint>> getRestEndpoint(String clusterId) {
		final int restPort = this.flinkConfig.getInteger(RestOptions.PORT);
		final KubernetesConfigOptions.ServiceExposedType serviceExposedType =
			flinkConfig.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);

		// Return the service.namespace directly when use ClusterIP.
		if (serviceExposedType == KubernetesConfigOptions.ServiceExposedType.ClusterIP) {
			return CompletableFuture.completedFuture(
				Optional.of(new Endpoint(KubernetesUtils.getInternalServiceName(clusterId) + "." + nameSpace, restPort)));
		}

		return getRestService(clusterId).thenApply(restService -> {
			if (!restService.isPresent()) {
				return Optional.empty();
			}
			Service service = restService.get().getInternalResource();

			String address = null;
			int endpointPort = restPort;

			if (service.getStatus() != null && (service.getStatus().getLoadBalancer() != null ||
				service.getStatus().getLoadBalancer().getIngress() != null)) {
				if (service.getStatus().getLoadBalancer().getIngress().size() > 0) {
					address = service.getStatus().getLoadBalancer().getIngress().get(0).getIp();
					if (address == null || address.isEmpty()) {
						address = service.getStatus().getLoadBalancer().getIngress().get(0).getHostname();
					}
				} else {
					address = this.internalClient.getMasterUrl().getHost();
					endpointPort = getServiceNodePort(service, RestOptions.PORT);
				}
			} else if (service.getSpec().getExternalIPs() != null && service.getSpec().getExternalIPs().size() > 0) {
				address = service.getSpec().getExternalIPs().get(0);
			}
			if (address == null || address.isEmpty()) {
				return Optional.empty();
			}
			return Optional.of(new Endpoint(address, endpointPort));
		});
	}

	@Override
	public CompletableFuture<List<KubernetesPod>> getPodsWithLabels(Map<String, String> labels) {
		return CompletableFuture.supplyAsync(() -> {
			final List<Pod> podList = this.internalClient.pods().withLabels(labels).list().getItems();

			if (podList == null || podList.size() < 1) {
				return new ArrayList<>();
			}

			return podList
				.stream()
				.map(KubernetesPod::new)
				.collect(Collectors.toList());
		}, executorWrapper.getExecutor());
	}

	@Override
	public CompletableFuture<Void> stopAndCleanupCluster(String clusterId) {
		return CompletableFuture.runAsync(() -> this.internalClient
			.apps()
			.deployments()
			.inNamespace(this.nameSpace)
			.withName(KubernetesUtils.getDeploymentName(clusterId))
			.cascading(true)
			.delete(), executorWrapper.getExecutor());
	}

	@Override
	public void handleException(Exception e) {
		LOG.error("A Kubernetes exception occurred.", e);
	}

	@Override
	public CompletableFuture<Optional<KubernetesService>> getInternalService(String clusterId) {
		return CompletableFuture.supplyAsync(
			() -> getService(KubernetesUtils.getInternalServiceName(clusterId)),
			executorWrapper.getExecutor());
	}

	@Override
	public CompletableFuture<Optional<KubernetesService>> getRestService(String clusterId) {
		return CompletableFuture.supplyAsync(
			() -> getService(KubernetesUtils.getRestServiceName(clusterId)),
			executorWrapper.getExecutor());
	}

	@Override
	public CompletableFuture<KubernetesWatch> watchPodsAndDoCallback(
			Map<String, String> labels,
			KubernetesPodsWatcher podsWatcher) {
		return CompletableFuture.supplyAsync(
			() -> new KubernetesWatch(this.internalClient.pods().withLabels(labels).watch(podsWatcher)),
			executorWrapper.getExecutor());
	}

	@Override
	public void close() {
		this.internalClient.close();
		this.executorWrapper.close();
	}

	private void setOwnerReference(Deployment deployment, List<HasMetadata> resources) {
		final OwnerReference deploymentOwnerReference = new OwnerReferenceBuilder()
			.withName(deployment.getMetadata().getName())
			.withApiVersion(deployment.getApiVersion())
			.withUid(deployment.getMetadata().getUid())
			.withKind(deployment.getKind())
			.withController(true)
			.withBlockOwnerDeletion(true)
			.build();
		resources.forEach(resource ->
			resource.getMetadata().setOwnerReferences(Collections.singletonList(deploymentOwnerReference)));
	}

	private Optional<KubernetesService> getService(String serviceName) {
		final Service service = this
			.internalClient
			.services()
			.inNamespace(nameSpace)
			.withName(serviceName)
			.fromServer()
			.get();

		if (service == null) {
			LOG.debug("Service {} does not exist", serviceName);
			return Optional.empty();
		}

		return Optional.of(new KubernetesService(service));
	}

	/**
	 * To get nodePort of configured ports.
	 */
	private int getServiceNodePort(Service service, ConfigOption<Integer> configPort) {
		final int port = this.flinkConfig.getInteger(configPort);
		if (service.getSpec() != null && service.getSpec().getPorts() != null) {
			for (ServicePort p : service.getSpec().getPorts()) {
				if (p.getPort() == port) {
					return p.getNodePort();
				}
			}
		}
		return port;
	}
}
