package softtainter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/util/taints"

	desv1 "github.com/openshift/cluster-kube-descheduler-operator/pkg/apis/descheduler/v1"
	"github.com/openshift/cluster-kube-descheduler-operator/pkg/operator/operatorclient"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler"
	desclient "sigs.k8s.io/descheduler/pkg/descheduler/client"
	nodeutil "sigs.k8s.io/descheduler/pkg/descheduler/node"
	"sigs.k8s.io/descheduler/pkg/framework/pluginregistry"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization/classifier"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization/normalizer"
	desutils "sigs.k8s.io/descheduler/pkg/utils"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	prometheusAuthTokenSecretKey        = "prometheusAuthToken"
	OverUtilizedSoftTaintKey            = "nodeutilization.descheduler.openshift.io/overutilized"
	OverUtilizedSoftTaintValue          = "true"
	AppropriatelyUtilizedSoftTaintKey   = "nodeutilization.descheduler.openshift.io/appropriate"
	AppropriatelyUtilizedSoftTaintValue = "true"

	// SealedNodeTaintKey marks a node that the compactor has chosen for removal.
	// It uses the NoSchedule effect (not PreferNoSchedule) so the scheduler is
	// forced to place migrated VMs elsewhere. The key stays inside the
	// "descheduler.openshift.io" namespace allowed by the soft-tainter
	// ValidatingAdmissionPolicy.
	SealedNodeTaintKey   = "compaction.descheduler.openshift.io/sealed"
	SealedNodeTaintValue = "true"

	// defaultMinCompactionReadyNodes is the fallback floor of schedulable
	// KubeVirt nodes when the CR does not set
	// profileCustomizations.devMinCompactionReadyNodes.
	defaultMinCompactionReadyNodes = 2
)

var (
	log = logf.Log.WithName("controller_nodeClassifier")
)

type softTainterArgs struct {
	useDeviationThresholds bool
	thresholds             api.ResourceThresholds
	targetThresholds       api.ResourceThresholds
	mode                   desv1.Mode

	// minCompactionReadyNodes is the floor resolved from the CR, defaulting to
	// defaultMinCompactionReadyNodes.
	minCompactionReadyNodes int32
}

type softTainter struct {
	args                              *softTainterArgs
	resourceNames                     []corev1.ResourceName
	previousPrometheusClientTransport *http.Transport
	currentPrometheusAuthToken        string
	promClient                        promapi.Client
	promQuery                         string
	promURL                           string
	client                            client.Client
	resyncPeriod                      time.Duration
	policyConfigFile                  string
	nodeUtilizationFactory            func(promapi.Client, string) NodeUtilization
	// compactionEnabled is set on every Reconcile from the descheduler policy:
	// it reports whether the KubeVirtCompactAndScale profile is in effect.
	compactionEnabled bool
}

type NodeUtilization interface {
	NodeUsageFromPrometheusMetrics(ctx context.Context) (map[string]map[corev1.ResourceName]*resource.Quantity, error)
}

type actualNodeUtilization struct {
	promClient promapi.Client
	promQuery  string
}

var _ NodeUtilization = &actualNodeUtilization{}

func (anu *actualNodeUtilization) NodeUsageFromPrometheusMetrics(ctx context.Context) (map[string]map[corev1.ResourceName]*resource.Quantity, error) {
	return nodeutilization.NodeUsageFromPrometheusMetrics(ctx, anu.promClient, anu.promQuery)
}

func (st *softTainter) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)

	// Reset the profile state on every pass so that removing the profile from
	// the CR takes effect immediately.
	st.compactionEnabled = false

	des := desv1.KubeDescheduler{}

	descheduler.SetupPlugins()
	policy, err := descheduler.LoadPolicyConfig(st.policyConfigFile, nil, pluginregistry.PluginRegistry)
	if err != nil {
		return reconcile.Result{}, err
	}

	err = st.client.Get(ctx, client.ObjectKey{
		Namespace: operatorclient.OperatorNamespace,
		Name:      operatorclient.OperatorConfigName,
	}, &des)
	if err != nil {
		logger.Error(err, "failed reading descheduler operator CR")
		return reconcile.Result{}, err
	}

	if des.Spec.DeschedulingIntervalSeconds == nil || *des.Spec.DeschedulingIntervalSeconds <= 0 {
		return reconcile.Result{}, fmt.Errorf("descheduler should have an interval set and it should be greater than 0")
	}
	st.resyncPeriod = time.Duration(*des.Spec.DeschedulingIntervalSeconds) * time.Second

	nodeSelector := labels.Everything()
	if policy.NodeSelector != nil {
		sel, err := labels.Parse(*policy.NodeSelector)
		if err != nil {
			return reconcile.Result{}, err
		}
		nodeSelector = sel
	}

	nl := corev1.NodeList{}
	err = st.client.List(ctx, &nl, &client.ListOptions{LabelSelector: nodeSelector})
	if err != nil {
		return reconcile.Result{}, err
	}
	nodeList := make([]*corev1.Node, len(nl.Items))
	for i, node := range nl.Items {
		nodeList[i] = &node
	}

	var lnargs *nodeutilization.LowNodeUtilizationArgs
	var hnuargs *nodeutilization.HighNodeUtilizationArgs

	for _, p := range policy.Profiles {
		if p.Name == string(desv1.KubeVirtCompactAndScale) {
			st.compactionEnabled = true
			for _, pc := range p.PluginConfigs {
				if pc.Name == nodeutilization.HighNodeUtilizationPluginName {
					hnuargs = pc.Args.(*nodeutilization.HighNodeUtilizationArgs)
				}
			}
		}
		if p.Name == string(desv1.KubeVirtRelieveAndMigrate) || p.Name == string(desv1.DevKubeVirtRelieveAndMigrate) {
			for _, pc := range p.PluginConfigs {
				if pc.Name == nodeutilization.LowNodeUtilizationPluginName {
					lnargs = pc.Args.(*nodeutilization.LowNodeUtilizationArgs)
				}
			}
		}
	}

	if lnargs == nil {
		// No LowNodeUtilization profile: this may be a compaction cluster.
		if st.compactionEnabled {
			if hnuargs == nil {
				return reconcile.Result{}, fmt.Errorf("profile %q is enabled but has no %q plugin config", desv1.KubeVirtCompactAndScale, nodeutilization.HighNodeUtilizationPluginName)
			}
			st.args = &softTainterArgs{
				useDeviationThresholds:  false, // HighNodeUtilization has no deviation mode
				thresholds:              hnuargs.Thresholds,
				targetThresholds:        nil, // HighNodeUtilization has no target thresholds
				mode:                    des.Spec.Mode,
				minCompactionReadyNodes: resolveMinCompactionReadyNodes(des.Spec.ProfileCustomizations),
			}
			// Predictive mode is a dry run: sealing a node removes real
			// capacity, so it must never happen outside Automatic mode. This
			// mirrors syncSoftTaints, which gates every taint write on
			// Automatic.
			if st.args.mode != desv1.Automatic {
				logger.Info("Compaction skipped: the descheduler is not in Automatic mode", "mode", st.args.mode)
				return reconcile.Result{RequeueAfter: st.resyncPeriod}, nil
			}
			if err := st.reconcileCompaction(ctx, nodeList); err != nil {
				logger.Error(err, "compaction reconciliation failed")
				return reconcile.Result{}, err
			}
			return reconcile.Result{RequeueAfter: st.resyncPeriod}, nil
		}
		logger.Info("SoftTainter is disabled, cleaning up eventual leftover taints")
		err = st.cleanAllSoftTaints(ctx, nodeList)
		if err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: st.resyncPeriod}, nil
	}

	st.args = &softTainterArgs{
		useDeviationThresholds: lnargs.UseDeviationThresholds,
		thresholds:             lnargs.Thresholds,
		targetThresholds:       lnargs.TargetThresholds,
		mode:                   des.Spec.Mode,
	}

	if lnargs.MetricsUtilization == nil || lnargs.MetricsUtilization.Prometheus == nil {
		err := fmt.Errorf("unable to read MetricsUtilization.Prometheus")
		logger.Error(err, "reconciliation failed")
		return reconcile.Result{}, err
	}
	st.promQuery = lnargs.MetricsUtilization.Prometheus.Query

	var authToken *api.AuthToken
	for _, provider := range policy.MetricsProviders {
		if provider.Prometheus != nil {
			st.promURL = provider.Prometheus.URL
			authToken = provider.Prometheus.AuthToken
		}
	}
	if st.promURL == "" {
		err := fmt.Errorf("unable to read promURL")
		logger.Error(err, "reconciliation failed")
		return reconcile.Result{}, err
	}

	if authToken != nil {
		err = st.reconcileSecretToken(ctx, authToken)
		if err != nil {
			return reconcile.Result{}, err
		}
	} else {
		err = st.reconcileInClusterSAToken()
		if err != nil {
			return reconcile.Result{}, err
		}
	}

	st.resourceNames = getResourceNames(st.args.thresholds)

	err = st.syncSoftTaints(ctx, nodeList)
	if err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: st.resyncPeriod}, nil
}

func (st *softTainter) syncSoftTaints(ctx context.Context, nodes []*corev1.Node) error {
	nodesMap := make(map[string]*corev1.Node, len(nodes))

	nu := st.nodeUtilizationFactory(st.promClient, st.promQuery)
	nodeUsages, err := nu.NodeUsageFromPrometheusMetrics(ctx)
	if err != nil {
		return err
	}

	// just to keep a symmetry with the behaviour of the descheduler
	// in case we cannot simplistically assume that the capacity for
	// the selected metric is always 100% for all the nodes
	capacities := referencedResourceListForNodesCapacity(nodes)

	for _, node := range nodes {
		if _, exists := nodeUsages[node.Name]; !exists {
			return fmt.Errorf("unable to find metric entry for %v, query: %v", node.Name, st.promQuery)
		}
		nodesMap[node.Name] = node
	}

	// usage, by default, is exposed in absolute values. we need to normalize
	// them (convert them to percentages) to be able to compare them with the
	// user provided thresholds. thresholds are already provided in percentage
	// in the <0; 100> interval.
	var usage map[string]api.ResourceThresholds
	var thresholds map[string][]api.ResourceThresholds
	if st.args.useDeviationThresholds {
		// here the thresholds provided by the user represent
		// deviations from the average so we need to treat them
		// differently. when calculating the average we only
		// need to consider the resources for which the user
		// has provided thresholds.
		usage, thresholds = assessNodesUsagesAndRelativeThresholds(
			filterResourceNames(nodeUsages, st.resourceNames),
			capacities,
			st.args.thresholds,
			st.args.targetThresholds,
		)
	} else {
		usage, thresholds = assessNodesUsagesAndStaticThresholds(
			nodeUsages,
			capacities,
			st.args.thresholds,
			st.args.targetThresholds,
		)
	}

	// classify nodes in under and over utilized. we will later try to move
	// pods from the overutilized nodes to the underutilized ones.
	nodeGroups := classifier.Classify(
		usage, thresholds,
		// underutilization criteria processing. nodes that are
		// underutilized but aren't schedulable are ignored.
		func(nodeName string, usage, threshold api.ResourceThresholds) bool {
			if nodeutil.IsNodeUnschedulable(nodesMap[nodeName]) {
				log.V(2).Info(
					"Node is unschedulable, thus not considered as underutilized",
					"node", nodesMap[nodeName],
				)
				return false
			}
			return isNodeBelowThreshold(usage, threshold)
		},
		// overutilization criteria evaluation.
		func(nodeName string, usage, threshold api.ResourceThresholds) bool {
			return isNodeAboveThreshold(usage, threshold)
		},
	)
	var lowNodes []*corev1.Node
	var apprNodes []*corev1.Node
	var highNodes []*corev1.Node
	categories := []string{"underutilized", "overutilized"}

	classifiedNodes := map[string]bool{}
	for i := range nodeGroups {
		for nodeName := range nodeGroups[i] {
			classifiedNodes[nodeName] = true
			log.Info(
				"Node has been classified",
				"category", categories[i],
				"node", nodesMap[nodeName],
				"usage", nodeUsages[nodeName],
				"usagePercentage", normalizer.Round(usage[nodeName]),
			)
			if i == 0 {
				lowNodes = append(lowNodes, nodesMap[nodeName])
			} else if i == 1 {
				highNodes = append(highNodes, nodesMap[nodeName])
			}

		}
	}

	// avoid soft-tainting appropriately utilized nodes
	// when we don't have underutilized
	category := "appropriatelyutilized"
	if len(lowNodes) == 0 {
		category = categories[0]
	}

	// log nodes that are appropriately utilized.
	for nodeName := range nodesMap {
		if !classifiedNodes[nodeName] {
			log.Info(
				"Node has been classified",
				"category", category,
				"node", nodesMap[nodeName],
				"usage", nodeUsages[nodeName],
				"usagePercentage", normalizer.Round(usage[nodeName]),
			)
			if category == categories[0] {
				lowNodes = append(lowNodes, nodesMap[nodeName])
			} else {
				apprNodes = append(apprNodes, nodesMap[nodeName])
			}
		}
	}
	log.Info("Number of under utilized nodes", "totalNumber", len(lowNodes))
	log.Info("Number of appropriately utilized nodes", "totalNumber", len(apprNodes))
	log.Info("Number of over utilized nodes", "totalNumber", len(highNodes))

	if st.args.mode == desv1.Automatic {
		err := st.taintNodes(ctx, lowNodes, apprNodes, highNodes)
		if err != nil {
			return err
		}
	}

	return nil
}

func (st *softTainter) cleanAllSoftTaints(ctx context.Context, nodes []*corev1.Node) error {
	log.Info("cleaning all soft taints on nodes")

	// Limit concurrent API calls to avoid overwhelming the API server
	// 20 is a reasonable default for most clusters
	var g errgroup.Group
	g.SetLimit(20)

	var mu sync.Mutex
	var errs []error

	collectErr := func(err error) {
		if err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
	}

	for _, node := range nodes {
		node := node
		g.Go(func() error {
			log.Info("cleaning soft taints on node", "node", node.Name)
			for k, v := range map[string]string{
				AppropriatelyUtilizedSoftTaintKey: AppropriatelyUtilizedSoftTaintValue,
				OverUtilizedSoftTaintKey:          OverUtilizedSoftTaintValue,
				SealedNodeTaintKey:                SealedNodeTaintValue,
			} {
				collectErr(st.dropTaint(ctx, node, k, v))
			}
			return nil // to avoid cancelling the execution on errors
		})
	}

	_ = g.Wait() // Ignore error since we're aggregating errors in errs

	return utilerrors.NewAggregate(errs)
}

// Compaction contract:
//   1. The compactor only *seals* (NoSchedule taint) and *drains*. It never
//      deletes machines or drains pods itself — the descheduler profile from
//      plan 001 performs the evictions and KubeVirt performs live migrations.
//   2. A sealed node that reaches zero migratable pods is left sealed and
//      empty, ready for the Machine API / cluster-autoscaler / an operator to
//      remove. This controller does not know how to delete a machine, and must
//      not learn: machine provisioning is a different subsystem with different
//      failure modes.
//   3. Unsealing is always safe and always allowed: removing the taint returns
//      the node to service without any data movement.

// nodeDrainability describes whether a node can be sealed and removed.
type nodeDrainability struct {
	// migratablePods counts pods that can move elsewhere: pods with PVCs
	// (live-migratable VMs) and ordinary pods.
	migratablePods int
	// blockedPods counts pods that cannot move: pods with local storage,
	// which would restart rather than migrate.
	blockedPods int
	// reason is a short human-readable explanation for logs and events.
	reason string
}

// assessNodeDrainability inspects the pods currently assigned to node and
// decides whether sealing it is safe.
func (st *softTainter) assessNodeDrainability(ctx context.Context, node *corev1.Node) (nodeDrainability, error) {
	result := nodeDrainability{}
	pods := corev1.PodList{}
	if err := st.client.List(ctx, &pods, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return result, fmt.Errorf("unable to list pods on node %q: %v", node.Name, err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		// Mirror the eviction policy from plan 001: pods with local storage
		// cannot live-migrate, so they must not be stranded on a sealed node.
		if hasLocalStorage(pod) {
			result.blockedPods++
			continue
		}
		result.migratablePods++
	}
	if result.blockedPods > 0 {
		result.reason = fmt.Sprintf("%d pod(s) with local storage cannot be migrated", result.blockedPods)
	}
	return result, nil
}

// hasLocalStorage reports whether the pod uses an emptyDir or hostPath volume,
// matching the descheduler's own local-storage protection.
func hasLocalStorage(pod *corev1.Pod) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.EmptyDir != nil || volume.HostPath != nil {
			return true
		}
	}
	return false
}

// podsOnNode returns the pods currently assigned to node.
func (st *softTainter) podsOnNode(ctx context.Context, nodeName string) ([]*corev1.Pod, error) {
	podList := corev1.PodList{}
	if err := st.client.List(ctx, &podList, client.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		return nil, fmt.Errorf("unable to list pods on node %q: %v", nodeName, err)
	}
	pods := make([]*corev1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pods = append(pods, &podList.Items[i])
	}
	return pods, nil
}

// requestedUsagesForNodes returns, for every node, the sum of the pod resource
// requests for the resources the compaction thresholds care about. Unlike the
// LowNodeUtilization path this never consults Prometheus: HighNodeUtilization
// packs on requested resources.
func (st *softTainter) requestedUsagesForNodes(ctx context.Context, nodes []*corev1.Node) (map[string]api.ReferencedResourceList, error) {
	resourceNames := getResourceNames(st.args.thresholds)
	usages := make(map[string]api.ReferencedResourceList, len(nodes))
	for _, node := range nodes {
		pods, err := st.podsOnNode(ctx, node.Name)
		if err != nil {
			return nil, err
		}
		usage := api.ReferencedResourceList{}
		for _, resourceName := range resourceNames {
			if resourceName == corev1.ResourcePods {
				usage[resourceName] = resource.NewQuantity(int64(len(pods)), resource.DecimalSI)
				continue
			}
			total := resource.NewQuantity(0, resource.DecimalSI)
			for _, pod := range pods {
				quantity := desutils.GetResourceRequestQuantity(pod, resourceName)
				total.Add(quantity)
			}
			usage[resourceName] = total
		}
		usages[node.Name] = usage
	}
	return usages, nil
}

// compactionCandidates returns the schedulable nodes that are below the
// compaction packing threshold, sorted ascending by total usage: the emptiest
// node is the best candidate to remove. Unlike LowNodeUtilization there is no
// target threshold, so only the low span is consulted.
func (st *softTainter) compactionCandidates(ctx context.Context, nodes []*corev1.Node) ([]*corev1.Node, error) {
	if len(nodes) == 0 {
		return nil, nil
	}
	usages, err := st.requestedUsagesForNodes(ctx, nodes)
	if err != nil {
		return nil, err
	}
	capacities := referencedResourceListForNodesCapacity(nodes)

	usage, thresholds := assessNodesUsagesAndStaticThresholds(
		usages,
		capacities,
		st.args.thresholds,
		// HighNodeUtilization has no target threshold; the second span is
		// never consulted on this path.
		nil,
	)

	nodesMap := make(map[string]*corev1.Node, len(nodes))
	for _, node := range nodes {
		nodesMap[node.Name] = node
	}

	nodeGroups := classifier.Classify(
		usage, thresholds,
		// underutilized criteria: a node is a compaction candidate purely by
		// being below the packing threshold, but only if it is schedulable.
		func(nodeName string, usage, threshold api.ResourceThresholds) bool {
			if nodeutil.IsNodeUnschedulable(nodesMap[nodeName]) {
				return false
			}
			return isNodeBelowThreshold(usage, threshold)
		},
	)

	candidates := make([]*corev1.Node, 0, len(nodeGroups[0]))
	for nodeName := range nodeGroups[0] {
		log.Info(
			"Compaction: node is below the packing threshold",
			"node", nodeName,
			"usage", usages[nodeName],
			"usagePercentage", normalizer.Round(usage[nodeName]),
		)
		candidates = append(candidates, nodesMap[nodeName])
	}

	// Sort ascending by total usage, mirroring sortNodesByUsage(nodes, true)
	// in the descheduler's HighNodeUtilization plugin.
	sort.Slice(candidates, func(i, j int) bool {
		return totalUsageValue(usages[candidates[i].Name]) < totalUsageValue(usages[candidates[j].Name])
	})

	return candidates, nil
}

// totalUsageValue sums the usage quantities of a node, using milli-units for
// CPU so that CPU and memory are not compared with wildly different scales.
func totalUsageValue(usage api.ReferencedResourceList) int64 {
	total := int64(0)
	for resourceName, quantity := range usage {
		if quantity == nil {
			continue
		}
		if resourceName == corev1.ResourceCPU {
			total += quantity.MilliValue()
			continue
		}
		total += quantity.Value()
	}
	return total
}

// resolveMinCompactionReadyNodes returns the configured compaction floor, or the
// default when the CR does not set it. A value below 1 is rejected at the API
// level, so anything non-nil here is usable as-is.
func resolveMinCompactionReadyNodes(pc *desv1.ProfileCustomizations) int32 {
	if pc != nil && pc.DevMinCompactionReadyNodes != nil {
		return *pc.DevMinCompactionReadyNodes
	}
	return defaultMinCompactionReadyNodes
}

// reconcileCompaction seals at most one underutilized, drainable KubeVirt node
// per invocation, and unseals nodes that are no longer candidates.
func (st *softTainter) reconcileCompaction(ctx context.Context, nodes []*corev1.Node) error {
	// 1. Partition nodes into ready (schedulable, unsealed) and sealed.
	var ready, sealed []*corev1.Node
	for _, node := range nodes {
		if nodeutil.IsNodeUnschedulable(node) {
			continue
		}
		if taints.TaintExists(node.Spec.Taints, &corev1.Taint{Key: SealedNodeTaintKey, Value: SealedNodeTaintValue, Effect: corev1.TaintEffectNoSchedule}) {
			sealed = append(sealed, node)
			continue
		}
		ready = append(ready, node)
	}

	// 2. Unseal any sealed node that is no longer below the packing threshold:
	//    the cluster now needs its capacity back. Unsealing is always safe, so
	//    this runs before the floor check.
	if err := st.unsealNodesAboveThreshold(ctx, sealed); err != nil {
		return err
	}

	// 3. Never go below the floor. This *pauses* compaction: it does not
	//    unseal the remaining sealed nodes. Those are the drained, empty result
	//    of the compaction already performed and are awaiting machine removal;
	//    unsealing them here would restart the compaction on the next pass and
	//    oscillate forever.
	if int32(len(ready)+len(sealed)) <= st.args.minCompactionReadyNodes {
		log.V(1).Info("Compaction: the configured floor is not below the number of KubeVirt nodes, so no node can ever be sealed", "nodes", len(ready)+len(sealed), "floor", st.args.minCompactionReadyNodes)
	}
	if int32(len(ready)) <= st.args.minCompactionReadyNodes {
		log.Info("Compaction paused: not enough ready KubeVirt nodes", "ready", len(ready), "floor", st.args.minCompactionReadyNodes)
		return nil
	}

	// 4. Sort candidates by usage ascending: the emptiest node is the best
	//    candidate to remove.
	candidates, err := st.compactionCandidates(ctx, ready)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		log.Info("Compaction: no node below the packing threshold, nothing to seal")
		return nil
	}

	// 5. Seal at most one node, and only if it is fully drainable.
	for _, candidate := range candidates {
		drainability, err := st.assessNodeDrainability(ctx, candidate)
		if err != nil {
			return err
		}
		if drainability.blockedPods > 0 {
			log.Info("Compaction: skipping node", "node", candidate.Name, "reason", drainability.reason)
			continue
		}
		updated, err := AddOrUpdateTaint(ctx, st.client, candidate, SealedNodeTaintKey, SealedNodeTaintValue, corev1.TaintEffectNoSchedule)
		if err != nil {
			return err
		}
		if updated {
			log.Info("Compaction: sealed node for removal", "node", candidate.Name, "migratablePods", drainability.migratablePods)
		}
		return nil
	}

	log.Info("Compaction: every candidate has non-migratable pods, nothing to seal")
	return nil
}

// unsealNodesAboveThreshold removes the sealed taint from nodes that are no
// longer below the compaction packing threshold: their capacity is needed
// again, and unsealing is always safe.
func (st *softTainter) unsealNodesAboveThreshold(ctx context.Context, sealed []*corev1.Node) error {
	if len(sealed) == 0 {
		return nil
	}
	// A sealed node is never a compaction candidate (it is excluded from
	// "ready"), so compare it against the threshold explicitly.
	below, err := st.compactionCandidates(ctx, sealed)
	if err != nil {
		return err
	}
	stillBelow := make(map[string]bool, len(below))
	for _, node := range below {
		stillBelow[node.Name] = true
	}
	for _, node := range sealed {
		if stillBelow[node.Name] {
			continue
		}
		removed, err := RemoveTaint(ctx, st.client, node, SealedNodeTaintKey)
		if err != nil {
			return err
		}
		if removed {
			log.Info("Compaction: unsealed node, it is no longer below the packing threshold", "node", node.Name)
		}
	}
	return nil
}

func (st *softTainter) reconcileInClusterSAToken() error {
	// Read the sa token and assume it has the sufficient permissions to authenticate
	cfg, err := rest.InClusterConfig()
	if err == nil {
		if st.currentPrometheusAuthToken != cfg.BearerToken {
			log.V(2).Info("Creating Prometheus client (with SA token)")
			prometheusClient, transport, err := desclient.CreatePrometheusClient(st.promURL, cfg.BearerToken)
			if err != nil {
				return fmt.Errorf("unable to create a prometheus client: %v", err)
			}
			st.promClient = prometheusClient
			if st.previousPrometheusClientTransport != nil {
				st.previousPrometheusClientTransport.CloseIdleConnections()
			}
			st.previousPrometheusClientTransport = transport
			st.currentPrometheusAuthToken = cfg.BearerToken
		}
		return nil
	}
	if errors.Is(err, rest.ErrNotInCluster) {
		return nil
	}
	return fmt.Errorf("unexpected error when reading in cluster config: %v", err)
}

func (st *softTainter) reconcileSecretToken(ctx context.Context, authToken *api.AuthToken) error {
	authTokenSecret := authToken.SecretReference
	if authTokenSecret == nil || authTokenSecret.Namespace == "" || authTokenSecret.Name == "" {
		return fmt.Errorf("prometheus metrics source configuration is missing authentication token secret")
	}

	secret := corev1.Secret{}
	err := st.client.Get(ctx, client.ObjectKey{
		Namespace: authTokenSecret.Namespace,
		Name:      authTokenSecret.Name,
	}, &secret)
	if err != nil {
		return err
	}
	token := string(secret.Data[prometheusAuthTokenSecretKey])
	if token == "" {
		return fmt.Errorf("prometheus authentication token secret missing %q data or empty", prometheusAuthTokenSecretKey)
	}
	if st.currentPrometheusAuthToken == token {
		return nil
	} else {
		prometheusClient, transport, err := desclient.CreatePrometheusClient(st.promURL, token)
		if err != nil {
			return fmt.Errorf("unable to create a prometheus client: %v", err)
		}
		st.promClient = prometheusClient
		if st.previousPrometheusClientTransport != nil {
			st.previousPrometheusClientTransport.CloseIdleConnections()
		}
		st.previousPrometheusClientTransport = transport
		st.currentPrometheusAuthToken = token
	}
	return nil
}

func (st *softTainter) taintNodes(ctx context.Context, lowNodes, apprNodes, highNodes []*corev1.Node) error {
	log.Info("reconciling soft taints on nodes")

	// Limit concurrent API calls to avoid overwhelming the API server
	// 20 is a reasonable default for most clusters
	var g errgroup.Group
	g.SetLimit(20)

	var mu sync.Mutex
	var errs []error

	collectErr := func(err error) {
		if err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
	}

	for _, node := range lowNodes {
		node := node
		g.Go(func() error {
			log.Info("reconciling soft taints on nodes - low", "node", node.Name)
			for k, v := range map[string]string{
				AppropriatelyUtilizedSoftTaintKey: AppropriatelyUtilizedSoftTaintValue,
				OverUtilizedSoftTaintKey:          OverUtilizedSoftTaintValue,
			} {
				collectErr(st.dropTaint(ctx, node, k, v))
			}
			return nil // to avoid cancelling the execution on errors
		})
	}

	for _, node := range apprNodes {
		node := node
		g.Go(func() error {
			log.Info("reconciling soft taints on nodes - appr", "node", node.Name)
			for k, v := range map[string]string{
				AppropriatelyUtilizedSoftTaintKey: AppropriatelyUtilizedSoftTaintValue,
			} {
				collectErr(st.addTaint(ctx, node, k, v))
			}
			for k, v := range map[string]string{
				OverUtilizedSoftTaintKey: OverUtilizedSoftTaintValue,
			} {
				collectErr(st.dropTaint(ctx, node, k, v))
			}
			return nil // to avoid cancelling the execution on errors
		})
	}

	for _, node := range highNodes {
		node := node // capture loop variable
		g.Go(func() error {
			log.Info("reconciling soft taints on nodes - high", "node", node.Name)
			for k, v := range map[string]string{
				AppropriatelyUtilizedSoftTaintKey: AppropriatelyUtilizedSoftTaintValue,
				OverUtilizedSoftTaintKey:          OverUtilizedSoftTaintValue,
			} {
				collectErr(st.addTaint(ctx, node, k, v))
			}
			return nil // to avoid cancelling the execution on errors
		})
	}

	_ = g.Wait() // Ignore error since we're aggregating errors in errs

	return utilerrors.NewAggregate(errs)
}

func (st *softTainter) addTaint(ctx context.Context, node *corev1.Node, k, v string) error {
	rNode := &corev1.Node{}
	err := st.client.Get(ctx, client.ObjectKey{
		Namespace: node.Namespace,
		Name:      node.Name,
	}, rNode)
	if err != nil {
		return err
	}
	if !taints.TaintExists(rNode.Spec.Taints, &corev1.Taint{Key: k, Value: v, Effect: corev1.TaintEffectPreferNoSchedule}) {
		updated, err := AddOrUpdateTaint(ctx, st.client, rNode, k, v, corev1.TaintEffectPreferNoSchedule)
		if err != nil {
			log.Error(err, "Failed adding soft taint to node, check RBAC", "node", rNode.Name, "taint", k)
			return err
		} else if updated {
			log.Info("The soft taint got added to the node", "node", rNode.Name, "taint", k)
		}
	}
	return nil
}

func (st *softTainter) dropTaint(ctx context.Context, node *corev1.Node, k, v string) error {
	rNode := &corev1.Node{}
	err := st.client.Get(ctx, client.ObjectKey{
		Namespace: node.Namespace,
		Name:      node.Name,
	}, rNode)
	if err != nil {
		return err
	}
	if taints.TaintKeyExists(rNode.Spec.Taints, k) {
		removed, err := RemoveTaint(ctx, st.client, rNode, k)
		if err != nil {
			log.Error(err, "Failed removing soft taint from node, check RBAC", "node", rNode.Name, "taint", k)
			return err
		} else if removed {
			log.Info("The soft taint got removed from the node", "node", rNode.Name, "taint", k)
		}
	}
	return nil
}

func newNodeUtilizationFactory(promClient promapi.Client, promQuery string) NodeUtilization {
	return &actualNodeUtilization{promClient: promClient, promQuery: promQuery}
}

// TickLimitedControllerRateLimiter is a no-arg constructor for a rate limiter for a workqueue.  It has
// both overall and per-item rate limiting.  The overall is a token bucket and the per-item is exponential
// up to tickLimit instead of the default value of 1000 seconds
func TickLimitedControllerRateLimiter[T comparable](tickLimit time.Duration) workqueue.TypedRateLimiter[T] {
	return workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[T](5*time.Millisecond, tickLimit),
		// 10 qps, 100 bucket size.  This is only for retry speed, and it's only the overall factor (not per item)
		&workqueue.TypedBucketRateLimiter[T]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}

// RegisterReconciler creates a new Reconciler and registers it into manager.
func RegisterReconciler(mgr manager.Manager, policyConfigFile string) error {
	des := desv1.KubeDescheduler{}
	err := mgr.GetAPIReader().Get(context.Background(), client.ObjectKey{
		Namespace: operatorclient.OperatorNamespace,
		Name:      operatorclient.OperatorConfigName,
	}, &des)
	if err != nil {
		log.Error(err, "failed reading descheduler operator CR")
		return err
	}

	if des.Spec.DeschedulingIntervalSeconds == nil || *des.Spec.DeschedulingIntervalSeconds <= 0 {
		return fmt.Errorf("descheduler should have an interval set and it should be greater than 0")
	}
	resyncPeriod := time.Duration(*des.Spec.DeschedulingIntervalSeconds) * time.Second

	// The compaction path needs to list the pods assigned to a node, so index
	// pods by spec.nodeName in the cache. This must be registered before the
	// controller starts consuming the cache.
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&corev1.Pod{},
		"spec.nodeName",
		func(rawObj client.Object) []string {
			pod, ok := rawObj.(*corev1.Pod)
			if !ok || pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		},
	); err != nil {
		return fmt.Errorf("unable to register spec.nodeName field index: %v", err)
	}

	// Create a new controller
	c, err := controller.New(
		"nodeclassification-controller",
		mgr,
		controller.Options{
			RateLimiter: TickLimitedControllerRateLimiter[reconcile.Request](resyncPeriod),
			Reconciler: &softTainter{
				client:                 mgr.GetClient(),
				resyncPeriod:           resyncPeriod,
				policyConfigFile:       policyConfigFile,
				nodeUtilizationFactory: newNodeUtilizationFactory,
			},
		},
	)
	if err != nil {
		return err
	}

	// Watch and enqueue to sync on descheduler configuration
	if err := c.Watch(source.Kind(mgr.GetCache(), &desv1.KubeDescheduler{}, &handler.TypedEnqueueRequestForObject[*desv1.KubeDescheduler]{})); err != nil {
		return err
	}

	return nil
}
