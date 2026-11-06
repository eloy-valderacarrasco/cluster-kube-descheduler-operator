package softtainter

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	operatorv1 "github.com/openshift/api/operator/v1"
	promapi "github.com/prometheus/client_golang/api"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/pointer"

	deschedulerv1 "github.com/openshift/cluster-kube-descheduler-operator/pkg/apis/descheduler/v1"
	"github.com/openshift/cluster-kube-descheduler-operator/pkg/operator/operatorclient"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	pkgDirectory = "pkg/softtainter"
	testFilesLoc = "testdata"
)

type MockNodeUtilization struct {
	mockResults map[string]map[corev1.ResourceName]*resource.Quantity
	mockErrors  error
}

func (mnu *MockNodeUtilization) NodeUsageFromPrometheusMetrics(_ context.Context) (map[string]map[corev1.ResourceName]*resource.Quantity, error) {
	return mnu.mockResults, mnu.mockErrors
}

func (mnu *MockNodeUtilization) newNodeUtilizationFactory(_ promapi.Client, _ string) NodeUtilization {
	return &MockNodeUtilization{
		mockResults: mnu.mockResults,
		mockErrors:  mnu.mockErrors,
	}
}

func TestReconcile(t *testing.T) {

	ctx := context.Background()

	resourcesSchemeFuncs := []func(*apiruntime.Scheme) error{
		corev1.AddToScheme,
		appsv1.AddToScheme,
		operatorv1.Install,
		deschedulerv1.AddToScheme,
	}

	scheme := runtime.NewScheme()

	for _, f := range resourcesSchemeFuncs {
		err := f(scheme)
		if err != nil {
			t.Errorf("Failed building scheme for the fake client: %v", err)
		}
	}

	tests := []struct {
		description     string
		nodes           []*corev1.Node
		expectednodes   []*corev1.Node
		nodeUtilization MockNodeUtilization
		expectedErr     string
		kubedescheduler deschedulerv1.KubeDescheduler
		testfilename    string
	}{
		{
			description: "3 nodes, no taints, 1 underutilized, 1 appropriately utilized, 1 overutilized",
			nodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, false),
				buildTestNodeWithTaints("node2", true, false, false, false),
				buildTestNodeWithTaints("node3", true, false, false, false),
			},
			expectednodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, false),
				buildTestNodeWithTaints("node2", true, true, false, false),
				buildTestNodeWithTaints("node3", true, true, true, false),
			},
			nodeUtilization: MockNodeUtilization{
				mockResults: map[string]map[corev1.ResourceName]*resource.Quantity{
					"node1": {"MetricResource": resource.NewQuantity(10, resource.BinarySI)},
					"node2": {"MetricResource": resource.NewQuantity(50, resource.BinarySI)},
					"node3": {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
				},
				mockErrors: nil,
			},
			kubedescheduler: deschedulerv1.KubeDescheduler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: operatorclient.OperatorNamespace,
					Name:      operatorclient.OperatorConfigName,
				},
				Spec: deschedulerv1.KubeDeschedulerSpec{
					DeschedulingIntervalSeconds: pointer.Int32(30),
					Mode:                        deschedulerv1.Automatic,
					ProfileCustomizations: &deschedulerv1.ProfileCustomizations{
						DevDeviationThresholds: &deschedulerv1.LowDeviationThreshold,
					},
				},
			},
			testfilename: "policy.yaml",
		},
		{
			description: "3 nodes, wrong taints, 1 underutilized, 1 appropriately utilized, 1 overutilized",
			nodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, true, true, false),
				buildTestNodeWithTaints("node2", true, false, true, false),
				buildTestNodeWithTaints("node3", true, false, false, false),
			},
			expectednodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, false),
				buildTestNodeWithTaints("node2", true, true, false, false),
				buildTestNodeWithTaints("node3", true, true, true, false),
			},
			nodeUtilization: MockNodeUtilization{
				mockResults: map[string]map[corev1.ResourceName]*resource.Quantity{
					"node1": {"MetricResource": resource.NewQuantity(10, resource.BinarySI)},
					"node2": {"MetricResource": resource.NewQuantity(50, resource.BinarySI)},
					"node3": {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
				},
				mockErrors: nil,
			},
			kubedescheduler: deschedulerv1.KubeDescheduler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: operatorclient.OperatorNamespace,
					Name:      operatorclient.OperatorConfigName,
				},
				Spec: deschedulerv1.KubeDeschedulerSpec{
					DeschedulingIntervalSeconds: pointer.Int32(30),
					Mode:                        deschedulerv1.Automatic,
					ProfileCustomizations: &deschedulerv1.ProfileCustomizations{
						DevDeviationThresholds: &deschedulerv1.LowDeviationThreshold,
					},
				},
			},
			testfilename: "policy.yaml",
		},
		{
			description: "3 nodes, extra taints, 1 underutilized, 1 appropriately utilized, 1 overutilized",
			nodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, true),
				buildTestNodeWithTaints("node2", true, false, false, true),
				buildTestNodeWithTaints("node3", true, false, false, true),
			},
			expectednodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, true),
				buildTestNodeWithTaints("node2", true, true, false, true),
				buildTestNodeWithTaints("node3", true, true, true, true),
			},
			nodeUtilization: MockNodeUtilization{
				mockResults: map[string]map[corev1.ResourceName]*resource.Quantity{
					"node1": {"MetricResource": resource.NewQuantity(10, resource.BinarySI)},
					"node2": {"MetricResource": resource.NewQuantity(50, resource.BinarySI)},
					"node3": {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
				},
				mockErrors: nil,
			},
			kubedescheduler: deschedulerv1.KubeDescheduler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: operatorclient.OperatorNamespace,
					Name:      operatorclient.OperatorConfigName,
				},
				Spec: deschedulerv1.KubeDeschedulerSpec{
					DeschedulingIntervalSeconds: pointer.Int32(30),
					Mode:                        deschedulerv1.Automatic,
					ProfileCustomizations: &deschedulerv1.ProfileCustomizations{
						DevDeviationThresholds: &deschedulerv1.LowDeviationThreshold,
					},
				},
			},
			testfilename: "policy.yaml",
		},
		{
			description: "It should ignore nodes that are not schedulable by KubeVirt",
			nodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, true),
				buildTestNodeWithTaints("node2", true, false, false, true),
				buildTestNodeWithTaints("node3", true, false, false, true),
				buildTestNodeWithTaints("nodeNS4", false, false, false, true),
				buildTestNodeWithTaints("nodeNS5", false, false, false, true),
			},
			expectednodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, true),
				buildTestNodeWithTaints("node2", true, true, false, true),
				buildTestNodeWithTaints("node3", true, true, true, true),
				buildTestNodeWithTaints("nodeNS4", false, false, false, true),
				buildTestNodeWithTaints("nodeNS5", false, false, false, true),
			},
			nodeUtilization: MockNodeUtilization{
				mockResults: map[string]map[corev1.ResourceName]*resource.Quantity{
					"node1":   {"MetricResource": resource.NewQuantity(10, resource.BinarySI)},
					"node2":   {"MetricResource": resource.NewQuantity(50, resource.BinarySI)},
					"node3":   {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
					"nodeNS4": {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
					"nodeNS5": {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
				},
				mockErrors: nil,
			},
			kubedescheduler: deschedulerv1.KubeDescheduler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: operatorclient.OperatorNamespace,
					Name:      operatorclient.OperatorConfigName,
				},
				Spec: deschedulerv1.KubeDeschedulerSpec{
					DeschedulingIntervalSeconds: pointer.Int32(30),
					Mode:                        deschedulerv1.Automatic,
					ProfileCustomizations: &deschedulerv1.ProfileCustomizations{
						DevDeviationThresholds: &deschedulerv1.LowDeviationThreshold,
					},
				},
			},
			testfilename: "policy.yaml",
		},
		{
			description: "3 nodes, equal metric value (low)",
			nodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, true),
				buildTestNodeWithTaints("node2", true, false, false, true),
				buildTestNodeWithTaints("node3", true, false, false, true),
			},
			expectednodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, true),
				buildTestNodeWithTaints("node2", true, false, false, true),
				buildTestNodeWithTaints("node3", true, false, false, true),
			},
			nodeUtilization: MockNodeUtilization{
				mockResults: map[string]map[corev1.ResourceName]*resource.Quantity{
					"node1": {"MetricResource": resource.NewQuantity(10, resource.BinarySI)},
					"node2": {"MetricResource": resource.NewQuantity(10, resource.BinarySI)},
					"node3": {"MetricResource": resource.NewQuantity(10, resource.BinarySI)},
				},
				mockErrors: nil,
			},
			kubedescheduler: deschedulerv1.KubeDescheduler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: operatorclient.OperatorNamespace,
					Name:      operatorclient.OperatorConfigName,
				},
				Spec: deschedulerv1.KubeDeschedulerSpec{
					DeschedulingIntervalSeconds: pointer.Int32(30),
					Mode:                        deschedulerv1.Automatic,
					ProfileCustomizations: &deschedulerv1.ProfileCustomizations{
						DevDeviationThresholds: &deschedulerv1.LowDeviationThreshold,
					},
				},
			},
			testfilename: "policy.yaml",
		},
		{
			description: "3 nodes, equal metric value (high)",
			nodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, true),
				buildTestNodeWithTaints("node2", true, false, false, true),
				buildTestNodeWithTaints("node3", true, false, false, true),
			},
			expectednodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, true),
				buildTestNodeWithTaints("node2", true, false, false, true),
				buildTestNodeWithTaints("node3", true, false, false, true),
			},
			nodeUtilization: MockNodeUtilization{
				mockResults: map[string]map[corev1.ResourceName]*resource.Quantity{
					"node1": {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
					"node2": {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
					"node3": {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
				},
				mockErrors: nil,
			},
			kubedescheduler: deschedulerv1.KubeDescheduler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: operatorclient.OperatorNamespace,
					Name:      operatorclient.OperatorConfigName,
				},
				Spec: deschedulerv1.KubeDeschedulerSpec{
					DeschedulingIntervalSeconds: pointer.Int32(30),
					Mode:                        deschedulerv1.Automatic,
					ProfileCustomizations: &deschedulerv1.ProfileCustomizations{
						DevDeviationThresholds: &deschedulerv1.LowDeviationThreshold,
					},
				},
			},
			testfilename: "policy.yaml",
		},
		{
			description: "3 nodes, RelieveAndMigrate is disabled",
			nodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, true),
				buildTestNodeWithTaints("node2", true, false, false, true),
				buildTestNodeWithTaints("node3", true, false, false, true),
			},
			expectednodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, true),
				buildTestNodeWithTaints("node2", true, false, false, true),
				buildTestNodeWithTaints("node3", true, false, false, true),
			},
			nodeUtilization: MockNodeUtilization{
				mockResults: map[string]map[corev1.ResourceName]*resource.Quantity{
					"node1": {"MetricResource": resource.NewQuantity(10, resource.BinarySI)},
					"node2": {"MetricResource": resource.NewQuantity(50, resource.BinarySI)},
					"node3": {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
				},
				mockErrors: nil,
			},
			kubedescheduler: deschedulerv1.KubeDescheduler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: operatorclient.OperatorNamespace,
					Name:      operatorclient.OperatorConfigName,
				},
				Spec: deschedulerv1.KubeDeschedulerSpec{
					DeschedulingIntervalSeconds: pointer.Int32(30),
					Mode:                        deschedulerv1.Automatic,
					ProfileCustomizations: &deschedulerv1.ProfileCustomizations{
						DevDeviationThresholds: &deschedulerv1.LowDeviationThreshold,
					},
				},
			},
			testfilename: "policyNoRelieveAndMigrate.yaml",
		},
		{
			description: "3 nodes, RelieveAndMigrate is disabled, leftover taints",
			nodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, true, false, true),
				buildTestNodeWithTaints("node2", true, false, true, true),
				buildTestNodeWithTaints("node3", true, true, true, true),
			},
			expectednodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, true),
				buildTestNodeWithTaints("node2", true, false, false, true),
				buildTestNodeWithTaints("node3", true, false, false, true),
			},
			nodeUtilization: MockNodeUtilization{
				mockResults: map[string]map[corev1.ResourceName]*resource.Quantity{
					"node1": {"MetricResource": resource.NewQuantity(10, resource.BinarySI)},
					"node2": {"MetricResource": resource.NewQuantity(50, resource.BinarySI)},
					"node3": {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
				},
				mockErrors: nil,
			},
			kubedescheduler: deschedulerv1.KubeDescheduler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: operatorclient.OperatorNamespace,
					Name:      operatorclient.OperatorConfigName,
				},
				Spec: deschedulerv1.KubeDeschedulerSpec{
					DeschedulingIntervalSeconds: pointer.Int32(30),
					Mode:                        deschedulerv1.Automatic,
					ProfileCustomizations: &deschedulerv1.ProfileCustomizations{
						DevDeviationThresholds: &deschedulerv1.LowDeviationThreshold,
					},
				},
			},
			testfilename: "policyNoRelieveAndMigrate.yaml",
		},
		{
			description: "3 nodes, production-like policy with KubevirtMigrationAware plugin configured",
			nodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, false),
				buildTestNodeWithTaints("node2", true, false, false, false),
				buildTestNodeWithTaints("node3", true, false, false, false),
			},
			expectednodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, false, false, false),
				buildTestNodeWithTaints("node2", true, true, false, false),
				buildTestNodeWithTaints("node3", true, true, true, false),
			},
			nodeUtilization: MockNodeUtilization{
				mockResults: map[string]map[corev1.ResourceName]*resource.Quantity{
					"node1": {"MetricResource": resource.NewQuantity(10, resource.BinarySI)},
					"node2": {"MetricResource": resource.NewQuantity(50, resource.BinarySI)},
					"node3": {"MetricResource": resource.NewQuantity(90, resource.BinarySI)},
				},
				mockErrors: nil,
			},
			kubedescheduler: deschedulerv1.KubeDescheduler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: operatorclient.OperatorNamespace,
					Name:      operatorclient.OperatorConfigName,
				},
				Spec: deschedulerv1.KubeDeschedulerSpec{
					DeschedulingIntervalSeconds: pointer.Int32(30),
					Mode:                        deschedulerv1.Automatic,
					ProfileCustomizations: &deschedulerv1.ProfileCustomizations{
						DevDeviationThresholds: &deschedulerv1.LowDeviationThreshold,
					},
				},
			},
			testfilename: "policyWithKubevirtMigrationAware.yaml",
		},
		{
			description: "zeroed out descheduling interval",
			nodes:       []*corev1.Node{},
			expectedErr: "descheduler should have an interval set and it should be greater than 0",
			kubedescheduler: deschedulerv1.KubeDescheduler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: operatorclient.OperatorNamespace,
					Name:      operatorclient.OperatorConfigName,
				},
				Spec: deschedulerv1.KubeDeschedulerSpec{
					DeschedulingIntervalSeconds: pointer.Int32(0),
					Mode:                        deschedulerv1.Automatic,
				},
			},
			testfilename: "policy.yaml",
		},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {

			objects := []client.Object{&tc.kubedescheduler}
			for _, node := range tc.nodes {
				objects = append(objects, node)
			}
			cl := fake.NewClientBuilder().WithObjects(objects...).WithScheme(scheme).Build()
			st := softTainter{
				client:                 cl,
				resyncPeriod:           60 * time.Second,
				policyConfigFile:       path.Join(getTestFilesLocation(t), tc.testfilename),
				nodeUtilizationFactory: tc.nodeUtilization.newNodeUtilizationFactory,
			}
			result, err := st.Reconcile(ctx, reconcile.Request{})
			if err != nil {
				if tc.expectedErr == "" {
					t.Errorf("Test %#v failed, unexpected error: %v", tc.description, err)
				} else if !strings.Contains(err.Error(), tc.expectedErr) {
					t.Errorf("Test %#v failed, test case error: %v, expected error to contain: %v", tc.description, err, tc.expectedErr)
				}
			} else if tc.expectedErr != "" {
				t.Errorf("Test %#v failed, expected error: %v, but got no error", tc.description, tc.expectedErr)
			}

			if tc.kubedescheduler.Spec.DeschedulingIntervalSeconds == nil || result.RequeueAfter != time.Duration(*tc.kubedescheduler.Spec.DeschedulingIntervalSeconds)*time.Second {
				t.Errorf("Test %#v failed, RequeueAfter is not correctly set", tc.description)
			}

			for _, expectedNode := range tc.expectednodes {
				node := &corev1.Node{}
				err := cl.Get(ctx, client.ObjectKeyFromObject(expectedNode), node)
				if err != nil {
					t.Errorf("Test %#v failed, test case error: %v", tc.description, err)
				}

				less := func(a, b corev1.Taint) bool { return a.Key < b.Key }
				diffIgnoreOrder := cmp.Diff(expectedNode.Spec.Taints, node.Spec.Taints, cmpopts.SortSlices(less))
				if diffIgnoreOrder != "" {
					t.Errorf("Test %#v failed, node %v should different taints: %v", tc.description, node.Name, diffIgnoreOrder)
				}
			}
		})
	}
}

func getTestFilesLocation(t *testing.T) string {
	wd, err := os.Getwd()
	if err != nil {
		t.Errorf("Test failed, error: %v", err)
	}
	if strings.HasSuffix(wd, pkgDirectory) {
		return testFilesLoc
	}
	return path.Join(pkgDirectory, testFilesLoc)
}

// buildCompactionNode builds a KubeVirt-schedulable node with the given
// allocatable resources, so the compaction tests can control requested-resource
// percentages precisely.
func buildCompactionNode(nodeName string, cpu, memory string, pods int64, taints ...corev1.Taint) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   nodeName,
			Labels: map[string]string{"kubevirt.io/schedulable": "true"},
		},
		Spec: corev1.NodeSpec{
			Taints: taints,
		},
		Status: corev1.NodeStatus{
			Phase: corev1.NodeRunning,
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(memory),
				corev1.ResourcePods:   *resource.NewQuantity(pods, resource.DecimalSI),
			},
		},
	}
	return node
}

// buildCompactionPod builds a running pod assigned to nodeName, requesting the
// given resources, with the given volumes.
func buildCompactionPod(name, nodeName, cpu, memory string, volumes ...corev1.Volume) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{
				{
					Name: "compute",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(cpu),
							corev1.ResourceMemory: resource.MustParse(memory),
						},
					},
				},
			},
			Volumes: volumes,
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	return pod
}

func sealedTaint() corev1.Taint {
	return corev1.Taint{Key: SealedNodeTaintKey, Value: SealedNodeTaintValue, Effect: corev1.TaintEffectNoSchedule}
}

// countSealedNodes returns the names of the nodes carrying the sealed taint.
func countSealedNodes(t *testing.T, ctx context.Context, cl client.Client, names []string) []string {
	t.Helper()
	sealed := []string{}
	for _, name := range names {
		node := &corev1.Node{}
		if err := cl.Get(ctx, client.ObjectKey{Name: name}, node); err != nil {
			t.Fatalf("failed to get node %s: %v", name, err)
		}
		for _, taint := range node.Spec.Taints {
			if taint.Key == SealedNodeTaintKey {
				sealed = append(sealed, name)
				break
			}
		}
	}
	return sealed
}

func TestReconcileCompaction(t *testing.T) {
	ctx := context.Background()

	resourcesSchemeFuncs := []func(*apiruntime.Scheme) error{
		corev1.AddToScheme,
		appsv1.AddToScheme,
		operatorv1.Install,
		deschedulerv1.AddToScheme,
	}

	scheme := runtime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		if err := f(scheme); err != nil {
			t.Fatalf("Failed building scheme for the fake client: %v", err)
		}
	}

	podIndexer := func(rawObj client.Object) []string {
		pod := rawObj.(*corev1.Pod)
		if pod.Spec.NodeName == "" {
			return nil
		}
		return []string{pod.Spec.NodeName}
	}

	emptyDirVolume := corev1.Volume{
		Name:         "scratch",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}
	pvcVolume := corev1.Volume{
		Name: "disk",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "vm-disk"},
		},
	}

	tests := []struct {
		description string
		nodes       []*corev1.Node
		pods        []*corev1.Pod
		testfile    string
		// expectedSealed lists the nodes that must carry the sealed taint
		// after Reconcile. Everything else must not be sealed.
		expectedSealed []string
		// maxSealed is the hard assertion that at most one node is sealed in
		// a single reconcile.
		expectAtMostOneSeal bool
		// mode defaults to Automatic when empty.
		mode deschedulerv1.Mode
	}{
		{
			description: "6 ready nodes below threshold, none sealed: seal exactly the least utilized one",
			nodes: []*corev1.Node{
				buildCompactionNode("node1", "8", "16Gi", 110),
				buildCompactionNode("node2", "8", "16Gi", 110),
				buildCompactionNode("node3", "8", "16Gi", 110),
				buildCompactionNode("node4", "8", "16Gi", 110),
				buildCompactionNode("node5", "8", "16Gi", 110),
				buildCompactionNode("node6", "8", "16Gi", 110),
			},
			pods: []*corev1.Pod{
				// node1 is the emptiest, node6 the busiest. All are below the
				// 20% packing threshold on cpu, memory and pods.
				buildCompactionPod("p1", "node1", "10m", "10Mi", pvcVolume),
				buildCompactionPod("p2", "node2", "200m", "200Mi", pvcVolume),
				buildCompactionPod("p3", "node3", "300m", "300Mi", pvcVolume),
				buildCompactionPod("p4", "node4", "400m", "400Mi", pvcVolume),
				buildCompactionPod("p5", "node5", "500m", "500Mi", pvcVolume),
				buildCompactionPod("p6", "node6", "600m", "600Mi", pvcVolume),
			},
			testfile:            "policyWithKubeVirtCompactAndScale.yaml",
			expectedSealed:      []string{"node1"},
			expectAtMostOneSeal: true,
		},
		{
			description: "2 ready nodes at the floor: no node is sealed",
			nodes: []*corev1.Node{
				buildCompactionNode("node1", "8", "16Gi", 110),
				buildCompactionNode("node2", "8", "16Gi", 110),
			},
			pods: []*corev1.Pod{
				buildCompactionPod("p1", "node1", "10m", "10Mi", pvcVolume),
				buildCompactionPod("p2", "node2", "200m", "200Mi", pvcVolume),
			},
			testfile:            "policyWithKubeVirtCompactAndScale.yaml",
			expectedSealed:      []string{},
			expectAtMostOneSeal: true,
		},
		{
			description: "least utilized candidate has local storage: skip it and seal the next one",
			nodes: []*corev1.Node{
				buildCompactionNode("node1", "8", "16Gi", 110),
				buildCompactionNode("node2", "8", "16Gi", 110),
				buildCompactionNode("node3", "8", "16Gi", 110),
			},
			pods: []*corev1.Pod{
				// node1 is the emptiest but holds a VM with an emptyDir.
				buildCompactionPod("blocker", "node1", "10m", "10Mi", emptyDirVolume),
				buildCompactionPod("p2", "node2", "100m", "100Mi", pvcVolume),
				buildCompactionPod("p3", "node3", "200m", "200Mi", pvcVolume),
			},
			testfile:            "policyWithKubeVirtCompactAndScale.yaml",
			expectedSealed:      []string{"node2"},
			expectAtMostOneSeal: true,
		},
		{
			description: "already sealed node is now above the packing threshold: unseal it",
			nodes: []*corev1.Node{
				buildCompactionNode("node1", "8", "16Gi", 110, sealedTaint()),
				buildCompactionNode("node2", "8", "16Gi", 110),
				buildCompactionNode("node3", "8", "16Gi", 110),
			},
			pods: []*corev1.Pod{
				// node1 is far above the 20% threshold on every resource, so
				// it must not stay sealed.
				buildCompactionPod("hot", "node1", "7", "15Gi", pvcVolume),
				buildCompactionPod("p2", "node2", "10m", "10Mi", pvcVolume),
				buildCompactionPod("p3", "node3", "100m", "100Mi", pvcVolume),
			},
			testfile:            "policyWithKubeVirtCompactAndScale.yaml",
			expectedSealed:      []string{},
			expectAtMostOneSeal: true,
		},
		{
			description: "KubeVirtCompactAndScale absent: no seal is added and leftovers are cleaned up",
			nodes: []*corev1.Node{
				buildCompactionNode("node1", "8", "16Gi", 110),
				buildCompactionNode("node2", "8", "16Gi", 110, sealedTaint()),
				buildCompactionNode("node3", "8", "16Gi", 110),
			},
			pods: []*corev1.Pod{
				buildCompactionPod("p1", "node1", "10m", "10Mi", pvcVolume),
				buildCompactionPod("p2", "node2", "10m", "10Mi", pvcVolume),
				buildCompactionPod("p3", "node3", "10m", "10Mi", pvcVolume),
			},
			testfile:            "policyNoRelieveAndMigrate.yaml",
			expectedSealed:      []string{},
			expectAtMostOneSeal: true,
		},
		{
			description: "Predictive mode is a dry run: no node is sealed",
			nodes: []*corev1.Node{
				buildCompactionNode("node1", "8", "16Gi", 110),
				buildCompactionNode("node2", "8", "16Gi", 110),
				buildCompactionNode("node3", "8", "16Gi", 110),
			},
			pods: []*corev1.Pod{
				buildCompactionPod("p1", "node1", "10m", "10Mi", pvcVolume),
				buildCompactionPod("p2", "node2", "100m", "100Mi", pvcVolume),
				buildCompactionPod("p3", "node3", "200m", "200Mi", pvcVolume),
			},
			testfile:            "policyWithKubeVirtCompactAndScale.yaml",
			expectedSealed:      []string{},
			expectAtMostOneSeal: true,
			mode:                deschedulerv1.Predictive,
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			kubedescheduler := deschedulerv1.KubeDescheduler{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: operatorclient.OperatorNamespace,
					Name:      operatorclient.OperatorConfigName,
				},
				Spec: deschedulerv1.KubeDeschedulerSpec{
					DeschedulingIntervalSeconds: pointer.Int32(30),
					Mode:                        tc.mode,
				},
			}
			if kubedescheduler.Spec.Mode == "" {
				kubedescheduler.Spec.Mode = deschedulerv1.Automatic
			}

			objects := []client.Object{&kubedescheduler}
			nodeNames := []string{}
			for _, node := range tc.nodes {
				objects = append(objects, node)
				nodeNames = append(nodeNames, node.Name)
			}
			for _, pod := range tc.pods {
				objects = append(objects, pod)
			}

			// The spec.nodeName index is mandatory: without it MatchingFields
			// silently returns nothing and the local-storage case would pass
			// for the wrong reason.
			cl := fake.NewClientBuilder().
				WithObjects(objects...).
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, "spec.nodeName", podIndexer).
				Build()

			st := softTainter{
				client:                 cl,
				resyncPeriod:           60 * time.Second,
				policyConfigFile:       path.Join(getTestFilesLocation(t), tc.testfile),
				nodeUtilizationFactory: (&MockNodeUtilization{}).newNodeUtilizationFactory,
			}

			result, err := st.Reconcile(ctx, reconcile.Request{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.RequeueAfter != 30*time.Second {
				t.Errorf("RequeueAfter is not correctly set: %v", result.RequeueAfter)
			}

			sealed := countSealedNodes(t, ctx, cl, nodeNames)

			if tc.expectAtMostOneSeal && len(sealed) > 1 {
				t.Fatalf("more than one node sealed in a single reconcile: %v", sealed)
			}

			got := map[string]bool{}
			for _, name := range sealed {
				got[name] = true
			}
			if len(sealed) != len(tc.expectedSealed) {
				t.Fatalf("expected sealed nodes %v, got %v", tc.expectedSealed, sealed)
			}
			for _, name := range tc.expectedSealed {
				if !got[name] {
					t.Errorf("expected node %q to be sealed, sealed nodes are %v", name, sealed)
				}
			}
		})
	}
}

// TestReconcileCompactionSealsOneNodeAtATime asserts the central safety
// invariant of the compactor: no matter how many nodes are sealable, a single
// reconcile seals exactly one, and successive reconciles converge on the floor.
func TestReconcileCompactionSealsOneNodeAtATime(t *testing.T) {
	ctx := context.Background()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed building scheme for the fake client: %v", err)
	}
	if err := deschedulerv1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed building scheme for the fake client: %v", err)
	}

	podIndexer := func(rawObj client.Object) []string {
		pod := rawObj.(*corev1.Pod)
		if pod.Spec.NodeName == "" {
			return nil
		}
		return []string{pod.Spec.NodeName}
	}

	kubedescheduler := deschedulerv1.KubeDescheduler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: operatorclient.OperatorNamespace,
			Name:      operatorclient.OperatorConfigName,
		},
		Spec: deschedulerv1.KubeDeschedulerSpec{
			DeschedulingIntervalSeconds: pointer.Int32(30),
			Mode:                        deschedulerv1.Automatic,
		},
	}

	pvcVolume := corev1.Volume{
		Name: "disk",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "vm-disk"},
		},
	}

	nodeNames := []string{}
	objects := []client.Object{&kubedescheduler}
	for i := 1; i <= 6; i++ {
		name := fmt.Sprintf("node%d", i)
		nodeNames = append(nodeNames, name)
		objects = append(objects, buildCompactionNode(name, "8", "16Gi", 110))
		// Keep every node well below the 20% packing threshold, with node1 the
		// emptiest so the ordering is deterministic.
		objects = append(objects, buildCompactionPod(fmt.Sprintf("p%d", i), name, fmt.Sprintf("%dm", i*10), fmt.Sprintf("%dMi", i*10), pvcVolume))
	}

	cl := fake.NewClientBuilder().
		WithObjects(objects...).
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, "spec.nodeName", podIndexer).
		Build()

	st := softTainter{
		client:                 cl,
		resyncPeriod:           60 * time.Second,
		policyConfigFile:       path.Join(getTestFilesLocation(t), "policyWithKubeVirtCompactAndScale.yaml"),
		nodeUtilizationFactory: (&MockNodeUtilization{}).newNodeUtilizationFactory,
	}

	// Each pass may add at most one seal.
	for pass := 1; pass <= 4; pass++ {
		before := len(countSealedNodes(t, ctx, cl, nodeNames))
		if _, err := st.Reconcile(ctx, reconcile.Request{}); err != nil {
			t.Fatalf("pass %d: unexpected error: %v", pass, err)
		}
		after := countSealedNodes(t, ctx, cl, nodeNames)
		if len(after)-before > 1 {
			t.Fatalf("pass %d: more than one node sealed: %v -> %v", pass, before, after)
		}
	}

	// 6 nodes, floor of 2: at most 4 can ever be sealed, and the loop must not
	// seal the last two ready nodes.
	sealed := countSealedNodes(t, ctx, cl, nodeNames)
	if len(sealed) == 0 {
		t.Fatalf("no node was ever sealed: the test would not prove anything")
	}
	if len(sealed) > 4 {
		t.Fatalf("sealed more nodes than the floor allows: %v", sealed)
	}
	if len(nodeNames)-len(sealed) < minCompactionReadyNodes {
		t.Fatalf("compaction went below the ready-node floor: %d ready, floor %d", len(nodeNames)-len(sealed), minCompactionReadyNodes)
	}
}

func TestAssessNodeDrainability(t *testing.T) {
	ctx := context.Background()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed building scheme for the fake client: %v", err)
	}

	podIndexer := func(rawObj client.Object) []string {
		pod := rawObj.(*corev1.Pod)
		if pod.Spec.NodeName == "" {
			return nil
		}
		return []string{pod.Spec.NodeName}
	}

	emptyDirVolume := corev1.Volume{
		Name:         "scratch",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}
	hostPathVolume := corev1.Volume{
		Name: "host",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: "/var/lib/foo"},
		},
	}
	pvcVolume := corev1.Volume{
		Name: "disk",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "vm-disk"},
		},
	}

	tests := []struct {
		description        string
		pods               []*corev1.Pod
		expectedMigratable int
		expectedBlocked    int
	}{
		{
			description: "pod with an emptyDir cannot be migrated",
			pods: []*corev1.Pod{
				buildCompactionPod("vm", "node1", "100m", "100Mi", emptyDirVolume),
			},
			expectedMigratable: 0,
			expectedBlocked:    1,
		},
		{
			description: "pod with only a PVC can be live-migrated",
			pods: []*corev1.Pod{
				buildCompactionPod("vm", "node1", "100m", "100Mi", pvcVolume),
			},
			expectedMigratable: 1,
			expectedBlocked:    0,
		},
		{
			description: "pod with a hostPath cannot be migrated",
			pods: []*corev1.Pod{
				buildCompactionPod("vm", "node1", "100m", "100Mi", hostPathVolume),
			},
			expectedMigratable: 0,
			expectedBlocked:    1,
		},
		{
			description: "mixed pods on the same node",
			pods: []*corev1.Pod{
				buildCompactionPod("migratable", "node1", "100m", "100Mi", pvcVolume),
				buildCompactionPod("blocked", "node1", "100m", "100Mi", emptyDirVolume),
				buildCompactionPod("elsewhere", "node2", "100m", "100Mi", emptyDirVolume),
			},
			expectedMigratable: 1,
			expectedBlocked:    1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			objects := []client.Object{buildCompactionNode("node1", "8", "16Gi", 110)}
			for _, pod := range tc.pods {
				objects = append(objects, pod)
			}

			cl := fake.NewClientBuilder().
				WithObjects(objects...).
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, "spec.nodeName", podIndexer).
				Build()

			st := softTainter{client: cl}

			node := &corev1.Node{}
			if err := cl.Get(ctx, client.ObjectKey{Name: "node1"}, node); err != nil {
				t.Fatalf("failed to get node: %v", err)
			}

			drainability, err := st.assessNodeDrainability(ctx, node)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if drainability.migratablePods != tc.expectedMigratable {
				t.Errorf("expected %d migratable pods, got %d", tc.expectedMigratable, drainability.migratablePods)
			}
			if drainability.blockedPods != tc.expectedBlocked {
				t.Errorf("expected %d blocked pods, got %d", tc.expectedBlocked, drainability.blockedPods)
			}

			// Positive assertion that the field index actually works: if it
			// silently returned nothing, every case above would trivially
			// report zero pods.
			podsOnNode, err := st.podsOnNode(ctx, "node1")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expectedOnNode := 0
			for _, pod := range tc.pods {
				if pod.Spec.NodeName == "node1" {
					expectedOnNode++
				}
			}
			if len(podsOnNode) != expectedOnNode {
				t.Fatalf("field index did not observe the pods on node1: got %d, want %d", len(podsOnNode), expectedOnNode)
			}
			if expectedOnNode > 0 && drainability.migratablePods+drainability.blockedPods == 0 {
				t.Fatalf("drainability saw no pods even though node1 has %d", expectedOnNode)
			}
		})
	}
}

func buildTestNodeWithTaints(nodeName string, kubevirtShedulable, appropriatelyUtilized, overUtilized, extra bool) *corev1.Node {
	taints := []corev1.Taint{}
	if appropriatelyUtilized {
		taints = append(taints, corev1.Taint{
			Key:    AppropriatelyUtilizedSoftTaintKey,
			Value:  AppropriatelyUtilizedSoftTaintValue,
			Effect: corev1.TaintEffectPreferNoSchedule,
		})
	}
	if overUtilized {
		taints = append(taints, corev1.Taint{
			Key:    OverUtilizedSoftTaintKey,
			Value:  OverUtilizedSoftTaintValue,
			Effect: corev1.TaintEffectPreferNoSchedule,
		})
	}
	if extra {
		taints = append(taints, corev1.Taint{
			Key:    "extra",
			Value:  "extra",
			Effect: corev1.TaintEffectNoSchedule,
		})
	}
	if len(taints) == 0 {
		taints = nil
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Spec: corev1.NodeSpec{
			Taints: taints,
		},
		Status: corev1.NodeStatus{
			Phase: corev1.NodeRunning,
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if kubevirtShedulable {
		node.SetLabels(map[string]string{"kubevirt.io/schedulable": "true"})
	}
	return node
}

type mockClientWithPatchConflicts struct {
	client.Client
	mu            sync.Mutex
	conflictNodes map[string]bool
	patchAttempts map[string]int
}

func (m *mockClientWithPatchConflicts) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if node, ok := obj.(*corev1.Node); ok {
		m.mu.Lock()
		shouldConflict := m.conflictNodes[node.Name]
		if shouldConflict {
			m.patchAttempts[node.Name]++
		}
		m.mu.Unlock()

		if shouldConflict {
			return apierrors.NewConflict(
				schema.GroupResource{Group: "", Resource: "nodes"},
				node.Name,
				fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"),
			)
		}
	}
	return m.Client.Patch(ctx, obj, patch, opts...)
}

func TestTaintNodesErrorAggregation(t *testing.T) {
	ctx := context.Background()

	resourcesSchemeFuncs := []func(*apiruntime.Scheme) error{
		corev1.AddToScheme,
	}

	scheme := runtime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		err := f(scheme)
		if err != nil {
			t.Fatalf("Failed building scheme for the fake client: %v", err)
		}
	}

	tests := []struct {
		name          string
		lowNodes      []*corev1.Node
		apprNodes     []*corev1.Node
		highNodes     []*corev1.Node
		conflictNodes map[string]bool
		expectError   bool
	}{
		{
			name: "all nodes succeed - no errors",
			lowNodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, true, true, false),
			},
			apprNodes: []*corev1.Node{
				buildTestNodeWithTaints("node2", true, false, false, false),
			},
			highNodes: []*corev1.Node{
				buildTestNodeWithTaints("node3", true, false, false, false),
			},
			conflictNodes: map[string]bool{},
			expectError:   false,
		},
		{
			name: "one node patch conflict - continues processing other nodes",
			lowNodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, true, true, false),
			},
			apprNodes: []*corev1.Node{
				buildTestNodeWithTaints("node2", true, false, false, false),
			},
			highNodes: []*corev1.Node{
				buildTestNodeWithTaints("node3", true, false, false, false),
			},
			conflictNodes: map[string]bool{"node2": true},
			expectError:   true,
		},
		{
			name: "multiple nodes patch conflicts - aggregates all errors",
			lowNodes: []*corev1.Node{
				buildTestNodeWithTaints("node1", true, true, true, false),
				buildTestNodeWithTaints("node2", true, false, true, false),
			},
			apprNodes: []*corev1.Node{
				buildTestNodeWithTaints("node3", true, false, false, false),
			},
			highNodes: []*corev1.Node{
				buildTestNodeWithTaints("node4", true, false, false, false),
				buildTestNodeWithTaints("node5", true, false, false, false),
			},
			conflictNodes: map[string]bool{"node1": true, "node4": true},
			expectError:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objects := []client.Object{}
			for _, node := range tc.lowNodes {
				objects = append(objects, node)
			}
			for _, node := range tc.apprNodes {
				objects = append(objects, node)
			}
			for _, node := range tc.highNodes {
				objects = append(objects, node)
			}

			baseClient := fake.NewClientBuilder().WithObjects(objects...).WithScheme(scheme).Build()
			cl := &mockClientWithPatchConflicts{
				Client:        baseClient,
				conflictNodes: tc.conflictNodes,
				patchAttempts: make(map[string]int),
			}

			st := softTainter{
				client: cl,
			}

			err := st.taintNodes(ctx, tc.lowNodes, tc.apprNodes, tc.highNodes)

			if tc.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			// Verify that taintNodes continued processing despite conflicts
			for nodeName := range tc.conflictNodes {
				if cl.patchAttempts[nodeName] == 0 {
					t.Errorf("Expected patch attempt for conflicting node %s", nodeName)
				}
			}

			// Verify successful nodes were processed
			for _, node := range tc.lowNodes {
				if !tc.conflictNodes[node.Name] {
					n := &corev1.Node{}
					err := baseClient.Get(ctx, client.ObjectKey{Name: node.Name}, n)
					if err != nil {
						t.Errorf("Failed to get node %s: %v", node.Name, err)
					}
				}
			}

			for _, node := range tc.apprNodes {
				if !tc.conflictNodes[node.Name] {
					n := &corev1.Node{}
					err := baseClient.Get(ctx, client.ObjectKey{Name: node.Name}, n)
					if err != nil {
						t.Errorf("Failed to get node %s: %v", node.Name, err)
					}
				}
			}

			for _, node := range tc.highNodes {
				if !tc.conflictNodes[node.Name] {
					n := &corev1.Node{}
					err := baseClient.Get(ctx, client.ObjectKey{Name: node.Name}, n)
					if err != nil {
						t.Errorf("Failed to get node %s: %v", node.Name, err)
					}
				}
			}
		})
	}
}
