package softtainter

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AddOrUpdateTaint add a taint with the given effect to the node. If taint was added/updated into node, it'll trigger a
// patch operation to update the node; otherwise, no API calls. Return error if any.
func AddOrUpdateTaint(ctx context.Context, c client.Client, node *v1.Node, taintKey, taintValue string, effect v1.TaintEffect) (bool, error) {
	newNode := &v1.Node{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(node), newNode); err != nil {
		return false, err
	}
	nodeTaints := newNode.Spec.Taints

	var newTaints []v1.Taint
	updated := false
	found := false

	for _, taint := range nodeTaints {
		if taint.Key != taintKey {
			newTaints = append(newTaints, taint)
		} else {
			if taint.Value != taintValue || taint.Effect != effect {
				newTaints = append(newTaints, v1.Taint{Key: taint.Key, Value: taintValue, Effect: effect})
				updated = true
			} else {
				found = true
			}
		}
	}
	if !updated && !found {
		newTaints = append(newTaints, v1.Taint{Key: taintKey, Value: taintValue, Effect: effect})
		updated = true
	}

	if updated {
		newNode.Spec.Taints = newTaints
		err := patchNodeTaints(ctx, c, node.Name, node, newNode)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// RemoveTaint delete a taint from the node. If taint was found on the node, it'll trigger a patch operation
// to update the node; otherwise, no API calls. Return error if any.
func RemoveTaint(ctx context.Context, c client.Client, node *v1.Node, taintKey string) (bool, error) {
	newNode := &v1.Node{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(node), newNode); err != nil {
		return false, err
	}
	nodeTaints := newNode.Spec.Taints
	var newTaints []v1.Taint
	found := false

	for _, taint := range nodeTaints {
		if taint.Key != taintKey {
			newTaints = append(newTaints, taint)
		} else {
			found = true
		}
	}

	if found {
		newNode.Spec.Taints = newTaints
		err := patchNodeTaints(ctx, c, node.Name, node, newNode)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// patchNodeTaints patches node's taints.
func patchNodeTaints(ctx context.Context, c client.Client, nodeName string, oldNode, newNode *v1.Node) error {
	// Strip base diff node from RV to ensure that our Patch request will set RV to check for conflicts over .spec.taints.
	// This is needed because .spec.taints does not specify patchMergeKey and patchStrategy and adding them is no longer an option for compatibility reasons.
	// Using other Patch strategy works for adding new taints, however will not resolve problem with taint removal.
	oldNodeNoRV := oldNode.DeepCopy()
	oldNodeNoRV.ResourceVersion = ""
	oldDataNoRV, err := json.Marshal(oldNodeNoRV)
	if err != nil {
		return fmt.Errorf("failed to marshal old node %#v for node %q: %v", oldNodeNoRV, nodeName, err)
	}

	newTaints := newNode.Spec.Taints
	newNodeClone := oldNode.DeepCopy()
	newNodeClone.Spec.Taints = newTaints
	newData, err := json.Marshal(newNodeClone)
	if err != nil {
		return fmt.Errorf("failed to marshal new node %#v for node %q: %v", newNodeClone, nodeName, err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldDataNoRV, newData, v1.Node{})
	if err != nil {
		return fmt.Errorf("failed to create patch for node %q: %v", nodeName, err)
	}

	err = c.Patch(ctx, newNode, client.RawPatch(types.StrategicMergePatchType, patchBytes), &client.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch node %q: %v", nodeName, err)
	}
	return nil

}
