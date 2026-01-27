package e2e

import "testing"

func TestComputeKeySlot_UsesHashTag(t *testing.T) {
	slot1, err := computeKeySlot("{scaling}test-key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	slot2, err := computeKeySlot("{scaling}test-key-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slot1 != slot2 {
		t.Fatalf("expected same slot for same hash tag, got %d and %d", slot1, slot2)
	}
}
