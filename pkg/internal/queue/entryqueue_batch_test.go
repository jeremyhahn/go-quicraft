package queue

import "testing"

// TestEntryQueueBatch_HandOffCoversEarlierBatchesAndNeverMovesBackwards
// hands off a later batch first: every item of an earlier batch is then
// taken too, and handing off the earlier batch afterwards does not undo it.
func TestEntryQueueBatch_HandOffCoversEarlierBatchesAndNeverMovesBackwards(t *testing.T) {
	q := NewEntryQueue[int](4, 16)
	var atA, atB uint64
	if !q.AddTracked([]int{1}, &atA) {
		t.Fatal("AddTracked into batch A refused")
	}
	_, idA := q.SwapBatch(nil)
	if idA == 0 || idA != atA {
		t.Fatalf("SwapBatch took batch %d, want %d", idA, atA)
	}
	if !q.AddTracked([]int{2}, &atB) {
		t.Fatal("AddTracked into batch B refused")
	}
	_, idB := q.SwapBatch(nil)
	if idB == 0 || idB != atB || idB <= idA {
		t.Fatalf("SwapBatch took batch %d, want %d after %d", idB, atB, idA)
	}
	if !q.HandOff(idB) {
		t.Fatal("HandOff(idB) refused on an open queue")
	}
	if taken, closed := q.Taken(&atA); !taken || closed {
		t.Fatalf("batch A after HandOff(idB): taken=%v closed=%v, want taken, open", taken, closed)
	}
	if taken, closed := q.Taken(&atB); !taken || closed {
		t.Fatalf("batch B after HandOff(idB): taken=%v closed=%v, want taken, open", taken, closed)
	}
	if !q.HandOff(idA) {
		t.Fatal("HandOff(idA) refused on an open queue")
	}
	if taken, _ := q.Taken(&atB); !taken {
		t.Fatal("HandOff of an earlier batch moved the hand-off mark backwards: batch B no longer taken")
	}
	if taken, _ := q.Taken(&atA); !taken {
		t.Fatal("batch A no longer taken after HandOff(idA)")
	}
}

// TestEntryQueueBatch_RefusedAddTrackedLeavesIndexUntouched fills a queue to
// its limit: a refused AddTracked leaves *at as it was, and a zero *at is
// never reported taken.
func TestEntryQueueBatch_RefusedAddTrackedLeavesIndexUntouched(t *testing.T) {
	q := NewEntryQueue[int](4, 1)
	if !q.Add([]int{0}) {
		t.Fatal("Add up to the limit refused")
	}
	at := uint64(7)
	if q.AddTracked([]int{1, 2}, &at) {
		t.Fatal("AddTracked past the limit accepted")
	}
	if at != 7 {
		t.Fatalf("refused AddTracked wrote %d into at, want it left at 7", at)
	}
	if q.Len() != 1 {
		t.Fatalf("refused AddTracked changed Len to %d, want 1", q.Len())
	}
	zero := uint64(0)
	if taken, closed := q.Taken(&zero); taken || closed {
		t.Fatalf("zero index: taken=%v closed=%v, want neither", taken, closed)
	}
}

// TestEntryQueueBatch_SwapBatchClearsRecycledBuffer recycles a returned
// buffer through SwapBatch: when the queue hands that buffer back, every
// slot past its length is zero, so no stale reference stays reachable.
func TestEntryQueueBatch_SwapBatchClearsRecycledBuffer(t *testing.T) {
	q := NewEntryQueue[*int](4, 16)
	a, b, c, d, e := 1, 2, 3, 4, 5
	if !q.Add([]*int{&a, &b, &c}) {
		t.Fatal("Add refused")
	}
	res, id := q.SwapBatch(nil)
	if len(res) != 3 || id == 0 {
		t.Fatalf("first SwapBatch: len=%d id=%d, want 3 items and a non-zero id", len(res), id)
	}
	if !q.Add([]*int{&d}) {
		t.Fatal("Add refused")
	}
	second, id2 := q.SwapBatch(res[:0])
	if len(second) != 1 || second[0] != &d || id2 <= id {
		t.Fatalf("second SwapBatch: len=%d id=%d", len(second), id2)
	}
	if !q.Add([]*int{&e}) {
		t.Fatal("Add refused")
	}
	got, id3 := q.SwapBatch(second[:0])
	if len(got) != 1 || got[0] != &e || id3 <= id2 {
		t.Fatalf("third SwapBatch: len=%d id=%d", len(got), id3)
	}
	if cap(got) < 3 {
		t.Fatalf("third SwapBatch returned cap %d, want the recycled buffer (cap >= 3)", cap(got))
	}
	full := got[:cap(got)]
	for i := len(got); i < len(full); i++ {
		if full[i] != nil {
			t.Fatalf("recycled buffer slot %d holds a stale reference to %d", i, *full[i])
		}
	}
}

// TestEntryQueueBatch_SwapBatchAfterCloseTakesNothing closes a queue holding
// items: SwapBatch then returns length 0 and id 0.
func TestEntryQueueBatch_SwapBatchAfterCloseTakesNothing(t *testing.T) {
	q := NewEntryQueue[int](4, 16)
	if !q.Add([]int{1, 2, 3}) {
		t.Fatal("Add refused")
	}
	q.Close()
	got, id := q.SwapBatch(make([]int, 0, 4))
	if len(got) != 0 || id != 0 {
		t.Fatalf("SwapBatch after Close: len=%d id=%d, want 0 and 0", len(got), id)
	}
}
