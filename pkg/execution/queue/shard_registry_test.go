package queue

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// registryTestShard is a QueueShard with a configurable group, for ByGroup
// tests. It reuses mockShardForIterator (defined in processor_iterator_test.go)
// to satisfy the rest of the QueueShard surface.
type registryTestShard struct {
	mockShardForIterator
	group string
}

func (r *registryTestShard) ShardAssignmentConfig() ShardAssignmentConfig {
	return ShardAssignmentConfig{ShardGroup: r.group}
}

func newTestShard(name, group string) *registryTestShard {
	return &registryTestShard{
		mockShardForIterator: mockShardForIterator{name: name},
		group:                group,
	}
}

func TestShardRegistry_NewAndRead(t *testing.T) {
	t.Run("zero values", func(t *testing.T) {
		r := NewShardRegistry(nil, nil)
		require.Nil(t, r.Primary())

		_, err := r.ByName("anything")
		require.ErrorIs(t, err, ErrQueueShardNotFound)

		require.Empty(t, r.ByGroup("g"))

		_, err = r.Resolve(context.Background(), uuid.New(), nil)
		require.Error(t, err)
	})

	t.Run("WithPrimary adds primary to set", func(t *testing.T) {
		p := newTestShard("primary", "")
		r := NewShardRegistry(nil, nil, WithPrimary(p))

		require.Equal(t, p, r.Primary())

		got, err := r.ByName("primary")
		require.NoError(t, err)
		require.Equal(t, p, got)
	})

	t.Run("explicit shards plus primary", func(t *testing.T) {
		a := newTestShard("a", "g1")
		b := newTestShard("b", "g2")
		p := newTestShard("primary", "g1")

		r := NewShardRegistry(map[string]QueueShard{
			"a": a,
			"b": b,
		}, nil, WithPrimary(p))

		// All three present
		got, err := r.ByName("a")
		require.NoError(t, err)
		require.Equal(t, a, got)
		got, err = r.ByName("primary")
		require.NoError(t, err)
		require.Equal(t, p, got)

		// ByGroup filters
		g1 := r.ByGroup("g1")
		require.Len(t, g1, 2)
		// no order guarantee — collect names
		names := []string{g1[0].Name(), g1[1].Name()}
		slices.Sort(names)
		require.Equal(t, []string{"a", "primary"}, names)

		require.Len(t, r.ByGroup("g2"), 1)
		require.Empty(t, r.ByGroup("missing"))
	})

	t.Run("input maps are cloned", func(t *testing.T) {
		a := newTestShard("a", "")
		input := map[string]QueueShard{"a": a}
		r := NewShardRegistry(input, nil)

		// Mutating the caller's map must not affect the registry.
		delete(input, "a")
		got, err := r.ByName("a")
		require.NoError(t, err)
		require.Equal(t, a, got)
	})
}

func TestShardRegistry_Resolve(t *testing.T) {
	t.Run("uses selector when configured", func(t *testing.T) {
		picked := newTestShard("picked", "")
		var sawAccount uuid.UUID
		sel := func(ctx context.Context, acct uuid.UUID, qn *string) (QueueShard, error) {
			sawAccount = acct
			return picked, nil
		}
		r := NewShardRegistry(map[string]QueueShard{"picked": picked}, sel)

		acct := uuid.New()
		got, err := r.Resolve(context.Background(), acct, nil)
		require.NoError(t, err)
		require.Equal(t, picked, got)
		require.Equal(t, acct, sawAccount)
	})

	t.Run("falls back to primary when selector nil", func(t *testing.T) {
		p := newTestShard("primary", "")
		r := NewShardRegistry(nil, nil, WithPrimary(p))

		got, err := r.Resolve(context.Background(), uuid.New(), nil)
		require.NoError(t, err)
		require.Equal(t, p, got)
	})

	t.Run("errors when neither selector nor primary set", func(t *testing.T) {
		r := NewShardRegistry(nil, nil)
		_, err := r.Resolve(context.Background(), uuid.New(), nil)
		require.Error(t, err)
	})

	t.Run("propagates selector errors", func(t *testing.T) {
		want := errors.New("boom")
		sel := func(ctx context.Context, _ uuid.UUID, _ *string) (QueueShard, error) {
			return nil, want
		}
		r := NewShardRegistry(nil, sel)
		_, err := r.Resolve(context.Background(), uuid.New(), nil)
		require.ErrorIs(t, err, want)
	})
}

func TestShardRegistry_ForEach(t *testing.T) {
	a := newTestShard("a", "")
	b := newTestShard("b", "")
	c := newTestShard("c", "")
	r := NewShardRegistry(map[string]QueueShard{
		"a": a, "b": b, "c": c,
	}, nil)

	t.Run("visits every shard", func(t *testing.T) {
		var visited sync.Map
		err := r.ForEach(context.Background(), func(ctx context.Context, s QueueShard) error {
			visited.Store(s.Name(), struct{}{})
			return nil
		})
		require.NoError(t, err)
		for _, n := range []string{"a", "b", "c"} {
			_, ok := visited.Load(n)
			require.True(t, ok, "expected %q visited", n)
		}
	})

	t.Run("surfaces first error", func(t *testing.T) {
		want := errors.New("nope")
		err := r.ForEach(context.Background(), func(ctx context.Context, s QueueShard) error {
			if s.Name() == "b" {
				return want
			}
			return nil
		})
		require.Error(t, err)
		require.ErrorIs(t, err, want)
	})
}

func TestShardRegistry_Mutation(t *testing.T) {
	t.Run("SetPrimary updates and inserts into set", func(t *testing.T) {
		r := NewShardRegistry(nil, nil)
		p := newTestShard("primary", "")
		r.SetPrimary(context.Background(), p)

		require.Equal(t, p, r.Primary())
		got, err := r.ByName("primary")
		require.NoError(t, err)
		require.Equal(t, p, got)
	})

	t.Run("SetPrimary(nil) clears", func(t *testing.T) {
		p := newTestShard("primary", "")
		r := NewShardRegistry(nil, nil, WithPrimary(p))
		r.SetPrimary(context.Background(), nil)
		require.Nil(t, r.Primary())
		// Shard remains in the set even after primary is cleared.
		_, err := r.ByName("primary")
		require.NoError(t, err)
	})

	t.Run("Add inserts and overwrites", func(t *testing.T) {
		r := NewShardRegistry(nil, nil)
		v1 := newTestShard("a", "g1")
		r.Add(v1)
		got, _ := r.ByName("a")
		require.Equal(t, v1, got)

		v2 := newTestShard("a", "g2")
		r.Add(v2)
		got, _ = r.ByName("a")
		require.Equal(t, v2, got)
	})

	t.Run("Remove deletes and clears primary if it was removed", func(t *testing.T) {
		p := newTestShard("primary", "")
		other := newTestShard("other", "")
		r := NewShardRegistry(map[string]QueueShard{"other": other}, nil, WithPrimary(p))

		r.Remove("other")
		_, err := r.ByName("other")
		require.ErrorIs(t, err, ErrQueueShardNotFound)
		require.Equal(t, p, r.Primary())

		r.Remove("primary")
		require.Nil(t, r.Primary())
		_, err = r.ByName("primary")
		require.ErrorIs(t, err, ErrQueueShardNotFound)
	})

	t.Run("Remove of missing name is a no-op", func(t *testing.T) {
		r := NewShardRegistry(nil, nil)
		require.NotPanics(t, func() { r.Remove("missing") })
	})

	t.Run("Replace swaps shards and selector atomically", func(t *testing.T) {
		oldShard := newTestShard("old", "")
		r := NewShardRegistry(map[string]QueueShard{"old": oldShard}, func(ctx context.Context, _ uuid.UUID, _ *string) (QueueShard, error) {
			return oldShard, nil
		})

		newShard := newTestShard("new", "")
		newSel := func(ctx context.Context, _ uuid.UUID, _ *string) (QueueShard, error) {
			return newShard, nil
		}
		r.Replace(map[string]QueueShard{"new": newShard}, newSel)

		_, err := r.ByName("old")
		require.ErrorIs(t, err, ErrQueueShardNotFound)
		got, err := r.ByName("new")
		require.NoError(t, err)
		require.Equal(t, newShard, got)

		resolved, err := r.Resolve(context.Background(), uuid.New(), nil)
		require.NoError(t, err)
		require.Equal(t, newShard, resolved)
	})

	t.Run("Replace preserves primary when its name is missing from new set", func(t *testing.T) {
		p := newTestShard("primary", "")
		r := NewShardRegistry(nil, nil, WithPrimary(p))

		other := newTestShard("other", "")
		r.Replace(map[string]QueueShard{"other": other}, nil)

		require.Equal(t, p, r.Primary())
		got, err := r.ByName("primary")
		require.NoError(t, err)
		require.Equal(t, p, got)
		got, err = r.ByName("other")
		require.NoError(t, err)
		require.Equal(t, other, got)
	})

	t.Run("Replace lets new set override primary's name", func(t *testing.T) {
		oldPrimary := newTestShard("p", "g-old")
		r := NewShardRegistry(nil, nil, WithPrimary(oldPrimary))

		newPrimary := newTestShard("p", "g-new")
		r.Replace(map[string]QueueShard{"p": newPrimary}, nil)

		// Primary label still points at the old object — Replace doesn't
		// change which shard is leased. ByName, however, returns the new
		// one (matching the topology).
		require.Equal(t, oldPrimary, r.Primary())
		got, err := r.ByName("p")
		require.NoError(t, err)
		require.Equal(t, newPrimary, got)
	})
}

func TestShardRegistry_ConcurrentReadsDuringMutation(t *testing.T) {
	r := NewShardRegistry(nil, nil)
	for i := 0; i < 8; i++ {
		r.Add(newTestShard(string(rune('a'+i)), "g"))
	}

	const goroutines = 16
	const iters = 1000

	var done atomic.Bool
	var wg sync.WaitGroup

	// Readers
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !done.Load() {
				_ = r.ByGroup("g")
				_, _ = r.Resolve(context.Background(), uuid.New(), nil)
				_ = r.ForEach(context.Background(), func(context.Context, QueueShard) error { return nil })
			}
		}()
	}

	// Mutators
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			r.Add(newTestShard("x", "g"))
			r.Remove("x")
			r.SetPrimary(context.Background(), newTestShard("p", "g"))
			r.SetPrimary(context.Background(), nil)
		}
		done.Store(true)
	}()

	wg.Wait()
}
