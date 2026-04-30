package queue

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// ShardRegistry is the read-only surface for components that need to look up
// shards, fan out across the active set, or resolve a shard for a given
// account/queue. It replaces the trio of (queueShardClients map, ShardSelector,
// primaryQueueShard) that used to be passed independently into queue.New,
// NewProducer, the executor, the singleton store, and various API surfaces.
type ShardRegistry interface {
	// Primary returns the shard this executor is currently leased against,
	// or nil when the registry is in shard-group mode and has not yet
	// claimed a lease.
	Primary() QueueShard

	// ByName returns the shard registered under name, or
	// ErrQueueShardNotFound if absent.
	ByName(name string) (QueueShard, error)

	// ByGroup returns all shards whose ShardAssignmentConfig.ShardGroup
	// matches groupName.
	ByGroup(groupName string) []QueueShard

	// Resolve picks a shard for a given enqueue, applying the registry's
	// ShardSelector. If no selector is configured but a primary shard is
	// set, Resolve returns the primary as a fallback.
	Resolve(ctx context.Context, accountID uuid.UUID, queueName *string) (QueueShard, error)

	// ForEach runs fn against every active shard concurrently, returning
	// the first error encountered. The shard set is snapshotted at call
	// time; mutations during iteration are not observed.
	ForEach(ctx context.Context, fn func(context.Context, QueueShard) error) error
}

// QueueShardRegistry is the surface the queue processor itself depends on:
// the read-only ShardRegistry plus SetPrimary, which the shard-lease loop
// calls when it claims a lease. Components that don't run the lease loop
// should depend on ShardRegistry instead.
type QueueShardRegistry interface {
	ShardRegistry

	// SetPrimary updates the leased primary shard. Pass nil to clear the
	// primary. A non-nil shard is also added to the active set if not
	// already present.
	SetPrimary(ctx context.Context, shard QueueShard)
}

// ShardRegistryController is the full mutate surface, held by bootstrap
// wiring that owns topology changes. Components outside the queue control
// plane should depend on ShardRegistry (or QueueShardRegistry) instead.
type ShardRegistryController interface {
	QueueShardRegistry

	// Replace atomically swaps the shard set and selector. The current
	// primary is preserved across replacement (re-inserted into the new
	// set if its name is missing); callers wanting to drop the primary
	// should call SetPrimary(ctx, nil) afterwards.
	Replace(shards map[string]QueueShard, selector ShardSelector)

	// Add registers a shard. If a shard with the same name already
	// exists, it is overwritten.
	Add(shard QueueShard)

	// Remove unregisters the shard with the given name. No-op if absent.
	// If the removed shard was the primary, the primary is cleared.
	Remove(name string)
}

// ShardRegistryOpt configures a registry at construction time.
type ShardRegistryOpt func(*shardRegistry)

// WithPrimary sets the initial primary shard at construction. In
// shard-group mode where the primary is claimed at runtime by the lease
// loop, omit this option and let the lease loop call SetPrimary later.
// A non-nil shard is also added to the active set if not present.
func WithPrimary(shard QueueShard) ShardRegistryOpt {
	return func(r *shardRegistry) {
		r.primary = shard
		if shard != nil {
			r.shards[shard.Name()] = shard
		}
	}
}

// NewSingleShardRegistry is a convenience constructor for the common
// single-shard case (devserver, tests). It seeds the topology with the
// shard, configures a selector that always returns it, and sets it as
// the primary.
func NewSingleShardRegistry(shard QueueShard) *shardRegistry {
	return NewShardRegistry(
		map[string]QueueShard{shard.Name(): shard},
		func(ctx context.Context, _ uuid.UUID, _ *string) (QueueShard, error) {
			return shard, nil
		},
		WithPrimary(shard),
	)
}

// NewShardRegistry constructs a registry with the given topology and
// selector. shards may be nil (an empty topology is allowed); selector
// may also be nil, in which case Resolve falls back to the primary
// shard (and errors if no primary is set).
func NewShardRegistry(shards map[string]QueueShard, selector ShardSelector, opts ...ShardRegistryOpt) *shardRegistry {
	r := &shardRegistry{
		shards:   maps.Clone(shards),
		selector: selector,
	}
	if r.shards == nil {
		r.shards = map[string]QueueShard{}
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

type shardRegistry struct {
	mu       sync.RWMutex
	shards   map[string]QueueShard
	selector ShardSelector
	primary  QueueShard
}

func (r *shardRegistry) Primary() QueueShard {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.primary
}

func (r *shardRegistry) ByName(name string) (QueueShard, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.shards[name]
	if !ok {
		return nil, ErrQueueShardNotFound
	}
	return s, nil
}

func (r *shardRegistry) ByGroup(groupName string) []QueueShard {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []QueueShard
	for _, s := range r.shards {
		if s.ShardAssignmentConfig().ShardGroup == groupName {
			out = append(out, s)
		}
	}
	return out
}

func (r *shardRegistry) Resolve(ctx context.Context, accountID uuid.UUID, queueName *string) (QueueShard, error) {
	r.mu.RLock()
	sel := r.selector
	primary := r.primary
	r.mu.RUnlock()

	if sel == nil {
		if primary != nil {
			return primary, nil
		}
		return nil, fmt.Errorf("shard registry has no selector and no primary shard")
	}
	return sel(ctx, accountID, queueName)
}

func (r *shardRegistry) ForEach(ctx context.Context, fn func(context.Context, QueueShard) error) error {
	snapshot := r.snapshot()
	eg, ctx := errgroup.WithContext(ctx)
	for name, s := range snapshot {
		eg.Go(func() error {
			if err := fn(ctx, s); err != nil {
				return fmt.Errorf("shard %q: %w", name, err)
			}
			return nil
		})
	}
	return eg.Wait()
}

func (r *shardRegistry) snapshot() map[string]QueueShard {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return maps.Clone(r.shards)
}

func (r *shardRegistry) SetPrimary(ctx context.Context, shard QueueShard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.primary = shard
	if shard != nil {
		r.shards[shard.Name()] = shard
	}
}

func (r *shardRegistry) Replace(shards map[string]QueueShard, selector ShardSelector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shards = maps.Clone(shards)
	if r.shards == nil {
		r.shards = map[string]QueueShard{}
	}
	if r.primary != nil {
		if _, ok := r.shards[r.primary.Name()]; !ok {
			r.shards[r.primary.Name()] = r.primary
		}
	}
	r.selector = selector
}

func (r *shardRegistry) Add(shard QueueShard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shards[shard.Name()] = shard
}

func (r *shardRegistry) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.shards, name)
	if r.primary != nil && r.primary.Name() == name {
		r.primary = nil
	}
}
