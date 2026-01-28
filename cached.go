package filekv

import (
	"bytes"
	"context"
	"time"
)

// CachedFileKVStore implements the KeyValueStore interface with caching
type CachedFileKVStore struct {
	store KeyValueStore
	cache map[string][]byte
}

func NewCachedFileKVStore(store KeyValueStore) *CachedFileKVStore {
	return &CachedFileKVStore{
		store: store,
		cache: make(map[string][]byte),
	}
}

func (c *CachedFileKVStore) Get(ctx context.Context, key string) ([]byte, error) {
	if val, ok := c.cache[key]; ok {
		return val, nil
	}

	val, err := c.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	// Cache the result
	c.cache[key] = val
	return val, nil
}

func (c *CachedFileKVStore) GetByVersion(ctx context.Context, key string, version string) ([]byte, error) {
	return c.store.GetByVersion(ctx, key, version)
}

func (c *CachedFileKVStore) Set(ctx context.Context, key string, value []byte) (string, error) {
	if val, ok := c.cache[key]; ok {
		if bytes.Equal(val, value) {
			return "", nil
		}
	}

	version, err := c.store.Set(ctx, key, value)
	if err != nil {
		return "", err
	}

	// Update cache if version is not empty (meaning value changed)
	if version != "" {
		c.cache[key] = value
	}

	return version, nil
}

func (c *CachedFileKVStore) SetWithTimestamp(ctx context.Context, key string, value []byte, timestamp time.Time) (string, error) {
	version, err := c.store.SetWithTimestamp(ctx, key, value, timestamp)
	if err != nil {
		return "", err
	}

	// Update cache if version is not empty (meaning value changed)
	if version != "" {
		c.cache[key] = value
	}

	return version, nil
}

func (c *CachedFileKVStore) SetMeta(ctx context.Context, key, version string, meta map[string]string) error {
	return c.store.SetMeta(ctx, key, version, meta)
}

func (c *CachedFileKVStore) UpdateMeta(ctx context.Context, key, version string, meta map[string]string) error {
	return c.store.UpdateMeta(ctx, key, version, meta)
}

func (c *CachedFileKVStore) Delete(ctx context.Context, key string, removeHistories bool) error {
	err := c.store.Delete(ctx, key, removeHistories)
	if err != nil {
		return err
	}

	// Remove from cache
	delete(c.cache, key)
	return nil
}

func (c *CachedFileKVStore) Exists(ctx context.Context, key string) (bool, error) {
	// Check cache first
	if _, ok := c.cache[key]; ok {
		return true, nil
	}

	return c.store.Exists(ctx, key)
}

func (c *CachedFileKVStore) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	return c.store.ListKeys(ctx, prefix)
}

func (c *CachedFileKVStore) GetHistories(ctx context.Context, key string) ([]Version, error) {
	return c.store.GetHistories(ctx, key)
}

func (c *CachedFileKVStore) GetLastVersion(ctx context.Context, key string) (*Version, error) {
	return c.store.GetLastVersion(ctx, key)
}

func (c *CachedFileKVStore) GetPrevVersion(ctx context.Context, key, revision string) (*Version, error) {
	return c.store.GetPrevVersion(ctx, key, revision)
}

func (c *CachedFileKVStore) GetNextVersion(ctx context.Context, key, revision string) (*Version, error) {
	return c.store.GetNextVersion(ctx, key, revision)
}

func (c *CachedFileKVStore) CleanupHistoriesByTime(ctx context.Context, key string, maxAge time.Duration) error {
	return c.store.CleanupHistoriesByTime(ctx, key, maxAge)
}

func (c *CachedFileKVStore) CleanupHistoriesByCount(ctx context.Context, key string, maxCount int) error {
	return c.store.CleanupHistoriesByCount(ctx, key, maxCount)
}

func (c *CachedFileKVStore) Fsck(ctx context.Context) error {
	return c.store.Fsck(ctx)
}
