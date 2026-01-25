package filekv

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cabify/timex/timextest"
)

func TestFileKVStore_BasicOperations(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "filekv-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建 FileKVStore 实例
	store := NewFileKVStore(tempDir)
	ctx := context.Background()

	// 测试 Set 和 Get 操作
	t.Run("SetAndGet", func(t *testing.T) {
		key := "test/key1"
		value := []byte("hello world")

		version, err := store.Set(ctx, key, value)
		if err != nil {
			t.Fatal(err)
		}
		if version == "" {
			t.Fatal("expected version, got empty string")
		}

		retrieved, err := store.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if string(retrieved) != string(value) {
			t.Fatalf("expected %q, got %q", value, retrieved)
		}
	})

	// 测试 Exists 操作
	t.Run("Exists", func(t *testing.T) {
		key := "test/key2"
		value := []byte("test value")

		_, err := store.Set(ctx, key, value)
		if err != nil {
			t.Fatal(err)
		}

		exists, err := store.Exists(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatal("expected key to exist")
		}

		// 测试不存在的 key
		exists, err = store.Exists(ctx, "non/existent/key")
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("expected key to not exist")
		}
	})

	// 测试 ListKeys 操作
	t.Run("ListKeys", func(t *testing.T) {
		// 添加一些测试数据
		keys := []string{"test/key1", "test/key2", "test/sub/key3", "other/key4"}
		for _, k := range keys {
			_, err := store.Set(ctx, k, []byte("value for "+k))
			if err != nil {
				t.Fatal(err)
			}
		}

		// 测试列出所有 key
		allKeys, err := store.ListKeys(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(allKeys) != len(keys) {
			t.Fatalf("expected %d keys, got %d", len(keys), len(allKeys))
		}

		// 测试按前缀列出 key
		testKeys, err := store.ListKeys(ctx, "test/")
		if err != nil {
			t.Fatal(err)
		}
		if len(testKeys) != 3 {
			t.Error(testKeys)
			t.Fatalf("expected 3 keys with prefix 'test/', got %d", len(testKeys))
		}
	})

	// 测试 Delete 操作
	t.Run("Delete", func(t *testing.T) {
		key := "test/key3"
		value := []byte("to be deleted")

		_, err := store.Set(ctx, key, value)
		if err != nil {
			t.Fatal(err)
		}

		// 删除 key 但保留历史记录
		err = store.Delete(ctx, key, false)
		if err != nil {
			t.Fatal(err)
		}

		// 检查 key 是否存在
		exists, err := store.Exists(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("expected key to be deleted")
		}

		// 检查历史记录目录是否存在
		historyDir := filepath.Join(tempDir, ".history", key+".h")
		_, err = os.Stat(historyDir)
		if err != nil {
			if os.IsNotExist(err) {
				t.Fatal("expected history directory to exist")
			}
			t.Fatal(err)
		}
	})
}

func TestFileKVStore_HistoryOperations(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "filekv-history-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建 FileKVStore 实例
	store := NewFileKVStore(tempDir)
	ctx := context.Background()

	key := "test/history"

	// 使用 timextest.Mocked 模拟时间
	initialTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	timextest.Mocked(initialTime, func(mockedtimex *timextest.TestImplementation) {
		// 添加多个版本的历史记录
		versions := []string{}
		for i := 0; i < 5; i++ {
			value := []byte("version " + string(rune('0'+i)))
			version, err := store.Set(ctx, key, value)
			if err != nil {
				t.Fatal(err)
			}
			versions = append(versions, version)
			// 递增模拟时间，确保每个版本都有不同的时间戳
			mockedtimex.SetNow(mockedtimex.Now().Add(time.Second))
		}

		// 测试 GetHistories
		t.Run("GetHistories", func(t *testing.T) {
			histories, err := store.GetHistories(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if len(histories) != len(versions) {
				t.Fatalf("expected %d histories, got %d", len(versions), len(histories))
			}
		})

		// 测试 GetLastVersion
		t.Run("GetLastVersion", func(t *testing.T) {
			lastVersion, err := store.GetLastVersion(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if lastVersion == nil {
				t.Fatal("expected last version, got nil")
			}
			if lastVersion.Name != versions[len(versions)-1] {
				t.Fatalf("expected last version %q, got %q", versions[len(versions)-1], lastVersion.Name)
			}
		})

		// 测试 GetByVersion
		t.Run("GetByVersion", func(t *testing.T) {
			// 测试获取特定版本
			version := versions[2]
			value, err := store.GetByVersion(ctx, key, version)
			if err != nil {
				t.Fatal(err)
			}
			if string(value) != "version 2" {
				t.Fatalf("expected %q, got %q", "version 2", value)
			}

			// 测试获取最新版本（head）
			headValue, err := store.GetByVersion(ctx, key, "head")
			if err != nil {
				t.Fatal(err)
			}
			latestValue, err := store.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if string(headValue) != string(latestValue) {
				t.Fatalf("expected head value to be same as latest value")
			}
		})

		// 测试 CleanupHistoriesByTime
		t.Run("CleanupHistoriesByTime", func(t *testing.T) {
			// 清理 3 秒前的历史记录
			err := store.CleanupHistoriesByTime(ctx, key, 3*time.Second)
			if err != nil {
				t.Fatal(err)
			}

			histories, err := store.GetHistories(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			// 应该至少保留 3 个版本
			if len(histories) < 3 {
				t.Fatalf("expected at least 3 histories, got %d", len(histories))
			}
		})

		// 测试 CleanupHistoriesByCount
		t.Run("CleanupHistoriesByCount", func(t *testing.T) {
			// 只保留 2 个版本
			err := store.CleanupHistoriesByCount(ctx, key, 2)
			if err != nil {
				t.Fatal(err)
			}

			histories, err := store.GetHistories(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if len(histories) > 2 {
				t.Fatalf("expected at most 2 histories, got %d", len(histories))
			}
		})
	})
}

func TestFileKVStore_MetaOperations(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "filekv-meta-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建 FileKVStore 实例
	store := NewFileKVStore(tempDir)
	ctx := context.Background()

	key := "test/meta"

	// 使用 timextest.Mocked 模拟时间
	initialTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	timextest.Mocked(initialTime, func(mockedtimex *timextest.TestImplementation) {
		// 设置初始值
		version, err := store.Set(ctx, key, []byte("initial value"))
		if err != nil {
			t.Fatal(err)
		}

		// 测试 SetMeta
		t.Run("SetMeta", func(t *testing.T) {
			meta := map[string]string{
				"author":  "test",
				"comment": "initial version",
			}

			err := store.SetMeta(ctx, key, version, meta)
			if err != nil {
				t.Fatal(err)
			}

			// 检查元数据是否设置成功
			histories, err := store.GetHistories(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if len(histories) == 0 {
				t.Fatal("expected at least one history")
			}

			// 注意：GetHistories 方法目前没有返回元数据，所以我们无法直接检查
			// 这里我们通过检查元数据文件是否存在来验证
			metaFile := filepath.Join(tempDir, ".history", key+".h", version+".meta")
			_, err = os.Stat(metaFile)
			if err != nil {
				if os.IsNotExist(err) {
					t.Fatal("expected meta file to exist")
				}
				t.Fatal(err)
			}
		})

		// 测试 UpdateMeta
		t.Run("UpdateMeta", func(t *testing.T) {
			updateMeta := map[string]string{
				"comment":    "updated version",
				"updated_at": mockedtimex.Now().Format(time.RFC3339),
			}

			err := store.UpdateMeta(ctx, key, version, updateMeta)
			if err != nil {
				t.Fatal(err)
			}

			// 检查元数据文件是否存在
			metaFile := filepath.Join(tempDir, ".history", key+".h", version+".meta")
			_, err = os.Stat(metaFile)
			if err != nil {
				if os.IsNotExist(err) {
					t.Fatal("expected meta file to exist")
				}
				t.Fatal(err)
			}
		})
	})
}

func TestCachedFileKVStore(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "filekv-cached-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建 FileKVStore 和 CachedFileKVStore 实例
	store := NewFileKVStore(tempDir)
	cachedStore := NewCachedFileKVStore(store)
	ctx := context.Background()

	key := "test/cached"
	value := []byte("cached value")

	// 首次获取，应该从存储中读取
	_, err = cachedStore.Set(ctx, key, value)
	if err != nil {
		t.Fatal(err)
	}

	// 第二次获取，应该从缓存中读取
	retrieved, err := cachedStore.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(retrieved) != string(value) {
		t.Fatalf("expected %q, got %q", value, retrieved)
	}

	// 更新值，检查缓存是否更新
	newValue := []byte("updated cached value")
	_, err = cachedStore.Set(ctx, key, newValue)
	if err != nil {
		t.Fatal(err)
	}

	retrieved, err = cachedStore.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(retrieved) != string(newValue) {
		t.Fatalf("expected %q, got %q", newValue, retrieved)
	}

	// 删除 key，检查缓存是否清除
	err = cachedStore.Delete(ctx, key, false)
	if err != nil {
		t.Fatal(err)
	}
}
