package filekv

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
	
	"github.com/cabify/timex/timextest"
)

// 辅助函数：将测试数据写入文件系统
func writeTestDataToFS(t *testing.T, tempDir string, data map[string][]byte) {
	t.Helper()
	
	for key, value := range data {
		// 创建主数据文件
		keyPath := filepath.Join(tempDir, key)
		
		// 创建目录结构
		err := os.MkdirAll(filepath.Dir(keyPath), 0755)
		if err != nil {
			t.Fatalf("failed to create directory for key %s: %v", key, err)
		}
		
		// 写入文件
		err = os.WriteFile(keyPath, value, 0644)
		if err != nil {
			t.Fatalf("failed to write file for key %s: %v", key, err)
		}
	}
}

// 辅助函数：检查histories与versions是否匹配
func checkHistories(t *testing.T, histories []Version, versions []string) {
	t.Helper()
	
	if len(histories) != len(versions) {
		t.Fatalf("Expected %d histories, got %d", len(versions), len(histories))
	}
	
	// 将versions转换为map以便快速查找
	versionMap := make(map[string]bool)
	for _, v := range versions {
		versionMap[v] = true
	}
	
	// 检查histories中的每个版本是否都在versions中
	for _, h := range histories {
		if !versionMap[h.Version] {
			t.Fatalf("History version %s not found in generated versions", h.Version)
		}
		// 从map中移除，最后检查是否所有版本都被覆盖
		delete(versionMap, h.Version)
	}
	
	// 检查是否所有版本都被覆盖
	if len(versionMap) > 0 {
		var missingVersions []string
		for v := range versionMap {
			missingVersions = append(missingVersions, v)
		}
		t.Fatalf("Some generated versions not found in histories: %v", missingVersions)
	}
}

// 测试 Fsck 基本功能
func TestFileKVStore_Fsck(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "filekv-fsck-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建 FileKVStore 实例
	store := NewFileKVStore(tempDir)
	ctx := context.Background()

	// 使用 timextest.Mocked 模拟时间
	initialTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	timextest.Mocked(initialTime, func(mockedtimex *timextest.TestImplementation) {
		// 添加一些测试数据
		key := "test/fsck"
		for i := 0; i < 10; i++ {
			_, err := store.Set(ctx, key, []byte("version "+string(rune('0'+i))))
			if err != nil {
				t.Fatal(err)
			}
			// 递增模拟时间，确保每个版本都有不同的时间戳
			mockedtimex.SetNow(mockedtimex.Now().Add(time.Second))
		}

		// 运行 Fsck
		err = store.Fsck(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}

// 测试 Fsck 功能：删除孤立的历史记录
func TestFileKVStore_Fsck_RemoveOrphanedHistories(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "filekv-fsck-orphaned-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 测试数据：只包含key1，不包含key2
	testData := map[string][]byte{
		"key1": []byte("value1"),
		".history/key1.h/1672531203000000000": []byte("abc"),
	}

	orphanedHistoryData := map[string][]byte{
		".history/key2.h/1672531200000000000": []byte("abc"),
		".history/key2.h/1672531201000000000": []byte("abc"),
	}


	// 将测试数据写入文件系统
	writeTestDataToFS(t, tempDir, testData)
	writeTestDataToFS(t, tempDir, orphanedHistoryData)


	expectedFiles := []string {
		"key1",
		".history/key1.h/1672531203000000000",
		".history/key2.h/1672531200000000000",
		".history/key2.h/1672531201000000000",
	}
	checkFiles(t, tempDir, expectedFiles)

	// 创建 FileKVStore 实例并运行 Fsck
	store := NewFileKVStore(tempDir)
	ctx := context.Background()
	err = store.Fsck(ctx)
	if err != nil {
		t.Fatalf("Fsck failed: %v", err)
	}

	expectedFiles = []string {
		"key1",
		".history/key1.h/1672531203000000000",
	}
	checkFiles(t, tempDir, expectedFiles)

	t.Log("Fsck successfully removed orphaned histories")
}

// 测试 Fsck 功能：为没有历史记录的键创建初始历史记录
func TestFileKVStore_Fsck_CreateMissingHistories(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "filekv-fsck-missing-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

		// 使用 timextest.Mocked 模拟时间
	initialTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	timextest.Mocked(initialTime, func(mockedtimex *timextest.TestImplementation) {
	

	// 测试数据：包含多个键，但没有历史记录
	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"multi/level/key": []byte("multi value"),
	}

	// 将测试数据写入文件系统
	writeTestDataToFS(t, tempDir, testData)

	// 定义预期的初始文件结构（只有主数据文件，没有历史记录）
	initialExpected := []string{
		"key1",
		"key2",
		"multi/level/key",
	}

	// 验证初始文件结构
	checkFiles(t, tempDir, initialExpected)

	// 创建 FileKVStore 实例并运行 Fsck
	store := NewFileKVStore(tempDir)
	ctx := context.Background()
	err = store.Fsck(ctx)
	if err != nil {
		t.Fatalf("Fsck failed: %v", err)
	}

	expectedFiles := []string{
		"key1",
		"key2",
		"multi/level/key",

		".history/key1.h/1672531200000000000",
		".history/key2.h/1672531200000000000",
		".history/multi/level/key.h/1672531200000000000",
	}
	checkFiles(t, tempDir, expectedFiles)

	t.Logf("Fsck created history records for all %d keys", len(testData))
	})
}

// 测试 Fsck 功能：历史记录过多，需要组织成子目录
func TestFileKVStore_Fsck_OrganizeHistories(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("d:\\", "filekv-fsck-organize-test")
	if err != nil {
		t.Fatal(err)
	}
	// defer os.RemoveAll(tempDir)

	key := "key1"
	// 测试数据：单个键
	testData := map[string][]byte{
		key: []byte("value1"),
	}

	
	now := time.Now()
	count := 450

	versions := make([]string, 0, count);
	for i := 0; i < count; i++ {
		// 使用递增的时间戳作为版本号
		timestamp := now.Add(time.Duration(i+1) * time.Second).UnixNano()
		version := strconv.FormatInt(timestamp, 10)
		testData[".history/"+key+".h/"+version] = []byte(version)
		versions = append(versions, version)
	}

	// 将测试数据写入文件系统（只写主数据文件）
	writeTestDataToFS(t, tempDir, testData)

	var expectedFiles []string
	expectedFiles = append(expectedFiles, key)
	for _, version := range versions {
		expectedFiles = append(expectedFiles, filepath.Join(".history", key+".h", version))
	}

	// 验证初始文件结构
	checkFiles(t, tempDir, expectedFiles)

	// 创建 FileKVStore 实例
	store := NewFileKVStore(tempDir)
	ctx := context.Background()

	historiesBefore, err := store.GetHistories(ctx, key)
	if err != nil {
		t.Fatalf("GetHistories before Fsck failed: %v", err)
	}
	t.Logf("Before Fsck: %d histories returned by GetHistories", len(historiesBefore))

	// check historiesBefore with versions
	checkHistories(t, historiesBefore, versions)

	// 运行 Fsck
	err = store.Fsck(ctx)
	if err != nil {
		t.Fatalf("Fsck failed: %v", err)
	}

	// 在 Fsck 之后调用 GetHistories，验证组织后的历史记录仍可访问
	historiesAfter, err := store.GetHistories(ctx, key)
	if err != nil {
		t.Fatalf("GetHistories after Fsck failed: %v", err)
	}
	t.Logf("After Fsck: %d histories returned by GetHistories", len(historiesAfter))
	
	// check historiesAfter with versions
	checkHistories(t, historiesAfter, versions)

	
	expectedFiles = make([]string, 0, count)
	expectedFiles = append(expectedFiles, key)

	currentHistories := versions
	for len(currentHistories) >= maxHistoryCount  {
		pageHistories := currentHistories[:maxHistoryCount]
		for _, version := range pageHistories {
			expectedFiles = append(expectedFiles, filepath.Join(".history", key+".h", pagePrefix + pageHistories[0], version))
		}
		currentHistories = currentHistories[maxHistoryCount:]
	}
	for _, version := range currentHistories {
		expectedFiles = append(expectedFiles, filepath.Join(".history", key+".h", version))
	}

	// 验证初始文件结构
	checkFiles(t, tempDir, expectedFiles)

	t.Log("Fsck successfully organized histories into subdirectories")
}