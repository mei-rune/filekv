package filekv

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cabify/timex/timextest"
)

// 辅助函数：递归获取目录下所有文件路径
func getAllFiles(dir string) ([]string, error) {
	var files []string

	// 遍历目录树
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// 只添加文件，跳过目录
		if !d.IsDir() {
			// 转换为相对路径（相对于dir）
			relPath, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}

			// 统一路径分隔符为 /
			relPath = filepath.ToSlash(relPath)
			files = append(files, relPath)
		}

		return nil
	})

	return files, err
}

// 辅助函数：比对预期文件和实际文件是否完全相等
func checkFiles(t *testing.T, baseDir string, expectedFiles []string) {
	t.Helper()

	// 获取实际文件列表
	actualFiles, err := getAllFiles(baseDir)
	if err != nil {
		t.Fatalf("failed to get files: %v", err)
	}

	// 处理预期文件，统一路径分隔符
	processedExpected := make([]string, len(expectedFiles))
	for i, f := range expectedFiles {
		// 统一路径分隔符为 /
		processedExpected[i] = filepath.ToSlash(f)
	}

	// 创建实际文件的map
	actualMap := make(map[string]bool)
	for _, f := range actualFiles {
		actualMap[f] = true
	}

	// 创建预期文件的map
	expectedMap := make(map[string]bool)
	for _, f := range processedExpected {
		expectedMap[f] = true
	}

	// 检查预期文件是否都存在于实际文件中
	for _, expectedFile := range processedExpected {
		if !actualMap[expectedFile] {
			t.Fatalf("file mismatch: expected %s to exist, but it's missing", filepath.Join(baseDir, expectedFile))
		}
	}

	// 检查实际文件是否都存在于预期文件中
	for _, actualFile := range actualFiles {
		if !expectedMap[actualFile] {
			t.Fatalf("file mismatch: unexpected file %s found", filepath.Join(baseDir, actualFile))
		}
	}

	// 检查文件数量是否一致
	if len(actualFiles) != len(processedExpected) {
		t.Fatalf("file count mismatch: expected %d files, got %d files", len(processedExpected), len(actualFiles))
	}

	t.Logf("Files match exactly: %d files verified", len(processedExpected))
}

func TestFileKVStore_SetFileStructure(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "filekv-set-test")
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
		// 初始化随机数生成器，使用固定种子确保测试可重现
		r := rand.New(rand.NewSource(42))

		// 测试用例：简单键
		key1 := "simple_key"
		value1 := []byte("simple value")
		version1, err := store.Set(ctx, key1, value1)
		if err != nil {
			t.Fatal(err)
		}

		// 随机递增时间（1-10秒）
		randomSeconds := r.Intn(10) + 1
		mockedtimex.SetNow(mockedtimex.Now().Add(time.Duration(randomSeconds) * time.Second))

		// 测试用例：多级键
		key2 := "multi/level/key"
		value2 := []byte("multi level value")
		version2, err := store.Set(ctx, key2, value2)
		if err != nil {
			t.Fatal(err)
		}

		// 随机递增时间（1-10秒）
		randomSeconds = r.Intn(10) + 1
		mockedtimex.SetNow(mockedtimex.Now().Add(time.Duration(randomSeconds) * time.Second))

		// 测试用例：更新同一个键
		value1Updated := []byte("updated simple value")
		version1Updated, err := store.Set(ctx, key1, value1Updated)
		if err != nil {
			t.Fatal(err)
		}

		// 随机递增时间（1-10秒）
		randomSeconds = r.Intn(10) + 1
		mockedtimex.SetNow(mockedtimex.Now().Add(time.Duration(randomSeconds) * time.Second))

		// 定义预期的文件结构
		expectedFiles := []string{
			// 主数据文件
			"simple_key",
			"multi/level/key",
			// 历史文件
			".history/simple_key.h/" + version1,
			".history/simple_key.h/" + version1Updated,
			".history/multi/level/key.h/" + version2,
		}

		// 使用辅助函数验证文件结构
		checkFiles(t, tempDir, expectedFiles)

		// 调用 Fsck 检查和修复数据
		t.Log("Running Fsck...")
		if err := store.Fsck(ctx); err != nil {
			t.Fatalf("Fsck failed: %v", err)
		}
		t.Log("Fsck completed successfully")

		// 再次验证文件结构，确保 Fsck 没有破坏数据
		checkFiles(t, tempDir, expectedFiles)
	})
}

func TestFileKVStore_SetHistoryStructure(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "filekv-set-history-test")
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
		// 初始化随机数生成器，使用固定种子确保测试可重现
		r := rand.New(rand.NewSource(42))

		key := "test/history_structure"

		// 设置多个版本，验证历史记录结构
		var versions []string
		var expectedFiles []string

		// 添加主数据文件
		expectedFiles = append(expectedFiles, key)

		for i := 0; i < 3; i++ {
			value := []byte("version " + string(rune('0'+i)))
			version, err := store.Set(ctx, key, value)
			if err != nil {
				t.Fatal(err)
			}
			versions = append(versions, version)

			// 添加历史文件到预期列表
			expectedFiles = append(expectedFiles, filepath.Join(".history", key+".h", version))

			// 随机递增时间（1-10秒）
			randomSeconds := r.Intn(10) + 1
			mockedtimex.SetNow(mockedtimex.Now().Add(time.Duration(randomSeconds) * time.Second))
		}

		// 使用辅助函数验证文件结构
		checkFiles(t, tempDir, expectedFiles)

		// 调用 Fsck 检查和修复数据
		t.Log("Running Fsck...")
		if err := store.Fsck(ctx); err != nil {
			t.Fatalf("Fsck failed: %v", err)
		}
		t.Log("Fsck completed successfully")

		// 再次验证文件结构，确保 Fsck 没有破坏数据
		checkFiles(t, tempDir, expectedFiles)
	})
}
