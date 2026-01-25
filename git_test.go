package filekv

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// TestImportGitRepo 测试基本的 git 仓库导入功能
func TestImportGitRepo(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := ioutil.TempDir("", "git-import-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建测试用的 git 仓库
	repoDir := filepath.Join(tempDir, "test-repo")
	err = os.MkdirAll(repoDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create repo dir: %v", err)
	}

	// 初始化 git 仓库
	r, err := git.PlainInit(repoDir, false)
	if err != nil {
		t.Fatalf("Failed to init git repo: %v", err)
	}

	// 创建测试文件
	testFiles := map[string]string{
		"file1.txt":           "content1",
		"dir1/file2.txt":      "content2",
		"dir1/dir2/file3.txt": "content3",
	}

	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("Failed to get worktree: %v", err)
	}

	// 创建文件并添加到 git
	for path, content := range testFiles {
		fullPath := filepath.Join(repoDir, path)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		if err != nil {
			t.Fatalf("Failed to create file dir: %v", err)
		}
		err = ioutil.WriteFile(fullPath, []byte(content), 0644)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}
		_, err = wt.Add(path)
		if err != nil {
			t.Fatalf("Failed to add file to git: %v", err)
		}
	}

	// 提交更改
	_, err = wt.Commit("Initial commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test Author",
			Email: "test@example.com",
		},
	})
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// 创建 KV 存储
	kvDir := filepath.Join(tempDir, "kv-store")
	store := NewFileKVStore(kvDir)
	ctx := context.Background()

	// 导入 git 仓库
	result, err := ImportGitRepo(ctx, store, repoDir, nil)
	if err != nil {
		t.Fatalf("Failed to import git repo: %v", err)
	}

	// 验证导入结果
	if len(result.ImportedFiles) != len(testFiles) {
		t.Fatalf("Expected %d imported files, got %d", len(testFiles), len(result.ImportedFiles))
	}
	for filePath := range testFiles {
		if len(result.ImportedFiles[filePath]) == 0 {
			t.Fatalf("Expected at least one version for file %s", filePath)
		}
	}
	if len(result.Errors) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(result.Errors), result.Errors)
	}

	// 验证文件内容是否正确导入
	for path, expectedContent := range testFiles {
		content, err := store.Get(ctx, path)
		if err != nil {
			t.Fatalf("Failed to get file %s: %v", path, err)
		}
		if string(content) != expectedContent {
			t.Fatalf("Expected content '%s' for file %s, got '%s'", expectedContent, path, string(content))
		}
	}
}

// TestImportGitRepoWithFilter 测试带过滤器的 git 仓库导入功能
func TestImportGitRepoWithFilter(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := ioutil.TempDir("", "git-import-test-filter")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建测试用的 git 仓库
	repoDir := filepath.Join(tempDir, "test-repo")
	err = os.MkdirAll(repoDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create repo dir: %v", err)
	}

	// 初始化 git 仓库
	r, err := git.PlainInit(repoDir, false)
	if err != nil {
		t.Fatalf("Failed to init git repo: %v", err)
	}

	// 创建测试文件
	testFiles := map[string]string{
		"file1.txt": "content1",
		"file2.md":  "content2",
		"file3.txt": "content3",
	}

	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("Failed to get worktree: %v", err)
	}

	// 创建文件并添加到 git
	for path, content := range testFiles {
		fullPath := filepath.Join(repoDir, path)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		if err != nil {
			t.Fatalf("Failed to create file dir: %v", err)
		}
		err = ioutil.WriteFile(fullPath, []byte(content), 0644)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}
		_, err = wt.Add(path)
		if err != nil {
			t.Fatalf("Failed to add file to git: %v", err)
		}
	}

	// 提交更改
	_, err = wt.Commit("Initial commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test Author",
			Email: "test@example.com",
		},
	})
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// 创建 KV 存储
	kvDir := filepath.Join(tempDir, "kv-store")
	store := NewFileKVStore(kvDir)
	ctx := context.Background()

	// 定义过滤器：只导入 .txt 文件
	filter := func(ctx context.Context, file string) bool {
		return filepath.Ext(file) == ".txt"
	}

	// 导入 git 仓库
	result, err := ImportGitRepo(ctx, store, repoDir, filter)
	if err != nil {
		t.Fatalf("Failed to import git repo: %v", err)
	}

	// 验证导入结果：应该只导入 2 个 .txt 文件
	if len(result.ImportedFiles) != 2 {
		t.Fatalf("Expected 2 imported files, got %d", len(result.ImportedFiles))
	}
	// 验证只有 .txt 文件被导入
	for filePath := range result.ImportedFiles {
		if filepath.Ext(filePath) != ".txt" {
			t.Fatalf("Expected only .txt files, got %s", filePath)
		}
		if len(result.ImportedFiles[filePath]) == 0 {
			t.Fatalf("Expected at least one version for file %s", filePath)
		}
	}
	if len(result.Errors) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(result.Errors), result.Errors)
	}

	// 验证文件内容是否正确导入
	assertFileExistsWithContent(t, ctx, store, "file1.txt", "content1")
	assertFileExistsWithContent(t, ctx, store, "file3.txt", "content3")

	// 验证 .md 文件没有被导入
	_, err = store.Get(ctx, "file2.md")
	if err == nil {
		t.Fatalf("Expected file2.md not to be imported")
	}
}

// TestImportGitRepoEmptyRepo 测试导入空仓库
func TestImportGitRepoEmptyRepo(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := ioutil.TempDir("", "git-import-test-empty")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建测试用的空 git 仓库
	repoDir := filepath.Join(tempDir, "empty-repo")
	err = os.MkdirAll(repoDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create repo dir: %v", err)
	}

	// 初始化 git 仓库
	_, err = git.PlainInit(repoDir, false)
	if err != nil {
		t.Fatalf("Failed to init git repo: %v", err)
	}

	// 创建 KV 存储
	kvDir := filepath.Join(tempDir, "kv-store")
	store := NewFileKVStore(kvDir)
	ctx := context.Background()

	// 导入空 git 仓库
	result, err := ImportGitRepo(ctx, store, repoDir, nil)
	if err != nil {
		t.Fatalf("Failed to import git repo: %v", err)
	}

	// 验证导入结果：应该导入 0 个文件
	if len(result.ImportedFiles) != 0 {
		t.Fatalf("Expected empty imported files map, got %d entries", len(result.ImportedFiles))
	}
	if len(result.Errors) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(result.Errors), result.Errors)
	}
}

// 辅助函数：验证文件存在且内容正确
func assertFileExistsWithContent(t *testing.T, ctx context.Context, store KeyValueStore, path, expectedContent string) {
	content, err := store.Get(ctx, path)
	if err != nil {
		t.Fatalf("Failed to get file %s: %v", path, err)
	}
	if string(content) != expectedContent {
		t.Fatalf("Expected content '%s' for file %s, got '%s'", expectedContent, path, string(content))
	}
}
