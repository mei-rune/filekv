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

	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("Failed to get worktree: %v", err)
	}

	// 第一次提交：添加初始文件
	testFiles := map[string]string{
		"file1.txt":           "content1",
		"dir1/file2.txt":      "content2",
		"dir1/dir2/file3.txt": "content3",
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

	// 第一次提交
	_, err = wt.Commit("Initial commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test Author",
			Email: "test@example.com",
		},
	})
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// 第二次提交：修改文件
	err = ioutil.WriteFile(filepath.Join(repoDir, "file1.txt"), []byte("content1-updated"), 0644)
	if err != nil {
		t.Fatalf("Failed to update file: %v", err)
	}
	_, err = wt.Add("file1.txt")
	if err != nil {
		t.Fatalf("Failed to add file to git: %v", err)
	}

	// 第二次提交
	_, err = wt.Commit("Update file1.txt", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test Author",
			Email: "test@example.com",
		},
	})
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// 第三次提交：添加新文件
	err = ioutil.WriteFile(filepath.Join(repoDir, "file4.txt"), []byte("content4"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	_, err = wt.Add("file4.txt")
	if err != nil {
		t.Fatalf("Failed to add file to git: %v", err)
	}

	// 第三次提交
	_, err = wt.Commit("Add file4.txt", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test Author",
			Email: "test@example.com",
		},
	})
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// 第四次提交：修改多个文件
	err = ioutil.WriteFile(filepath.Join(repoDir, "dir1/file2.txt"), []byte("content2-updated"), 0644)
	if err != nil {
		t.Fatalf("Failed to update file: %v", err)
	}
	err = ioutil.WriteFile(filepath.Join(repoDir, "file4.txt"), []byte("content4-updated"), 0644)
	if err != nil {
		t.Fatalf("Failed to update file: %v", err)
	}
	_, err = wt.Add("dir1/file2.txt")
	if err != nil {
		t.Fatalf("Failed to add file to git: %v", err)
	}
	_, err = wt.Add("file4.txt")
	if err != nil {
		t.Fatalf("Failed to add file to git: %v", err)
	}

	// 第四次提交
	_, err = wt.Commit("Update multiple files", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test Author",
			Email: "test@example.com",
		},
	})
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// 更新测试文件映射，包含所有文件
	testFiles = map[string]string{
		"file1.txt":           "content1-updated",
		"dir1/file2.txt":      "content2-updated",
		"dir1/dir2/file3.txt": "content3",
		"file4.txt":           "content4-updated",
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

	// 验证每个文件的导入版本数量（每个文件在每次提交中都会被导入，所以数量等于包含该文件的提交数量）
	expectedImportedVersions := map[string]int{
		"file1.txt":           4, // 4次提交都包含file1.txt
		"dir1/file2.txt":      4, // 4次提交都包含dir1/file2.txt
		"dir1/dir2/file3.txt": 4, // 4次提交都包含dir1/dir2/file3.txt
		"file4.txt":           2, // 2次提交包含file4.txt (Add file4.txt + Update multiple files)
	}

	// 验证每个文件在KV存储中的历史记录数量（只有当文件内容变化时才会创建新的历史记录）
	expectedKVHistories := map[string]int{
		"file1.txt":           2, // 被修改2次（初始 + 第2次提交修改）
		"dir1/file2.txt":      2, // 被修改2次（初始 + 第4次提交修改）
		"dir1/dir2/file3.txt": 1, // 未被修改（只有初始版本）
		"file4.txt":           2, // 被修改2次（初始添加 + 第4次提交修改）
	}

	for filePath, expectedCount := range expectedImportedVersions {
		if len(result.ImportedFiles[filePath]) != expectedCount {
			t.Fatalf("Expected %d imported versions for file %s, got %d", expectedCount, filePath, len(result.ImportedFiles[filePath]))
		}
	}

	for filePath, expectedCount := range expectedKVHistories {
		// 验证文件在KV存储中的历史记录数量
		kvHistories, err := store.GetHistories(ctx, filePath)
		if err != nil {
			t.Fatalf("Failed to get histories for file %s: %v", filePath, err)
		}
		if len(kvHistories) != expectedCount {
			t.Fatalf("Expected %d histories in KV store for file %s, got %d", expectedCount, filePath, len(kvHistories))
		}
	}

	if len(result.Errors) > 0 {
		t.Fatalf("Expected no errors, got %d: %v", len(result.Errors), result.Errors)
	}

	// 验证文件最新内容是否正确
	for path, expectedContent := range testFiles {
		content, err := store.Get(ctx, path)
		if err != nil {
			t.Fatalf("Failed to get file %s: %v", path, err)
		}
		if string(content) != expectedContent {
			t.Fatalf("Expected content '%s' for file %s, got '%s'", expectedContent, path, string(content))
		}
	}

	// 验证 file1.txt 的初始版本内容
	file1Histories, err := store.GetHistories(ctx, "file1.txt")
	if err != nil {
		t.Fatalf("Failed to get histories for file1.txt: %v", err)
	}
	if len(file1Histories) >= 1 {
		// 获取最早的版本
		oldestVersion := file1Histories[0].Name
		oldContent, err := store.GetByVersion(ctx, "file1.txt", oldestVersion)
		if err != nil {
			t.Fatalf("Failed to get old version of file1.txt: %v", err)
		}
		// 初始版本应该是 "content1"
		if string(oldContent) != "content1" {
			t.Fatalf("Expected old content 'content1' for file1.txt, got '%s'", string(oldContent))
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
