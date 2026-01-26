package filekv

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cabify/timex"
)

type Version struct {
	Name    string
	Version string
	Meta    map[string]string
	hasMeta bool
}

// KeyValueStore 是键值存储接口
// 提供基本的键值操作、版本控制和元数据管理功能
type KeyValueStore interface {
	// Get 获取指定键的最新值
	// ctx: 上下文，用于取消或超时控制
	// key: 键名，支持多层级路径（如 "a/b/c"）
	// 返回值：键对应的值和错误信息
	Get(ctx context.Context, key string) ([]byte, error)

	// GetByVersion 根据版本获取键的值
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	// version: 版本号，当为 "head" 时表示获取最新版本
	GetByVersion(ctx context.Context, key string, version string) ([]byte, error)

	// Set 设置键的值，同时创建历史记录
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	// value: 要设置的值
	// 返回值：新版本号（如果值与上次相同则返回空串）和错误信息
	// 当 value 和上次相等时，不保存，不产生历史记录，返回值中 version 返回空串
	Set(ctx context.Context, key string, value []byte) (version string, err error)

	// SetWithTimestamp 设置键的值，使用指定的时间戳作为版本号
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	// value: 要设置的值
	// timestamp: 时间戳，单位为纳秒
	// 返回值：新版本号（如果值与上次相同则返回空串）和错误信息
	// 当 value 和上次相等时，不保存，不产生历史记录，返回值中 version 返回空串
	SetWithTimestamp(ctx context.Context, key string, value []byte, timestamp int64) (version string, err error)

	// SetMeta 设置键的元数据
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	// version: 版本号，当为 "head" 时表示最后一次历史记录
	// meta: 要设置的元数据
	// 当 version 为 head 时表示查询最后一次历史记录，同时注意下面两点：
	// 1. 当 key 存在时，在写 meta 之前要查询最后一次历史记录
	// 2. 当历史记录为空时要以 key 的创建时间为 time 来建一个历史记录
	SetMeta(ctx context.Context, key, version string, meta map[string]string) error

	// UpdateMeta 更新键的部分元数据
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	// version: 版本号，当为 "head" 时表示最后一次历史记录
	// meta: 要更新的元数据（仅更新提供的键值对）
	UpdateMeta(ctx context.Context, key, version string, meta map[string]string) error

	// Delete 删除键及其数据
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	// removeHistories: 是否同时删除历史记录
	// 注意 key 是多层的，当有一个 a/b/c 时，删除 a 时要返回失败
	Delete(ctx context.Context, key string, removeHistories bool) error

	// Exists 检查键是否存在
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	// 注意 key 是多层的，当有一个 a/b/c 时，检测 a/b 时要返回不存在
	Exists(ctx context.Context, key string) (bool, error)

	// ListKeys 列出指定前缀的所有键
	// ctx: 上下文，用于取消或超时控制
	// prefix: 键的前缀，列出以此开头的所有键
	// 要跳过 .history 等特殊目录
	ListKeys(ctx context.Context, prefix string) ([]string, error)

	// GetHistories 获取键的所有历史版本
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	GetHistories(ctx context.Context, key string) ([]Version, error)

	// GetLastVersion 获取键的最后版本信息
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	GetLastVersion(ctx context.Context, key string) (*Version, error)

	// GetPrevVersion 获取键的指定版本的前一个版本信息
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	// version: 版本号，当为 "head" 时表示获取最新版本
	GetPrevVersion(ctx context.Context, key, revision string) (*Version, error)

	// GetNextVersion 获取键的指定版本的下一个版本信息
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	// version: 版本号，当为 "head" 时表示获取最新版本
	GetNextVersion(ctx context.Context, key, revision string) (*Version, error)

	// CleanupHistoriesByTime 清理指定时间之前的旧历史记录
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	// maxAge: 最大保留时间，超过此时间的历史记录将被清理
	CleanupHistoriesByTime(ctx context.Context, key string, maxAge time.Duration) error

	// CleanupHistoriesByCount 清理超出指定数量的旧历史记录
	// ctx: 上下文，用于取消或超时控制
	// key: 键名
	// maxCount: 最大保留数量，超出此数量的历史记录将被清理
	CleanupHistoriesByCount(ctx context.Context, key string, maxCount int) error

	// Fsck 文件系统检查，修复不一致状态
	// ctx: 上下文，用于取消或超时控制
	// 实现以下功能：
	// 1: 当历史记录超过 200 个时，组织成子目录结构，按时间分页存储
	// 2: 删除不存在键对应的历史记录
	// 3: 确保每个存在的键都有对应的历史记录，如果没有则从当前值创建
	Fsck(ctx context.Context) error
}

const (
	metaSuffix       = ".meta"
	historyDirSuffix = ".h"
	historyDirConst  = ".history"
	pagePrefix       = "p_"
	maxHistoryCount  = 200
)

type wrapErr struct {
	err error
	msg string
}

func (w *wrapErr) Error() string {
	return w.msg + ": " + w.err.Error()
}

func (w *wrapErr) Unwrap() error {
	return w.err
}

func errorWrap(err error, msg string) error {
	if err == nil {
		return nil
	}
	return &wrapErr{err: err, msg: msg}
}

type FileKVStore struct {
	rootDir       string
	ignoreWarning bool
}

func NewFileKVStore(rootDir string) *FileKVStore {
	return &FileKVStore{
		rootDir: rootDir,
	}
}

func (f *FileKVStore) validateKey(key string) error {
	if key == "" {
		return errors.New("invalid key: must not empty")
	}
	if strings.HasPrefix(key, "/") || strings.Contains(key, "\\") {
		return errors.New("invalid key: must not start with '/' or contain '\\'")
	}

	parts := strings.Split(key, "/")
	for _, part := range parts {
		if part == "" {
			continue // Empty parts are allowed (e.g., "a//b")
		}
		if strings.HasPrefix(part, ".") ||
			strings.HasPrefix(part, pagePrefix) ||
			strings.HasSuffix(part, historyDirSuffix) {
			return errors.New("invalid key part: '" + part + "' cannot start with '.' or 'p_' or end with '.h'")
		}
	}
	return nil
}

func (f *FileKVStore) keyToPath(key string) string {
	return filepath.Join(f.rootDir, key)
}

func (f *FileKVStore) keyToHistoryPath(key string) string {
	return filepath.Join(f.rootDir, historyDirConst, key+historyDirSuffix)
}

func (f *FileKVStore) readProperties(filePath string) (map[string]string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errorWrap(err, "reading meta file")
	}

	properties := make(map[string]string)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := scanner.Text()
		if idx := strings.Index(line, "="); idx > 0 {
			key := strings.TrimSpace(line[:idx])
			value := strings.TrimSpace(line[idx+1:])
			properties[key] = value
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, errorWrap(err, "scanning meta file '"+filePath+"'")
	}

	return properties, nil
}

func (f *FileKVStore) writeProperties(filePath string, props map[string]string) error {
	var buf bytes.Buffer
	if len(props) > 0 {
		for k, v := range props {
			buf.WriteString(k)
			buf.WriteString("=")
			buf.WriteString(v)
			buf.WriteString("\n")
		}
	}

	// Try to write the file directly
	err := os.WriteFile(filePath, buf.Bytes(), 0644)
	if err != nil {
		if !os.IsNotExist(err) {
			return errorWrap(err, "writing meta file")
		}

		// Directory doesn't exist, create it and retry
		dir := filepath.Dir(filePath)
		if mkdirErr := os.MkdirAll(dir, 0755); mkdirErr != nil {
			return errorWrap(mkdirErr, "creating directory")
		}
		// Retry writing the file after creating the directory
		err = os.WriteFile(filePath, buf.Bytes(), 0644)
		if err != nil {
			return errorWrap(err, "writing meta file")
		}
	}
	return nil
}

func (f *FileKVStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := f.validateKey(key); err != nil {
		return nil, err
	}

	dataFile := f.keyToPath(key)
	data, err := os.ReadFile(dataFile)
	if err != nil {
		return nil, errorWrap(err, "reading file")
	}
	return data, nil
}

func (f *FileKVStore) searchVersionInSubDirs(ctx context.Context, historyDir string, version string, isExist func(versionFile string) error) (string, error) {
	entries, err := os.ReadDir(historyDir)
	if err != nil {
		return "", errorWrap(err, "reading history directory")
	}

	var errList []error
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), pagePrefix) {
			continue
		}

		subdirPath := filepath.Join(historyDir, entry.Name())
		versionFile := filepath.Join(subdirPath, version)

		err := isExist(versionFile)
		if err == nil {
			return versionFile, nil
		}
		if !os.IsNotExist(err) {
			errList = append(errList, err)
		}
	}
	if len(errList) == 0 {
		return "", os.ErrNotExist
	}
	if len(errList) == 1 {
		return "", errList[0]
	}
	return "", errors.Join(errList...)
}

func (f *FileKVStore) GetByVersion(ctx context.Context, key string, version string) ([]byte, error) {
	if version == "head" || version == "HEAD" {
		return f.Get(ctx, key)
	}

	if err := f.validateKey(key); err != nil {
		return nil, err
	}
	historyDir := f.keyToHistoryPath(key)

	// First check default directory
	defaultPath := filepath.Join(historyDir, version)
	data, err := os.ReadFile(defaultPath)
	if err == nil {
		return data, nil
	}
	if !os.IsNotExist(err) {
		return nil, errorWrap(err, "reading history")
	}

	_, err = f.searchVersionInSubDirs(ctx, historyDir, version, func(versionFile string) error {
		data, err = os.ReadFile(versionFile)
		return err
	})
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("version '" + version + "' not found for key '" + key + "'")
		}
		return nil, errorWrap(err, "reading history")
	}
	return data, nil
}

func (f *FileKVStore) Set(ctx context.Context, key string, value []byte) (string, error) {
	return f.SetWithTimestamp(ctx, key, value, timex.Now().UnixNano())
}

func (f *FileKVStore) SetWithTimestamp(ctx context.Context, key string, value []byte, timestamp int64) (string, error) {
	if err := f.validateKey(key); err != nil {
		return "", err
	}

	dataFile := f.keyToPath(key)

	// Read existing value to compare
	existingValue, err := os.ReadFile(dataFile)
	var shouldCreateHistory bool
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, need to create history
			shouldCreateHistory = true
		} else {
			return "", errorWrap(err, "reading file for comparison")
		}
	} else {
		// File exists, check if content has changed
		shouldCreateHistory = !bytes.Equal(existingValue, value)
	}

	// Create history record if needed
	timestampStr := strconv.FormatInt(timestamp, 10)
	historyDir := f.keyToHistoryPath(key)
	var historyFile string

	// Ensure data directory exists before writing
	if err := os.MkdirAll(filepath.Dir(dataFile), 0755); err != nil {
		return "", errorWrap(err, "creating data directory")
	}

	// Write new value
	if err := os.WriteFile(dataFile, value, 0644); err != nil {
		return "", errorWrap(err, "writing file")
	}

	// If content hasn't changed, return empty version string
	if !shouldCreateHistory {
		return "", nil
	}

	// Ensure history directory exists before writing history
	if err := os.MkdirAll(historyDir, 0755); err != nil {
		return "", errorWrap(err, "creating history directory")
	}

	// Ensure history file name is unique
	counter := 0
	historyFile = filepath.Join(historyDir, timestampStr)
	for {
		if _, err := os.Stat(historyFile); os.IsNotExist(err) {
			break
		}
		counter++
		historyFile = filepath.Join(historyDir, timestampStr+"_"+strconv.Itoa(counter))
	}

	// Write to history file
	if err := os.WriteFile(historyFile, value, 0644); err != nil {
		return "", errorWrap(err, "writing history file")
	}

	return timestampStr, nil
}

func (f *FileKVStore) ensureHistoryRecordExists(key, historyDir string, timestamp int64) (string, error) {
	timestampStr := strconv.FormatInt(timestamp, 10)
	historyFile := filepath.Join(historyDir, timestampStr)

	// Create history record from current value
	currentValue, err := f.Get(context.Background(), key)
	if err != nil {
		return "", err
	}

	err = os.WriteFile(historyFile, currentValue, 0644)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", errorWrap(err, "writing history file")
		}
		// Directory doesn't exist, create it and retry
		if mkdirErr := os.MkdirAll(historyDir, 0755); mkdirErr != nil {
			return "", errorWrap(mkdirErr, "creating history directory")
		}
		// Retry writing the file after creating the directory
		err = os.WriteFile(historyFile, currentValue, 0644)
		if err != nil {
			return "", errorWrap(err, "writing history file")
		}
	}
	return timestampStr, nil
}

func (f *FileKVStore) SetMeta(ctx context.Context, key, version string, meta map[string]string) error {
	if err := f.validateKey(key); err != nil {
		return err
	}

	historyDir := f.keyToHistoryPath(key)

	if version == "head" || version == "HEAD" {
		lastVersion, err := f.GetLastVersion(ctx, key)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			// If no history exists, create one based on current value
			timestamp := timex.Now().UnixNano()
			versionName, err := f.ensureHistoryRecordExists(key, historyDir, timestamp)
			if err != nil {
				return err
			}
			version = versionName
		} else {
			version = lastVersion.Name
		}

		// First try default directory
		metaFile := filepath.Join(historyDir, version+metaSuffix)
		return f.writeProperties(metaFile, meta)
	}

	versionFile := filepath.Join(historyDir, version)
	_, err := os.Stat(versionFile)
	if err != nil {
		if !os.IsNotExist(err) {
			return errorWrap(err, "check history")
		}
		versionFile, err = f.searchVersionInSubDirs(ctx, historyDir, version, func(versionFile string) error {
			_, err := os.Stat(versionFile)
			return err
		})
		if err != nil {
			if os.IsNotExist(err) {
				return errorWrap(os.ErrNotExist, "no history found for key '"+key+"'")
			}
			return errorWrap(err, "search history")
		}
	}
	return f.writeProperties(versionFile+metaSuffix, meta)
}

func (f *FileKVStore) UpdateMeta(ctx context.Context, key, version string, meta map[string]string) error {
	if err := f.validateKey(key); err != nil {
		return err
	}

	historyDir := f.keyToHistoryPath(key)

	var metaFile string
	if version == "head" || version == "HEAD" {
		lastVersion, err := f.GetLastVersion(ctx, key)
		if err != nil {
			// If no history exists, create one based on current value
			timestamp := timex.Now().UnixNano()
			versionName, err := f.ensureHistoryRecordExists(key, historyDir, timestamp)
			if err != nil {
				return err
			}
			version = versionName
		} else {
			version = lastVersion.Name
		}

		// First try default directory
		metaFile = filepath.Join(historyDir, version+metaSuffix)
	} else {
		versionFile := filepath.Join(historyDir, version)
		_, err := os.Stat(versionFile)
		if err != nil {
			if !os.IsNotExist(err) {
				return errorWrap(err, "check default history")
			}
			versionFile, err = f.searchVersionInSubDirs(ctx, historyDir, version, func(versionFile string) error {
				_, err := os.Stat(versionFile)
				return err
			})
			if err != nil {
				if os.IsNotExist(err) {
					return errorWrap(os.ErrNotExist, "no history found for key '"+key+"'")
				}
				return errorWrap(err, "search history")
			}
		}

		metaFile = versionFile + metaSuffix
	}

	// Read existing metadata
	existingMeta, err := f.readProperties(metaFile)
	if err != nil && !os.IsNotExist(err) {
		return errorWrap(err, "reading meta file")
	}
	// Merge with new metadata
	if len(existingMeta) == 0 {
		existingMeta = meta
	} else {
		for k, v := range meta {
			existingMeta[k] = v
		}
	}
	return f.writeProperties(metaFile, existingMeta)
}

func (f *FileKVStore) Delete(ctx context.Context, key string, removeHistories bool) error {
	if err := f.validateKey(key); err != nil {
		return err
	}

	keyPath := f.keyToPath(key)

	// Check if there are child keys
	st, err := os.Stat(keyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errorWrap(err, "checking existence of key '"+key+"'")
	}
	if st.IsDir() {
		return errors.New("cannot delete key " + key + ": it has child keys")
	}
	if removeHistories {
		historyDir := f.keyToHistoryPath(key)
		if err := os.RemoveAll(historyDir); err != nil && !os.IsNotExist(err) {
			return errorWrap(err, "removing history directory")
		}
	}

	if err := os.Remove(keyPath); err != nil {
		return errorWrap(err, "removing file")
	}
	return nil
}

func (f *FileKVStore) Exists(ctx context.Context, key string) (bool, error) {
	if err := f.validateKey(key); err != nil {
		return false, err
	}

	path := f.keyToPath(key)
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errorWrap(err, "checking existence of key '"+key+"'")
	}
	if st.IsDir() {
		return false, nil
	}
	return true, nil
}

func (f *FileKVStore) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	var keys []string

	err := filepath.WalkDir(f.rootDir, func(pa string, d fs.DirEntry, err error) error {
		if err != nil {
			return errorWrap(err, "walking directory '"+pa+"'")
		}
		if d.Name() == "." {
			return filepath.SkipDir
		}
		if d.Name() == historyDirConst {
			return filepath.SkipDir
		}
		if strings.HasPrefix(d.Name(), pagePrefix) {
			return filepath.SkipDir
		}
		if strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}
		if strings.HasSuffix(d.Name(), historyDirSuffix) {
			return filepath.SkipDir
		}

		relPath, err := filepath.Rel(f.rootDir, pa)
		if err != nil {
			return errorWrap(err, "getting relative path")
		}

		// Convert backslashes to forward slashes for consistent handling
		relPath = strings.ReplaceAll(relPath, "\\", "/")

		if d.IsDir() {
			// 对于目录，我们不应该根据前缀跳过，因为它可能包含匹配前缀的文件
			if len(relPath) > len(prefix) {
				if !strings.HasPrefix(relPath, prefix) {
					return filepath.SkipDir
				}
			}
			return nil
		}

		if prefix == "" {
			keys = append(keys, relPath)
		} else {
			// Only include files (not directories)
			if strings.HasPrefix(relPath, prefix) {
				keys = append(keys, relPath)
			}
		}
		return nil
	})

	return keys, err
}

func traverseDir(historyDir, prefix string, traverseSubDir bool, errList *[]error,
	callback func(historyFile, name, version string, hasMeta bool, info fs.DirEntry) (bool, error)) bool {
	entries, err := os.ReadDir(historyDir)
	if err != nil {
		if os.IsNotExist(err) {
			return true
		}
		*errList = append(*errList, errorWrap(err, "reading history directory"))
		return true
	}

	var metas = map[string]struct{}{}
	var offset = 0
	for i, entry := range entries {
		if entry.IsDir() {
			if traverseSubDir && strings.HasPrefix(entry.Name(), pagePrefix) {
				entryName := entry.Name()
				fullName := entryName
				if prefix != "" {
					fullName = prefix + "/" + entryName
				}

				continueTraverse := traverseDir(filepath.Join(historyDir, entryName), fullName, false, errList, callback)
				if !continueTraverse {
					return false
				}
			}
			continue
		}

		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		if strings.HasSuffix(entry.Name(), metaSuffix) {
			metas[strings.TrimSuffix(entry.Name(), metaSuffix)] = struct{}{}
			continue
		}

		if offset != i {
			entries[offset] = entries[i]
		}
		offset++
	}
	entries = entries[:offset]

	for _, entry := range entries {
		entryName := entry.Name()
		fullName := entryName
		if prefix != "" {
			fullName = prefix + "/" + entryName
		}

		_, metaExist := metas[entryName]
		entryPath := filepath.Join(historyDir, entryName)
		continueTraverse, err := callback(entryPath, fullName, entryName, metaExist, entry)
		if err != nil {
			*errList = append(*errList, err)
		}
		if !continueTraverse {
			return false
		}
	}
	return true
}

// foreachHistories 遍历指定历史目录下的所有历史记录，对每个历史记录执行回调函数
// historyDir: 历史记录目录
// callback: 回调函数，接收历史记录的文件路径、版本号和文件状态，返回是否继续遍历和错误
func (f *FileKVStore) foreachHistories(historyDir string, callback func(historyFile, name, version string, hasMeta bool, info fs.DirEntry) (bool, error)) []error {
	var errList []error
	traverseDir(historyDir, "", true, &errList, callback)
	return errList
}

// readHistories 枚举指定键的所有版本，返回不包含元数据的 Version 切片
func (f *FileKVStore) readHistories(ctx context.Context, historyDir string) ([]Version, error) {
	var versions []Version

	// 使用 foreachHistories 遍历所有版本文件，同时获取 hasMeta 信息
	errList := f.foreachHistories(historyDir, func(historyFile, name, version string, hasMeta bool, info fs.DirEntry) (bool, error) {
		versions = append(versions, Version{
			Name:    name,
			Version: version,
			hasMeta: hasMeta,
		})
		return true, nil
	})

	if len(errList) > 0 {
		if len(errList) == 1 {
			return nil, errList[0]
		}
		return nil, errors.Join(errList...)
	}

	// 按版本号排序（升序）
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Version < versions[j].Version
	})

	return versions, nil
}

func (f *FileKVStore) GetHistories(ctx context.Context, key string) ([]Version, error) {
	if err := f.validateKey(key); err != nil {
		return nil, err
	}

	historyDir := f.keyToHistoryPath(key)

	// 第一步：枚举所有版本
	versions, err := f.readHistories(ctx, historyDir)
	if err != nil {
		return nil, err
	}

	// 第二步：为有元数据的版本读取元数据
	for i := range versions {
		if versions[i].hasMeta {
			metaFile := filepath.Join(historyDir, versions[i].Name+metaSuffix)
			meta, err := f.readProperties(metaFile)
			if err != nil && !os.IsNotExist(err) {
				return nil, errorWrap(err, "reading meta file")
			}
			versions[i].Meta = meta
		}
	}

	return versions, nil
}

func (f *FileKVStore) GetLastVersion(ctx context.Context, key string) (*Version, error) {
	if err := f.validateKey(key); err != nil {
		return nil, err
	}

	historyDir := f.keyToHistoryPath(key)
	var maxTime int64 = 0
	var latestVersionName string
	var latestHistoryFile string
	var hasMeta bool

	// 使用 foreachHistories 遍历所有版本文件，找到最新版本
	errList := f.foreachHistories(historyDir, func(historyFile, name, version string, metaExists bool, info fs.DirEntry) (bool, error) {
		timestamp, err := strconv.ParseInt(version, 10, 64)
		if err != nil {
			return true, nil
		}

		if timestamp > maxTime {
			maxTime = timestamp
			latestVersionName = name
			latestHistoryFile = historyFile
			hasMeta = metaExists
		}
		return true, nil
	})

	if len(errList) > 0 {
		return nil, errors.Join(errList...)
	}

	if maxTime == 0 {
		return nil, errorWrap(os.ErrNotExist, "no history found for key '"+key+"'")
	}

	// 读取元数据
	var meta map[string]string
	if hasMeta {
		var err error
		meta, err = f.readProperties(latestHistoryFile + metaSuffix)
		if err != nil && !os.IsNotExist(err) {
			return nil, errorWrap(err, "reading meta file")
		}
	}

	return &Version{
		Name:    latestVersionName,
		Version: strconv.FormatInt(maxTime, 10),
		Meta:    meta,
	}, nil
}

func (f *FileKVStore) GetPrevVersion(ctx context.Context, key, revision string) (*Version, error) {
	if err := f.validateKey(key); err != nil {
		return nil, err
	}

	historyDir := f.keyToHistoryPath(key)

	// Get all histories (using readHistories instead of GetHistories for better performance)
	histories, err := f.readHistories(ctx, historyDir)
	if err != nil {
		return nil, err
	}
	if len(histories) == 0 {
		return nil, errorWrap(os.ErrNotExist, "no history found for key '"+key+"'")
	}

	// Find the target version index
	targetIndex := -1
	if revision == "head" || revision == "HEAD" {
		// For HEAD, we want the previous of the last version
		if len(histories) < 2 {
			// No previous version
			return nil, errorWrap(os.ErrNotExist, "no previous version found")
		}
		targetIndex = len(histories) - 1
	} else {
		// Find the index of the specified revision
		for i, v := range histories {
			if v.Version == revision {
				targetIndex = i
				break
			}
		}

		if targetIndex == -1 {
			return nil, errorWrap(os.ErrNotExist, "version '"+revision+"' not found for key '"+key+"'")
		}
	}

	// Get the previous version
	if targetIndex == 0 {
		// No previous version
		return nil, errorWrap(os.ErrNotExist, "no previous version found")
	}

	return &histories[targetIndex-1], nil
}

func (f *FileKVStore) GetNextVersion(ctx context.Context, key, revision string) (*Version, error) {
	if revision == "head" || revision == "HEAD" {
		return nil, errorWrap(os.ErrNotExist, "no next version found")
	}

	if err := f.validateKey(key); err != nil {
		return nil, err
	}

	historyDir := f.keyToHistoryPath(key)

	// Get all histories (using readHistories instead of GetHistories for better performance)
	histories, err := f.readHistories(ctx, historyDir)
	if err != nil {
		return nil, err
	}
	if len(histories) == 0 {
		return nil, errorWrap(os.ErrNotExist, "no history found for key '"+key+"'")
	}

	// Find the target version index
	targetIndex := -1
	// Find the index of the specified revision
	for i, v := range histories {
		if v.Version == revision {
			targetIndex = i
			break
		}
	}

	if targetIndex == -1 {
		return nil, errorWrap(os.ErrNotExist, "version '"+revision+"' not found for key '"+key+"'")
	}

	// Get the next version
	if targetIndex == len(histories)-1 {
		// No next version
		return nil, errorWrap(os.ErrNotExist, "no next version found")
	}

	return &histories[targetIndex+1], nil
}

func (f *FileKVStore) CleanupHistoriesByTime(ctx context.Context, key string, maxAge time.Duration) error {
	if err := f.validateKey(key); err != nil {
		return err
	}

	historyDir := f.keyToHistoryPath(key)
	cutoffTime := timex.Now().Add(-maxAge).Unix()

	errList := f.foreachHistories(historyDir, func(historyFile, name, version string, hasMeta bool, info fs.DirEntry) (bool, error) {
		timestamp, err := strconv.ParseInt(version, 10, 64)
		if err != nil {
			return true, nil
		}

		if timestamp < cutoffTime {
			// Remove the history file and its meta file
			if err := os.Remove(historyFile); err != nil && !os.IsNotExist(err) {
				return true, errorWrap(err, "removing history file")
			}
			if hasMeta {
				if err := os.Remove(historyFile + metaSuffix); err != nil && !os.IsNotExist(err) {
					return true, errorWrap(err, "removing history meta file")
				}
			}
		}
		return true, nil
	})

	if len(errList) > 0 {
		if len(errList) == 1 {
			return errList[0]
		}
		return errors.Join(errList...)
	}

	return nil
}

func (f *FileKVStore) CleanupHistoriesByCount(ctx context.Context, key string, maxCount int) error {
	if err := f.validateKey(key); err != nil {
		return err
	}

	historyDir := f.keyToHistoryPath(key)

	// Collect all history files
	var allHistories []Version

	errList := f.foreachHistories(historyDir, func(historyFile, name, version string, hasMeta bool, info fs.DirEntry) (bool, error) {
		allHistories = append(allHistories, Version{
			Name:    name,
			Version: version,
			hasMeta: hasMeta,
		})
		return true, nil
	})

	if len(errList) > 0 {
		if len(errList) == 1 {
			return errList[0]
		}
		return errors.Join(errList...)
	}

	// Sort by timestamp (oldest first)
	sort.Slice(allHistories, func(i, j int) bool {
		return allHistories[i].Version < allHistories[j].Version
	})

	// Determine which histories to keep
	if len(allHistories) <= maxCount {
		return nil
	}
	toRemove := allHistories[:len(allHistories)-maxCount]

	// Delete histories that should be removed
	var deleteErrList []error
	for _, history := range toRemove {
		historyFile := filepath.Join(historyDir, history.Name)
		if err := os.Remove(historyFile); err != nil && !os.IsNotExist(err) {
			deleteErrList = append(deleteErrList, errorWrap(err, "removing history file '"+historyFile+"'"))
		}
		if history.hasMeta {
			if err := os.Remove(historyFile + metaSuffix); err != nil && !os.IsNotExist(err) {
				deleteErrList = append(deleteErrList, errorWrap(err, "removing meta file for '"+historyFile+"'"))
			}
		}
	}

	if len(deleteErrList) > 0 {
		if len(deleteErrList) == 1 {
			return deleteErrList[0]
		}
		return errors.Join(deleteErrList...)
	}

	return nil
}

// organizeHistoriesIfNeeded 组织历史记录到子目录中（如果需要）
// 如果某个键的历史记录数量超过 maxHistoryCount，则将较早的历史记录移动到按时间命名的子目录中
// 最新的历史记录仍保留在默认目录下。
func (f *FileKVStore) organizeHistoriesIfNeeded(key, historyDir string) error {
	var allHistories []string

	// Add histories from default directory
	entries, err := os.ReadDir(historyDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 如果历史目录不存在，无需处理
		}
		return errorWrap(err, "reading history path")
	}
	metas := map[string]struct{}{}
	for _, entry := range entries {
		if entry.IsDir() {
			// Skip subdirectories for now, we'll process them separately
			continue
		}
		if strings.HasPrefix(entry.Name(), ".") {
			continue // Skip . files
		}
		if strings.HasSuffix(entry.Name(), metaSuffix) {
			metas[strings.TrimSuffix(entry.Name(), metaSuffix)] = struct{}{}
			continue // Skip meta files
		}
		allHistories = append(allHistories, entry.Name())
	}
	// Sort by timestamp (oldest first)
	sort.Slice(allHistories, func(i, j int) bool {
		return allHistories[i] < allHistories[j]
	})

	// 保留最新的一个在默认目录（如果有历史记录）
	allHistoriesForOrganizing := allHistories
	if len(allHistoriesForOrganizing) > 1 {
		allHistoriesForOrganizing = allHistoriesForOrganizing[:len(allHistoriesForOrganizing)-1]
	}

	// 按 maxHistoryCount 分组
	for len(allHistoriesForOrganizing) >= maxHistoryCount {
		pageHistories := allHistoriesForOrganizing[:maxHistoryCount]
		pageDirName := pagePrefix + pageHistories[0]
		pageDirPath := filepath.Join(historyDir, pageDirName)

		// 创建子目录
		err = os.MkdirAll(pageDirPath, 0755)
		if err != nil {
			return errorWrap(err, "creating page directory")
		}

		// 将该页的历史记录移动到子目录
		for _, historyName := range pageHistories {
			oldPath := filepath.Join(historyDir, historyName)
			newPath := filepath.Join(pageDirPath, historyName)

			if err := os.Rename(oldPath, newPath); err != nil {
				return errorWrap(err, "moving history file from "+oldPath+" to "+newPath)
			}

			_, exists := metas[historyName]
			if exists {
				oldMetaPath := oldPath + metaSuffix
				newMetaPath := newPath + metaSuffix
				if _, statErr := os.Stat(oldMetaPath); statErr == nil {
					if err := os.Rename(oldMetaPath, newMetaPath); err != nil {
						return errorWrap(err, "moving history meta file from "+oldMetaPath+" to "+newMetaPath)
					}
				}
			}
		}
		allHistoriesForOrganizing = allHistoriesForOrganizing[200:]
	}
	return nil
}

// walkAndOrganizeHistories 改进版：先列出所有键，然后逐一处理历史文件的组织
func (f *FileKVStore) walkAndOrganizeHistories(ctx context.Context) error {
	allMainKeys, err := f.ListKeys(ctx, "")
	if err != nil {
		return errorWrap(err, "listing all keys from main directory")
	}

	var errList []error
	for _, key := range allMainKeys {
		if validateErr := f.validateKey(key); validateErr != nil {
			if f.ignoreWarning {
				errList = append(errList, errorWrap(validateErr, "invalid key found during organization: "+key))
				continue
			} else {
				return errorWrap(validateErr, "invalid key found during organization: "+key)
			}
		}

		historyDir := f.keyToHistoryPath(key)
		err := f.organizeHistoriesIfNeeded(key, historyDir)
		if err != nil {
			if f.ignoreWarning {
				errList = append(errList, err)
				continue
			} else {
				return err
			}
		}
	}

	if len(errList) > 0 {
		if len(errList) == 1 {
			return errList[0]
		}
		return errors.Join(errList...)
	}

	return nil
}

// removeOrphanedHistories 删除孤立的历史记录（即对应键已不存在的历史记录）
func (f *FileKVStore) removeOrphanedHistories(ctx context.Context, historyRoot string) error {
	// Walk through the entire history directory tree
	err := filepath.WalkDir(historyRoot, func(pa string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return errorWrap(err, "accessing path "+pa)
		}
		if !d.IsDir() {
			return nil // Skip files
		}
		if strings.HasPrefix(d.Name(), ".") {
			return nil // Skip the root history directory itself
		}
		if !strings.HasSuffix(d.Name(), historyDirSuffix) {
			return nil
		}

		relPath, err := filepath.Rel(historyRoot, pa)
		if err != nil {
			return errorWrap(err, "getting relative path for "+pa)
		}
		if relPath == "." {
			return nil // Skip the root history directory itself
		}

		// Extract the original key from the directory name
		key := strings.TrimSuffix(relPath, historyDirSuffix)
		// Normalize the key path separator to forward slash
		key = strings.ReplaceAll(key, "\\", "/")

		// Check if the corresponding key still exists in the main data directory
		exists, err := f.Exists(ctx, key)
		if err != nil {
			return err
		}
		if !exists {
			// Key does not exist, remove its history directory
			if err := os.RemoveAll(pa); err != nil {
				return errorWrap(err, "removing orphaned history directory")
			}
		}
		return filepath.SkipDir
	})

	return err
}

// hasHistories 检查指定键是否有历史记录，并根据 ignoreWarning 设置处理错误
// 返回: hasHistory(bool), fatalErr(error)
func (f *FileKVStore) hasHistories(historyDir, key string, errList *[]error) (bool, error) {
	// 使用 foreachHistories 遍历历史记录，如果回调被调用，说明有历史记录
	found := false
	errList2 := f.foreachHistories(historyDir, func(historyFile, name, version string, hasMeta bool, info fs.DirEntry) (bool, error) {
		// 只要找到一个版本文件，就说明有历史记录
		found = true
		// 找到后立即停止遍历
		return false, nil
	})

	if len(errList2) > 0 {
		if f.ignoreWarning {
			for _, err := range errList2 {
				*errList = append(*errList, err)
			}
			return false, nil
		} else {
			return false, errors.Join(errList2...)
		}
	}

	return found, nil
}

// ensureHistoryForExistingKeys 确保存在的键都有对应的历史记录
// 如果某个存在的键没有历史记录，则基于其当前值创建一个。
// 改进：先用 ListKeys 列出所有 key 之后，再处理。
func (f *FileKVStore) ensureHistoryForExistingKeys(ctx context.Context, historyRoot string) error {
	// 1. 获取所有现存的主键
	allMainKeys, err := f.ListKeys(ctx, "") // 获取所有键
	if err != nil {
		return errorWrap(err, "listing all keys from main directory")
	}

	var errList []error // 用于收集过程中的错误

	for _, key := range allMainKeys {
		if validateErr := f.validateKey(key); validateErr != nil {
			if f.ignoreWarning {
				errList = append(errList, errorWrap(validateErr, "invalid key found during fsck: "+key))
				continue
			} else {
				return errorWrap(validateErr, "invalid key found during fsck: "+key)
			}
		}

		historyDir := f.keyToHistoryPath(key)

		hasHistory, fatalErr := f.hasHistories(historyDir, key, &errList)
		if fatalErr != nil {
			return fatalErr
		}
		if !hasHistory {
			timestamp := timex.Now().UnixNano()
			_, createErr := f.ensureHistoryRecordExists(key, historyDir, timestamp)
			if createErr != nil {
				if f.ignoreWarning {
					// 如果忽略警告，则记录错误并跳过此键
					errList = append(errList, errorWrap(createErr, "failed to create initial history for key '"+key+"'"))
				} else {
					// 如果不忽略警告，则视为致命错误
					return errorWrap(createErr, "failed to create initial history for key '"+key+"'")
				}
			}
		}
	}

	if len(errList) > 0 {
		if len(errList) == 1 {
			return errList[0]
		}
		return errors.Join(errList...)
	}

	return nil
}

// Fsck 执行文件系统检查和修复操作
// 实现以下功能：
// 8.1: 当历史记录超过 200 个时，组织成子目录结构，按时间分页存储
// 8.2: 删除不存在键对应的历史记录
// 8.3: 确保每个存在的键都有对应的历史记录，如果没有则从当前值创建
func (f *FileKVStore) Fsck(ctx context.Context) error {
	historyRoot := filepath.Join(f.rootDir, historyDirConst)

	// 8.2: 删除孤立的历史记录
	if err := f.removeOrphanedHistories(ctx, historyRoot); err != nil {
		return err
	}

	// 8.1: Walk through the history directory and organize histories if needed
	if err := f.walkAndOrganizeHistories(ctx); err != nil {
		return err
	}

	// 8.3: Ensure every existing key has history records
	if err := f.ensureHistoryForExistingKeys(ctx, historyRoot); err != nil {
		return err
	}

	return nil
}

// CachedFileKVStore implements the KeyValueStore interface with caching
type CachedFileKVStore struct {
	store *FileKVStore
	cache map[string][]byte
}

func NewCachedFileKVStore(store *FileKVStore) *CachedFileKVStore {
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
	return c.SetWithTimestamp(ctx, key, value, timex.Now().UnixNano())
}

func (c *CachedFileKVStore) SetWithTimestamp(ctx context.Context, key string, value []byte, timestamp int64) (string, error) {
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
