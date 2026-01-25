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
)

type Version struct {
	Name      string
	Version string
	Meta      map[string]string
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
	metaSuffix = ".meta"
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

func (f *FileKVStore) searchVersionPath(ctx context.Context, historyDir string, version string, isExist func(versionFile string) error) (string, error) {
	// Check subdirectories
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

	_, err = f.searchVersionPath(ctx, historyDir, version, func(versionFile string) error {
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
	if err := f.validateKey(key); err != nil {
		return "", err
	}

	dataFile := f.keyToPath(key)

	// Read existing value to compare
	existingValue, err := os.ReadFile(dataFile)
	if err != nil && !os.IsNotExist(err) {
		return "", errorWrap(err, "reading file for comparison")
	}

	// If value is the same, don't create new history
	if bytes.Equal(existingValue, value) {
		return "", nil
	}

	// Write new value
	err = os.WriteFile(dataFile, value, 0644)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", errorWrap(err, "writing file")
		}

		// Directory doesn't exist, create it and retry
		if mkdirErr := os.MkdirAll(filepath.Dir(dataFile), 0755); mkdirErr != nil {
			return "", errorWrap(mkdirErr, "creating directory")
		}

		// Retry writing the file after creating the directory
		err = os.WriteFile(dataFile, value, 0644)
		if err != nil {
			return "", errorWrap(err, "writing file")
		}
	}

	// Create history record
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	historyDir := f.keyToHistoryPath(key)
	historyFile := filepath.Join(historyDir, timestamp)

	err = os.WriteFile(historyFile, value, 0644)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", errorWrap(err, "writing history file")
		}
		// Directory doesn't exist, create it and retry
		mkdirErr := os.MkdirAll(historyDir, 0755)
		if mkdirErr != nil {
			if !f.ignoreWarning {
				return "", errorWrap(mkdirErr, "creating history directory")
			}
		} else {
			// Retry writing the file after creating the directory
			err = os.WriteFile(historyFile, value, 0644)
			if err != nil {
				return "", errorWrap(err, "writing history file")
			}
		}
	}

	return timestamp, nil
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
			timestamp := time.Now().Unix()
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

	versionFile, err := f.searchVersionPath(ctx, historyDir, version, func(versionFile string) error {
		_, err := os.Stat(versionFile)
		return err
	})
	if err != nil {
		if os.IsNotExist(err) {
			return errorWrap(os.ErrNotExist, "no history found for key '"+key+"'")
		}
		return errorWrap(err, "reading history")
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
			timestamp := time.Now().Unix()
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
		versionFile, err := f.searchVersionPath(ctx, historyDir, version, func(versionFile string) error {
			_, err := os.Stat(versionFile)
			return err
		})
		if err != nil {
			if os.IsNotExist(err) {
				return errorWrap(os.ErrNotExist, "no history found for key '"+key+"'")
			}
			return errorWrap(err, "reading history")
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
		if d.IsDir() {
			if prefix != "" {
				relPath, err := filepath.Rel(f.rootDir, pa)
				if err != nil {
					return errorWrap(err, "getting relative path")
				}

				if !strings.HasPrefix(relPath, prefix) {
					return filepath.SkipDir
				}
			}
			return nil
		}

		relPath, err := filepath.Rel(f.rootDir, pa)
		if err != nil {
			return errorWrap(err, "getting relative path")
		}
		if relPath == "" || relPath == "." || relPath == historyDirConst {
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

func (f *FileKVStore) GetHistories(ctx context.Context, key string) ([]Version, error) {
	if err := f.validateKey(key); err != nil {
		return nil, err
	}

	historyDir := f.keyToHistoryPath(key)
	var versions []Version

	// Scan default directory
	entries, err := os.ReadDir(historyDir)
	if err != nil {
		if os.IsNotExist(err) {
			return versions, nil
		}
		return nil, errorWrap(err, "reading history directory")
	}

	for _, entry := range entries {
		if entry.IsDir() {
			if !strings.HasPrefix(entry.Name(), pagePrefix) {
				continue
			}
			// Scan subdirectory
			subEntries, err := os.ReadDir(filepath.Join(historyDir, entry.Name()))
			if err != nil {
				return nil, errorWrap(err, "reading sub directory")
			}

			for _, subEntry := range subEntries {
				if subEntry.IsDir() || strings.HasSuffix(subEntry.Name(), metaSuffix) {
					continue
				}

				versionName := entry.Name() + "/" + subEntry.Name()
				metaFile := filepath.Join(historyDir, versionName+metaSuffix)
				meta, err := f.readProperties(metaFile)
				if err != nil && !os.IsNotExist(err) {
					return nil, errorWrap(err, "reading meta file")
				}

				versions = append(versions, Version{
					Name: versionName,
					Version: subEntry.Name(),
					Meta: meta,
				})
			}
		} else {
			if strings.HasSuffix(entry.Name(), metaSuffix) {
				continue
			}

			versionName := entry.Name()
			metaFile := filepath.Join(historyDir, versionName+metaSuffix)
			meta, err := f.readProperties(metaFile)
			if err != nil && !os.IsNotExist(err) {
				return nil, errorWrap(err, "reading meta file")
			}

			versions = append(versions, Version{
				Name: versionName,
				Version: entry.Name(),
				Meta: meta,
			})
		}
	}

	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Version < versions[j].Version
	})

	return versions, nil
}

func (f *FileKVStore) GetLastVersion(ctx context.Context, key string) (*Version, error) {
	if err := f.validateKey(key); err != nil {
		return nil, err
	}

	historyDir := f.keyToHistoryPath(key)

	// Check default directory first
	entries, err := os.ReadDir(historyDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errorWrap(os.ErrNotExist, "no history found for key '"+key+"'")
		}
		return nil, errorWrap(err, "reading history directory '"+historyDir+"'")
	}

	var maxTime int64 = 0

	for _, entry := range entries {
		if entry.IsDir() ||
			strings.HasSuffix(entry.Name(), metaSuffix) {
			continue
		}

		timestamp, err := strconv.ParseInt(entry.Name(), 10, 64)
		if err != nil {
			continue
		}

		if timestamp > maxTime {
			maxTime = timestamp
		}
	}

	if maxTime > 0 {
		versionName := strconv.FormatInt(maxTime, 10)
		metaFile := filepath.Join(historyDir, versionName+".mata")
		meta, err := f.readProperties(metaFile)
		if err != nil && !os.IsNotExist(err) {
			return nil, errorWrap(err, "reading meta file")
		}

		return &Version{
			Name: versionName,
			Version: versionName,
			Meta: meta,
		}, nil
	}

	var versionFile string
	var versionName string
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), pagePrefix) {
			continue
		}

		subdirPath := filepath.Join(historyDir, entry.Name())
		subEntries, err := os.ReadDir(subdirPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}

		for _, subEntry := range subEntries {
			if subEntry.IsDir() || strings.HasSuffix(subEntry.Name(), metaSuffix) {
				continue
			}

			timestamp, err := strconv.ParseInt(subEntry.Name(), 10, 64)
			if err != nil {
				continue
			}

			if timestamp > maxTime {
				maxTime = timestamp
				versionFile = filepath.Join(subdirPath, subEntry.Name())
				versionName = entry.Name() + "/" + subEntry.Name()
			}
		}
	}

	if maxTime > 0 {
		meta, err := f.readProperties(versionFile + metaSuffix)
		if err != nil && !os.IsNotExist(err) {
			return nil, errorWrap(err, "reading meta file '"+versionFile+metaSuffix+"'")
		}
		return &Version{
			Name: versionName,
			Version: strconv.FormatInt(maxTime, 10),
			Meta: meta,
		}, nil
	}

	return nil, errorWrap(os.ErrNotExist, "no history found for key '"+key+"'")
}

func (f *FileKVStore) CleanupHistoriesByTime(ctx context.Context, key string, maxAge time.Duration) error {
	if err := f.validateKey(key); err != nil {
		return err
	}

	historyDir := f.keyToHistoryPath(key)
	cutoffTime := time.Now().Add(-maxAge).Unix()

	// Clean up default directory
	entries, err := os.ReadDir(historyDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errorWrap(err, "reading history directory")
	}

	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), pagePrefix) {
			// Process subdirectory
			subdirPath := filepath.Join(historyDir, entry.Name())
			subEntries, err := os.ReadDir(subdirPath)
			if err != nil {
				continue
			}

			for _, subEntry := range subEntries {
				if subEntry.IsDir() || strings.HasSuffix(subEntry.Name(), metaSuffix) {
					continue
				}

				timestamp, err := strconv.ParseInt(subEntry.Name(), 10, 64)
				if err != nil {
					continue
				}

				if timestamp < cutoffTime {
					// Remove the history file and its meta file
					if err := os.Remove(filepath.Join(subdirPath, subEntry.Name())); err != nil && !os.IsNotExist(err) {
						return errorWrap(err, "removing history file")
					}
					if err := os.Remove(filepath.Join(subdirPath, subEntry.Name()+metaSuffix)); err != nil && !os.IsNotExist(err) {
						return errorWrap(err, "removing history meta file")
					}
				}
			}
		} else if !entry.IsDir() && !strings.HasSuffix(entry.Name(), metaSuffix) {
			timestamp, err := strconv.ParseInt(entry.Name(), 10, 64)
			if err != nil {
				continue
			}

			if timestamp < cutoffTime {
				// Remove the history file and its meta file
				if err := os.Remove(filepath.Join(historyDir, entry.Name())); err != nil && !os.IsNotExist(err) {
					return errorWrap(err, "removing history file")
				}
				if err := os.Remove(filepath.Join(historyDir, entry.Name()+metaSuffix)); err != nil && !os.IsNotExist(err) {
					return errorWrap(err, "removing history meta file")
				}
			}
		}
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
	// Add histories from default directory
	entries, err := os.ReadDir(historyDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errorWrap(err, "reading history directory")
	}

	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), pagePrefix) {
			// Add histories from subdirectory
			subdirPath := filepath.Join(historyDir, entry.Name())
			subEntries, err := os.ReadDir(subdirPath)
			if err != nil {
				continue
			}

			for _, subEntry := range subEntries {
				if !subEntry.IsDir() && !strings.HasSuffix(subEntry.Name(), metaSuffix) {
					allHistories = append(allHistories, Version{
						Name: entry.Name()+"/"+subEntry.Name(),
						Version: subEntry.Name(),
					})
				}
			}
		} else if !entry.IsDir() && !strings.HasSuffix(entry.Name(), metaSuffix) {
			allHistories = append(allHistories, Version{
						Name: entry.Name()+"/"+entry.Name(),
						Version: entry.Name(),
					})
		}
	}

	// Sort by timestamp (oldest first)
	sort.Slice(allHistories, func(i, j int) bool {
		return allHistories[i].Version < allHistories[i].Version
	})

	// Determine which histories to keep
	if len(allHistories) <= maxCount {
		return nil
	}
	toRemove := allHistories[:len(allHistories)-maxCount]

	// Delete histories that should be removed
	for _, history := range toRemove {
		historyFile := filepath.Join(historyDir, history.Name)
		if err := os.Remove(historyFile); err != nil && !os.IsNotExist(err) {
				return errorWrap(err, "removing history file '"+historyFile+"'")
		}
		if err := os.Remove(historyFile + metaSuffix); err != nil && !os.IsNotExist(err) {
				return errorWrap(err, "removing meta file")
		}
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
		return errorWrap(err, "reading history path")
	}
	for _, entry := range entries {
		if entry.IsDir() {
			// Skip subdirectories for now, we'll process them separately
			continue
		}
		allHistories = append(allHistories, entry.Name())
	}
	// Sort by timestamp
	sort.Slice(allHistories, func(i, j int) bool {
		return allHistories[i] < allHistories[i]
	})


	// 保留最新的一个在默认目录
	allHistoriesForOrganizing := allHistories[:len(allHistories)-1]

	if len(allHistoriesForOrganizing) <= maxHistoryCount {
		return nil // 数量未超过限制，无需处理
	}


	// 按 maxHistoryCount 分组
	for i := 0; (i+maxHistoryCount) <= len(allHistoriesForOrganizing); i += maxHistoryCount {
		end := i + maxHistoryCount

		pageHistories := allHistoriesForOrganizing[i:end]

		// 使用该页最早的 timestamp 作为子目录名
		minTime, err := strconv.ParseInt(pageHistories[0], 10, 64)
		if err != nil {
			return errorWrap(err, "invalid version file '"+pageHistories[0]+"'")
		}
		pageDirName := pagePrefix + strconv.FormatInt(minTime, 10)
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

			// 同时移动对应的 .meta 文件（如果存在）
			oldMetaPath := oldPath + metaSuffix
			newMetaPath := newPath + metaSuffix
			if _, statErr := os.Stat(oldMetaPath); statErr == nil {
				if err := os.Rename(oldMetaPath, newMetaPath); err != nil {
					return errorWrap(err, "moving history meta file from "+oldMetaPath+" to "+newMetaPath)
				}
			}
		}
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
			return errorWrap(err, "accessing path "+pa)
		}
		if !d.IsDir() {
			return nil // Skip files
		}

		relPath, err := filepath.Rel(historyRoot, pa)
		if err != nil {
			return errorWrap(err, "getting relative path for "+pa)
		}
		if relPath == "." {
			return nil // Skip the root history directory itself
		}

		// Check if this directory name ends with .h suffix
		if !strings.HasSuffix(d.Name(), historyDirSuffix) {
			return nil
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
	// 1. 检查历史目录是否存在
	_, statErr := os.Stat(historyDir)
	if statErr != nil {
		if !os.IsNotExist(statErr) {
			if f.ignoreWarning {
				*errList = append(*errList, errorWrap(statErr, "accessing history directory for key '"+key+"'"))
				return false, nil
			} else {
				return false, errorWrap(statErr, "accessing history directory for key '"+key+"'")
			}
		}
		return false, nil
	}

	entries, readDirErr := os.ReadDir(historyDir)
	if readDirErr != nil {
		if f.ignoreWarning {
			*errList = append(*errList, errorWrap(readDirErr, "reading history directory for key '"+key+"'"))
			return false, nil
		} else {
			return false, errorWrap(readDirErr, "reading history directory for key '"+key+"'")
		}
	}

	for _, entry := range entries {
		if entry.IsDir() {
			if !strings.HasPrefix(entry.Name(), pagePrefix) {
				continue
			}
			subDirPath := filepath.Join(historyDir, entry.Name())
			subEntries, subReadDirErr := os.ReadDir(subDirPath)
			if subReadDirErr != nil {
				err := errorWrap(subReadDirErr, "reading history subdirectory for key '"+key+"'")
				if f.ignoreWarning {
					*errList = append(*errList, err)
					continue
				}
				return false, err
			}
			for _, subEntry := range subEntries {
				if !subEntry.IsDir() && !strings.HasSuffix(subEntry.Name(), metaSuffix) {
					return true, nil
				}
			}
			continue
		}
		if !strings.HasSuffix(entry.Name(), metaSuffix) {
			return true, nil
		}
	}	
	return false, nil
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

		hasHistory, fatalErr := f.hasHistories(key, historyDir, &errList)
		if fatalErr != nil {
			return fatalErr
		}
		if !hasHistory {
			timestamp := time.Now().Unix()
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
		// 使用 errors.Join (Go 1.20+) 来聚合多个错误
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
	version, err := c.store.Set(ctx, key, value)
	if err != nil {
		return "", err
	}

	// Update cache if successful
	c.cache[key] = value
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

func (c *CachedFileKVStore) CleanupHistoriesByTime(ctx context.Context, key string, maxAge time.Duration) error {
	return c.store.CleanupHistoriesByTime(ctx, key, maxAge)
}

func (c *CachedFileKVStore) CleanupHistoriesByCount(ctx context.Context, key string, maxCount int) error {
	return c.store.CleanupHistoriesByCount(ctx, key, maxCount)
}

func (c *CachedFileKVStore) Fsck(ctx context.Context) error {
	return c.store.Fsck(ctx)
}

// Git import functionality
type GitImportResult struct {
	ImportedFiles int
	Errors        []error
}

// ImportGitRepo imports a git repository into the KV system
func ImportGitRepo(ctx context.Context, store KeyValueStore, repoPath, targetPrefix string) (*GitImportResult, error) {
	// result := &GitImportResult{}

	// This would require importing go-git library which is not available here
	// For demonstration purposes, we'll just return an error indicating the implementation
	return nil, errors.New("git import functionality requires external dependency 'github.com/go-git/go-git/v5' which is not included in this implementation")
}

func main() {
	// Example usage
	store := NewFileKVStore("./data")

	ctx := context.Background()

	// Set a value
	version, err := store.Set(ctx, "test/key", []byte("hello world"))
	if err != nil {
		println("Error setting value: " + err.Error())
		return
	}
	println("Set value with version: " + version)

	// Get the value
	value, err := store.Get(ctx, "test/key")
	if err != nil {
		println("Error getting value: " + err.Error())
		return
	}
	println("Got value: " + string(value))

	// Get by version
	valueByVersion, err := store.GetByVersion(ctx, "test/key", version)
	if err != nil {
		println("Error getting value by version: " + err.Error())
		return
	}
	println("Got value by version: " + string(valueByVersion))

	// Get histories
	histories, err := store.GetHistories(ctx, "test/key")
	if err != nil {
		println("Error getting histories: " + err.Error())
		return
	}
	println("Number of histories: " + strconv.Itoa(len(histories)))

	// Get last version
	lastVersion, err := store.GetLastVersion(ctx, "test/key")
	if err != nil {
		println("Error getting last version: " + err.Error())
		return
	}
	println("Last version: " + lastVersion.Name)

	// Test cached store
	cachedStore := NewCachedFileKVStore(store)
	valueFromCache, err := cachedStore.Get(ctx, "test/key")
	if err != nil {
		println("Error getting value from cache: " + err.Error())
		return
	}
	println("Got value from cache: " + string(valueFromCache))

	// Run fsck
	err = store.Fsck(ctx)
	if err != nil {
		println("Error running fsck: " + err.Error())
		return
	}
	println("Fsck completed successfully")
}
