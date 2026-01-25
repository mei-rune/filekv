
我用 qwen 生成了本代码，后续用 trae ide 作了小部份修改


用 golang 开发一个 kv 库，它有以下特点：
1. 它是基于文件系统的，文件名是 key， 文件内容是 value，注意文件名要和kek完全相等（windows下时\分隔符会转成/，这个是隐形规则代码中不用体现，因go的Io支持在windows下用/作为分隔符）
2. 它有历史记录功能，每次更新都要用当前修改时间的 unixtime 为历史记录文件名保存当前值
3. 像 git 仓库有一个 .git 目录一样，我们添加一个 .history 目录来保存历史，我们在以 key + ".h" 为名的目录下保存关于这个 key 相关的历史记录, 注意当这个 key 的历史记录太多时会以“p_”开头的子目录一分组（见下面的8.2）
4. 我们可以为指定的历史记录添加一些元数据，元数据文件名为历史记录文件名加上一个 “.meta”， 元数据以 key=value 格式保存，可以添加 readProperties 和 writeProperties 方法来实现读写（以最简单方式实现不考虑转义或注释等）。
5. key 像文件系统一样有多级，用 / 分隔，key 不可以用 “.” 或 “p_” 开头（每一级都不能）, 也不能以 ".h" 结尾（每一级都不能）， 整个 key 不能以 / 开头，不能包含 "\" (防止岐义),
6. 因为它是基于文件系统的，所以不需要锁，不用引入sync.Mutex
7. 在基本实现中不要引入 cache ，可以它的基础上用装饰模式实现一个 CachedFileKVStore，注意cache需要加锁保证多线程安全
8.增加一个Fsck函数，来修复下列情况
8.1 当一个key的历史记录数超过200时我们建立子目录，按时间排序后每200个文件一个目录，这个子目录名以“p_”开头加上子目录中时间最小的文件名作为目录名，注意元数据文件要一块移动。因为查找最后一次历史记录比较频繁，最后一次历史记录不移入子目录
8.2 删除key己经不存在的历史记录（为了防止误删除，可以看一下第5点中历史记录目录是以 “.h” 结尾的）
8.3 当key缺少对应的历史记录时，自动建一个

这个库将实现下列接口

type Version struct {
  Name string
  Meta map[string]string
}

type KeyValueStore interface {
  Get(ctx context.Context, key string) ([]byte, error)

  // 当 version 为 head 时表示查询最后一次，将调用 Get() 方法
  // 考虑到历史记录目录下可能有子目录，先在默认目录下找一下，找不到时再尝试在子目录下查找一下
  // 参考第8.1点子目录的名称中有时间，可以跳过一些目录，如
  // 有 p_11111110, p_11111120, p_11111130 时，当我们查询 version 为 11111123 时可以在 p_11111120 下查找（不用在 p_11111130 下查找）， 当我们查询 version 为 11111133 时可以在 p_11111130 下查找， 当我们查询 version 为 11111100 时可以不用在任何子目录下查找。 
  GetByVersion(ctx context.Context, key string, version string) ([]byte, error)
  // 当 value 和上次相等时，不保存，不产生历史记录，返回值中 version 返回空串
  Set(ctx context.Context, key string, value []byte) (version string, err error)

  // 当 version 为 head 时表示查询最后一次历史记录，同时注意下面两点
  // 1. 当 key 存在时，在写 meta 之前要查询最后一次历史记录，当历史记录为空时要
  // 2. 以 key 的创建时间为 time 来建一个历史记录
  // 元模型读写用  readProperties 和 writeProperties
  SetMeta(ctx context.Context, key, version string, meta map[string]string)  error
  // 同上，但这个是部分更新
  UpdateMeta(ctx context.Context, key, version string, meta map[string]string)  error
  // 注意 key 是多层的，当有一个 a/b/c 时，删除 a 时要返回失败
  // 当 removeHistories 为 true 时，要先删除历史记录，再删除 key
  Delete(ctx context.Context, key string, removeHistories bool) error
  // 注意 key 是多层的，当有一个 a/b/c 时，检测 a/b 时要返回不存在
  Exists(ctx context.Context, key string) (bool, error)
  // 要跳过 .history 等特殊目录
  ListKeys(ctx context.Context, prefix string) ([]string, error)
  // 考虑到历史记录目录下可能有子目录，不要忘了扫描子目录
  GetHistories(ctx context.Context, key string) ([]Version, error)

  // 因为我们在第 8.1 点中说了最后一个历史记录在默认目录下，如果默认目录下
  // 没有历史记录时我们也尝试扫描子目录
  GetLastVersion(ctx context.Context, key string) (*Version, error)
  // 考虑到历史记录目录下可能有子目录，不要忘了扫描子目录
  CleanupHistoriesByTime(ctx context.Context, key string, maxAge time.Duration) error
  // 考虑到历史记录目录下可能有子目录，不要忘了扫描子目录
  CleanupHistoriesByCount(ctx context.Context, key string, maxCount int) error
}
另外实现一个将 git 仓库导入这个 kv 系统中的函数，git 可以用 github.com/go-git/go-git/v5 库， 注意 git 的文件历史也要导入
ImportGitRepo(ctx context.Context, repoPath, targetPrefix string) (*GitImportResult, error) 


代码规范
1. 数字转换时尽可能用strconv
2. 添加一个errorWrap(err error, msg string) error 函数替换fmt.Errorf()
   type wrapErr struct {
     err error
     msg string
   }

3. 每当我们调用 os.WriteFile() 时不要先调用 os.MkdirAll(), 可以在失败后用 os.IsNotExist() 判断后调用 os.MkdirAll()
4. 两个 []byte 比较时要用 bytes.Equal()
5. 尽量少用 else，出错时可以先返回的话先返回。
