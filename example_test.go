package filekv

import (
	"context"
	"fmt"
	"os"
	"strconv"
)

// Example demonstrates the basic usage of FileKVStore
func Example() {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "filekv-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Example usage
	store := NewFileKVStore(tempDir)

	ctx := context.Background()

	// Set a value
	_, err := store.Set(ctx, "test/key", []byte("hello world"))
	if err != nil {
		fmt.Println("Error setting value: " + err.Error())
		return
	}
	fmt.Println("Set value with version: ")

	// Get the value
	value, err := store.Get(ctx, "test/key")
	if err != nil {
		fmt.Println("Error getting value: " + err.Error())
		return
	}
	fmt.Println("Got value: " + string(value))

	// Get histories
	histories, err := store.GetHistories(ctx, "test/key")
	if err != nil {
		fmt.Println("Error getting histories: " + err.Error())
		return
	}
	fmt.Println("Number of histories: " + strconv.Itoa(len(histories)))

	// Test cached store
	cachedStore := NewCachedFileKVStore(store)
	valueFromCache, err := cachedStore.Get(ctx, "test/key")
	if err != nil {
		fmt.Println("Error getting value from cache: " + err.Error())
		return
	}
	fmt.Println("Got value from cache: " + string(valueFromCache))

	// Run fsck
	err = store.Fsck(ctx)
	if err != nil {
		fmt.Println("Error running fsck: " + err.Error())
		return
	}
	fmt.Println("Fsck completed successfully")
}
