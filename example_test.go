package filekv

import (
	"context"
	"fmt"
	"os"
)

// Example demonstrates the basic usage of FileKVStore
func Example() {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "filekv-test")
	if err != nil {
		fmt.Println("mkdir temp dir failed: " + err.Error())
		return
	}
	defer os.RemoveAll(tempDir)

	// Example usage
	store := NewFileKVStore(tempDir)

	ctx := context.Background()

	// Set a value
	version1, err := store.Set(ctx, "test/key", []byte("hello world"))
	if err != nil {
		fmt.Println("Error setting value: " + err.Error())
		return
	}
	fmt.Println("Set value successfully")

	// Get the value
	value, err := store.Get(ctx, "test/key")
	if err != nil {
		fmt.Println("Error getting value: " + err.Error())
		return
	}
	fmt.Println("Got value: " + string(value))

	// Get histories count
	histories, err := store.GetHistories(ctx, "test/key")
	if err != nil {
		fmt.Println("Error getting histories: " + err.Error())
		return
	}
	fmt.Printf("Number of histories: %d\n", len(histories))

	// Set a new value
	version2, err := store.Set(ctx, "test/key", []byte("hello filekv"))
	if err != nil {
		fmt.Println("Error setting value: " + err.Error())
		return
	}
	fmt.Println("Set new value successfully")

	// Get the updated value
	value, err = store.Get(ctx, "test/key")
	if err != nil {
		fmt.Println("Error getting value: " + err.Error())
		return
	}
	fmt.Println("Got updated value: " + string(value))

	// Get histories count
	histories, err = store.GetHistories(ctx, "test/key")
	if err != nil {
		fmt.Println("Error getting histories: " + err.Error())
		return
	}
	fmt.Printf("Number of histories: %d\n", len(histories))

	// Get the old value by version
	oldValue, err := store.GetByVersion(ctx, "test/key", version1)
	if err != nil {
		fmt.Println("Error getting old value: " + err.Error())
		return
	}
	fmt.Println("Got old value by version: " + string(oldValue))

	// Get the new value by version
	newValue, err := store.GetByVersion(ctx, "test/key", version2)
	if err != nil {
		fmt.Println("Error getting new value: " + err.Error())
		return
	}
	fmt.Println("Got new value by version: " + string(newValue))

	// Output:
	// Set value successfully
	// Got value: hello world
	// Number of histories: 1
	// Set new value successfully
	// Got updated value: hello filekv
	// Number of histories: 2
	// Got old value by version: hello world
	// Got new value by version: hello filekv
}
