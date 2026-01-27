package filekv

import (
	"bytes"
	"context"
	"time"
)

// ImportedFile represents a file imported from git with its versions
type ImportedFile struct {
	GitCommitVersion string
	Version          string
}

// Git import functionality
type GitImportResult struct {
	ImportedFiles map[string][]ImportedFile
	Errors        []error
}

// ImportProgressCallback is a callback function for import progress updates
type ImportProgressCallback func(ctx context.Context, phase string, current int, total int, message string)

// ImportGitRepo imports a git repository into the KV system, including file history
func ImportGitRepo(ctx context.Context, store KeyValueStore, gitdir string, filter func(ctx context.Context, file string, timestamp time.Time) bool, progressCallback ...ImportProgressCallback) (*GitImportResult, error) {
	// Get progressCallback if provided
	var callback ImportProgressCallback
	if len(progressCallback) > 0 {
		callback = progressCallback[0]
	}
	result := &GitImportResult{
		ImportedFiles: make(map[string][]ImportedFile),
	}

	// Open the git repository
	r, err := GitPlainOpen(gitdir)
	if err != nil {
		return nil, err
	}

	// Get the HEAD reference
	reference, err := r.Head()
	if err != nil {
		// Handle empty repo case
		if err.Error() == "reference not found" {
			// No commits yet, return empty result
			return result, nil
		}
		return nil, err
	}

	// Set GitLogOptions
	logOptions := &GitLogOptions{
		From: reference.Hash(),
	}

	// Get the commit iterator
	commitIter, err := r.Log(logOptions)
	if err != nil {
		return nil, err
	}

	// Notify progress: starting to collect commits
	if callback != nil {
		callback(ctx, "collecting", 0, 0, "Starting to collect commits")
	}

	// Collect all commits in forward order (newest to oldest)
	var commits []*GitCommit
	err = commitIter.ForEach(func(c *GitCommit) error {
		commits = append(commits, c) // Append to forward order

		// Notify progress: finished collecting commits
		if callback != nil {
			callback(ctx, "collecting", len(commits), 0, "collecting commit - "+c.Committer.When.Format(time.RFC3339))
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Notify progress: finished collecting commits
	if callback != nil {
		callback(ctx, "collecting", 0, 0, "Finished collecting commits")
	}

	// Reverse the commits to get oldest to newest
	if callback != nil {
		callback(ctx, "sorting", 0, 0, "Sorting commits by time")
	}

	for i, j := 0, len(commits)-1; i < j; i, j = i+1, j-1 {
		commits[i], commits[j] = commits[j], commits[i]
	}

	if callback != nil {
		callback(ctx, "sorting", 0, 0, "Finished sorting commits")
	}

	// Map to track the last content of each file
	lastContent := make(map[string][]byte)

	// Iterate through all commits from oldest to newest
	if callback != nil {
		callback(ctx, "processing", 0, 0, "Starting to process commits")
	}

	for idx, c := range commits {

		// Iterate through all commits from oldest to newest
		if callback != nil {
			callback(ctx, "processing", idx, len(commits), "process commit - "+c.Committer.When.Format(time.RFC3339))
		}

		// Get the tree from the commit
		tree, err := c.Tree()
		if err != nil {
			result.Errors = append(result.Errors, errorWrap(err, "commit "+c.Committer.When.Format(time.RFC3339)))
			continue
		}

		// Iterate through all files in the tree
		err = tree.Files().ForEach(func(f *GitFile) error {
			// Get file path
			filePath := f.Name

			// Apply filter if provided
			if filter != nil && !filter(ctx, filePath, c.Committer.When) {
				return nil
			}

			// Read file content
			content, err := f.Contents()
			if err != nil {
				result.Errors = append(result.Errors, errorWrap(err, filePath))
				return nil
			}

			contentBytes := []byte(content)

			// Check if content has changed
			if lastBytes, ok := lastContent[filePath]; !ok || !bytes.Equal(lastBytes, contentBytes) {
				// Content has changed, create history record
				kvVersion, err := store.SetWithTimestamp(ctx, filePath, contentBytes, c.Committer.When.UnixNano())
				if err != nil {
					result.Errors = append(result.Errors, errorWrap(err, filePath))
					return nil
				}

				// Record the imported file with its versions
				importedFile := ImportedFile{
					GitCommitVersion: c.Hash.String(),
					Version:          kvVersion,
				}

				// Add to the result map
				result.ImportedFiles[filePath] = append(result.ImportedFiles[filePath], importedFile)

				// Update last content
				lastContent[filePath] = contentBytes
			}

			return nil
		})
		if err != nil {
			result.Errors = append(result.Errors, errorWrap(err, "commit "+c.Committer.When.Format(time.RFC3339)))
		}
	}

	// Notify progress: finished importing
	if callback != nil {
		callback(ctx, "finished", 0, 0, "Finished importing git repository")
	}

	return result, nil
}
