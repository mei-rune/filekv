package filekv

import (
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

// ImportGitRepo imports a git repository into the KV system, including file history
func ImportGitRepo(ctx context.Context, store KeyValueStore, gitdir string, filter func(ctx context.Context, file string, timestamp time.Time) bool) (*GitImportResult, error) {
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

	// Get the commit iterator
	commitIter, err := r.Log(&GitLogOptions{
		From: reference.Hash(),
	})
	if err != nil {
		return nil, err
	}

	// Collect all commits in forward order (newest to oldest)
	var commits []*GitCommit
	err = commitIter.ForEach(func(c *GitCommit) error {
		commits = append(commits, c) // Append to forward order
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Reverse the commits to get oldest to newest
	for i, j := 0, len(commits)-1; i < j; i, j = i+1, j-1 {
		commits[i], commits[j] = commits[j], commits[i]
	}

	// Iterate through all commits from oldest to newest
	for _, c := range commits {
		// Get the tree from the commit
		tree, err := c.Tree()
		if err != nil {
			result.Errors = append(result.Errors, err)
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
				result.Errors = append(result.Errors, err)
				return nil
			}

			// Import into KV store
			// Each SetWithTimestamp call creates a new version with git commit time as timestamp
			kvVersion, err := store.SetWithTimestamp(ctx, filePath, []byte(content), c.Committer.When.UnixNano())
			if err != nil {
				result.Errors = append(result.Errors, err)
				return nil
			}

			// Record the imported file with its versions
			importedFile := ImportedFile{
				GitCommitVersion: c.Hash.String(),
				Version:          kvVersion,
			}

			// Add to the result map
			result.ImportedFiles[filePath] = append(result.ImportedFiles[filePath], importedFile)

			return nil
		})
		if err != nil {
			result.Errors = append(result.Errors, err)
		}
	}

	return result, nil
}
