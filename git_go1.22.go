//go:build go1.22
// +build go1.22

package filekv

import (

	// "gopkg.in/src-d/go-git.v4"
	// "gopkg.in/src-d/go-git.v4/plumbing"
	// "gopkg.in/src-d/go-git.v4/plumbing/format/index"
	// "gopkg.in/src-d/go-git.v4/plumbing/object"
	// "gopkg.in/src-d/go-git.v4/plumbing/storer"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
)

var GitErrStop = storer.ErrStop
var GitErrFileNotFound = object.ErrFileNotFound
var GitErrEntryNotFound = index.ErrEntryNotFound
var GitErrRepositoryNotExists = git.ErrRepositoryNotExists

type GitRepository = git.Repository

var GitPlainOpen = git.PlainOpen
var GitPlainInit = git.PlainInit
var GitLogOrderCommitterTime = git.LogOrderCommitterTime

type GitRevision = plumbing.Revision

var GitUntracked = git.Untracked
var GitUnmodified = git.Unmodified

type GitCommitOptions = git.CommitOptions

type GitSignature = object.Signature
type GitCommit = object.Commit
type GitFile = object.File

var GitNewCommitFileIterFromIter = object.NewCommitFileIterFromIter

type GitLogOptions = git.LogOptions
