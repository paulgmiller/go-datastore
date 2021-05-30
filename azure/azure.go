// Package fs is a simple Datastore implementation that stores keys
// as directories and files, mirroring the key. That is, the key
// "/foo/bar" is stored as file "PATH/foo/bar/.dsobject".
//
// This means key some segments will not work. For example, the
// following keys will result in unwanted behavior:
//
//     - "/foo/./bar"
//     - "/foo/../bar"
//     - "/foo\x00bar"
//
// Keys that only differ in case may be confused with each other on
// case insensitive file systems, for example in OS X.
//
// This package is intended for exploratory use, where the user would
// examine the file system manually, and should only be used with
// human-friendly, trusted keys. You have been warned.
package azure

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

var ObjectKeySuffix = ".dsobject"

// Datastore uses a uses a file per key to store values.
type datastore struct {
	containerUrl azblob.ContainerURL
}

// NewDatastore returns a new fs Datastore at given `path`
func NewDatastore(accountName, accountKey, container string) (ds.Datastore, error) {
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/mycontainer", accountName))
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return err
	}
	return &datastore{containerUrl: azblob.NewContainerURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{}))}, nil
}

// KeyFilename returns the filename associated with `key`
func (d *datastore) keyUrl(key ds.Key) azblob.BlockBlobURL {
	return d.containerUrl.NewBlockBlobURL(key.String())
}

// Put stores the given value.
func (d *datastore) Put(key ds.Key, value []byte) (err error) {
	blob := d.keyUrl(key)
	ctx := context.TODO()
	//block if exists?
	put, err = blob.Upload(ctx, bytes.NewReader(value), azblob.BlobHTTPHeaders{}, azblob.Metadata{},
		azblob.BlobAccessConditions{}, azblob.AccessTierCool, nil, azblob.ClientProvidedKeyOptions{})
	// put into go routine an only block on sync
	// check _ respoonse.statuscode?
	fmt.Println(put.Status())
	return err
}

// Sync would ensure that any previous Puts done
// skipping for now
func (d *datastore) Sync(prefix ds.Key) error {
	return nil
}

// Get returns the value for given key
func (d *datastore) Get(key ds.Key) (value []byte, err error) {
	blob := d.keyUrl(key)
	ctx := context.TODO()
	//block if exists?
	_, err = blob.Download(ctx, bytes.NewReader(value), azblob.BlobHTTPHeaders{}, azblob.Metadata{},
		azblob.BlobAccessConditions{}, azblob.AccessTierCool, nil, azblob.ClientProvidedKeyOptions{})
}

// Has returns whether the datastore has a value for a given key
func (d *datastore) Has(key ds.Key) (exists bool, err error) {
	blob := d.keyUrl(key)
	ctx := context.TODO()
	//block if exists?
	get, err = blob.GetMetadata()
	ctx, 0, 0, BlobAccessConditions{}, false, ClientProvidedKeyOptions{})
b := bytes.Buffer{}
reader := get.Body(RetryReaderOptions{})
b.ReadFrom(reader)
reader.Clo
}

func (d *datastore) GetSize(key ds.Key) (size int, err error) {
	return ds.GetBackedSize(d, key)
}

// Delete removes the value for given key
func (d *datastore) Delete(key ds.Key) (err error) {
	blob := d.keyUrl(key)
	ctx := context.TODO()
	//block if exists?
	_, err = blob.Delete(ctx)
}

// Query implements Datastore.Query
func (d *datastore) Query(q query.Query) (query.Results, error) {
	results := make(chan query.Result)

	walkFn := func(path string, info os.FileInfo, _ error) error {
		// remove ds path prefix
		relPath, err := filepath.Rel(d.path, path)
		if err == nil {
			path = filepath.ToSlash(relPath)
		}

		if !info.IsDir() {
			path = strings.TrimSuffix(path, ObjectKeySuffix)
			var result query.Result
			key := ds.NewKey(path)
			result.Entry.Key = key.String()
			if !q.KeysOnly {
				result.Entry.Value, result.Error = d.Get(key)
			}
			results <- result
		}
		return nil
	}

	go func() {
		filepath.Walk(d.path, walkFn)
		close(results)
	}()
	r := query.ResultsWithChan(q, results)
	r = query.NaiveQueryApply(q, r)
	return r, nil
}

func (d *datastore) Close() error {
	return nil
}

func (d *datastore) Batch() (ds.Batch, error) {
	return ds.NewBasicBatch(d), nil
}

// DiskUsage returns the disk size used by the datastore in bytes.
func (d *datastore) DiskUsage() (uint64, error) {
	return 100, nil
}
