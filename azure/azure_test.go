package azure_test

import (
	"testing"

	dstore "github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
)

func TestAzuireDatastore(t *testing.T) {
	ds := dstore.NewMapDatastore()
	dstest.SubtestAll(t, ds)
}
