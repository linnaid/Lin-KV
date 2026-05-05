package tests

import "testing"

func TestLeaseCoveragePlaceholder(t *testing.T) {
	t.Skip("lease manager is not exported; concrete lease behavior is covered by package-local tests in internal/storage/mvcc")
}
