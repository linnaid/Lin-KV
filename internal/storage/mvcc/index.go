// 放 key index / revision 查找的公共 helper
package mvcc

import "sort"

// 找到第一个 >= target 的 key 位置
func orderedKeyRangeStart(keys []string, target string) int {
	return sort.Search(len(keys), func(i int) bool {
		return keys[i] >= target
	})
}

// 找到 [startKey, endKey) 的右边界位置
func orderedKeyRangeEnd(keys []string, start int, endKey string) int {
	if endKey == "" {
		return len(keys)
	}

	return start + sort.Search(len(keys)-start, func(i int) bool {
		return keys[start+i] >= endKey
	})
}


// 找到 rev 时刻对外可见的最新版本
func latestVisibleRevision(versions []ValueRevision, rev int64) (ValueRevision, bool) {
	left := 0
	right := len(versions) - 1
	pos := -1

	for left <= right {
		mid := (left + right) / 2
		if versions[mid].Rev.Main <= rev {
			pos = mid
			left = mid + 1
			continue
		}

		right = mid - 1
	}

	if pos == -1 {
		return ValueRevision{}, false
	}
	if versions[pos].Deleted {
		return ValueRevision{}, false
	}

	return versions[pos], true
}

// 找到第一条 main revision > rev 的位置
func firstRevisionAfter(versions []ValueRevision, rev int64) int {
	idx := sort.Search(len(versions), func(i int) bool {
		return versions[i].Rev.Main > rev
	})

	if idx == len(versions) {
		return -1
	}

	return idx
}