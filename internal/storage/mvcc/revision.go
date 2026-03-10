package mvcc

type Revision struct {
	Main int64  // 主版本号
	Sub int64   // 字序号
}

func (r *Revision) Less(o *Revision) bool {
	if r.Main != o.Main {
		return r.Main < o.Main
	}
	return r.Sub < o.Sub
}