package memorykv

type Item struct {
	key      string
	value    []byte
	timeout  string
	callback *cb
}

func (i *Item) Key() string {
	return i.key
}

func (i *Item) Value() []byte {
	return i.value
}

func (i *Item) Timeout() string {
	return i.timeout
}
