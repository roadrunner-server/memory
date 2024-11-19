package memorykv

import (
	"sync"
)

type hmap struct {
	mu    sync.RWMutex
	items map[string]*Item
}

func newHMap() *hmap {
	return &hmap{
		items: make(map[string]*Item, 10),
	}
}

func (h *hmap) Get(key string) (*Item, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	item, ok := h.items[key]
	return item, ok
}

func (h *hmap) Set(key string, item *Item) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.items[key] = item
}

func (h *hmap) LoadAndDelete(key string) (*Item, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	item, ok := h.items[key]
	if ok {
		if item.callback != nil {
			select {
			case item.callback.stopCh <- struct{}{}:
			default:
			}
		}
		delete(h.items, key)
	}

	return item, ok
}

func (h *hmap) Clean() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for k, v := range h.items {
		if v != nil {
			if v.callback != nil {
				select {
				case v.callback.stopCh <- struct{}{}:
				default:
				}
			}
		}

		delete(h.items, k)
	}

	h.items = make(map[string]*Item, 10)
}

func (h *hmap) Delete(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.items[key] != nil && h.items[key].callback != nil {
		select {
		case h.items[key].callback.stopCh <- struct{}{}:
		default:
		}
	}

	delete(h.items, key)
}

// IMPORTANT: Only use this method when the callback has already been cleaned up.
func (h *hmap) removeEntry(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.items, key)
}
