package memorypubsub

import (
	"context"
	"sync"

	"github.com/roadrunner-server/api/v2/plugins/pubsub"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v2/bst"
	"go.uber.org/zap"
)

type Driver struct {
	sync.RWMutex
	// channel with the messages from the RPC
	pushCh chan *pubsub.Message
	// user-subscribed topics
	storage bst.Storage
	log     *zap.Logger
}

func NewPubSubDriver(log *zap.Logger, _ string) (*Driver, error) {
	ps := &Driver{
		pushCh:  make(chan *pubsub.Message, 100),
		storage: bst.NewBST(),
		log:     log,
	}
	return ps, nil
}

func (d *Driver) Publish(msg *pubsub.Message) error {
	d.pushCh <- msg
	return nil
}

func (d *Driver) PublishAsync(msg *pubsub.Message) {
	go func() {
		d.pushCh <- msg
	}()
}

func (d *Driver) Subscribe(connectionID string, topics ...string) error {
	d.Lock()
	defer d.Unlock()
	for i := 0; i < len(topics); i++ {
		d.storage.Insert(connectionID, topics[i])
	}
	return nil
}

func (d *Driver) Unsubscribe(connectionID string, topics ...string) error {
	d.Lock()
	defer d.Unlock()
	for i := 0; i < len(topics); i++ {
		d.storage.Remove(connectionID, topics[i])
	}
	return nil
}

func (d *Driver) Connections(topic string, res map[string]struct{}) {
	d.RLock()
	defer d.RUnlock()

	ret := d.storage.Get(topic)
	for rr := range ret {
		res[rr] = struct{}{}
	}
}

func (d *Driver) Stop() {
	// no-op for the in-memory
}

func (d *Driver) Next(ctx context.Context) (*pubsub.Message, error) {
	const op = errors.Op("pubsub_memory")
	select {
	case msg := <-d.pushCh:
		if msg == nil {
			return nil, nil
		}

		d.RLock()
		defer d.RUnlock()
		// push only messages, which topics are subscibed
		// TODO(rustatian) better???
		// if we have active subscribers - send a message to a topic
		// or send nil instead
		if ok := d.storage.Contains(msg.Topic); ok {
			return msg, nil
		}
	case <-ctx.Done():
		return nil, errors.E(op, errors.TimeOut, ctx.Err())
	}

	return nil, nil
}
