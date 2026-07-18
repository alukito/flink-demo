package kafkaclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequiredTopicsContainsAllExpected(t *testing.T) {
	topics := RequiredTopics()

	// Input topics
	assert.Contains(t, topics, "product.listed")
	assert.Contains(t, topics, "cart.item.added")
	assert.Contains(t, topics, "cart.checkout")
	assert.Contains(t, topics, "order.confirmed")
	assert.Contains(t, topics, "shipment.picked")
	assert.Contains(t, topics, "shipment.delivered")

	// Output topics
	assert.Contains(t, topics, "flink.window.stats")
	assert.Contains(t, topics, "flink.cep.alerts")

	assert.Len(t, topics, 8)
}
