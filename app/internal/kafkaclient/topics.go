package kafkaclient

// requiredTopics lists all Kafka topics the demo needs.
// 6 input topics (produced by Go API) + 2 output topics (produced by Flink).
var requiredTopics = []string{
	// Input topics — produced by Go API from role actions
	"product.listed",
	"cart.item.added",
	"cart.checkout",
	"order.confirmed",
	"shipment.picked",
	"shipment.delivered",
	// Output topics — produced by Flink jobs
	"flink.window.stats",
	"flink.cep.alerts",
}

// RequiredTopics returns a copy of the required topic list.
func RequiredTopics() []string {
	result := make([]string, len(requiredTopics))
	copy(result, requiredTopics)
	return result
}
