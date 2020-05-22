module github.com/rstorlabs/queuelib/experiments

go 1.13

require (
	github.com/jessevdk/go-flags v1.4.0
	github.com/rstorlabs/queue-lib v0.0.0-20200119233628-307026ed798b
	github.com/segmentio/kafka-go v0.3.6
)

replace github.com/rstorlabs/queue-lib v0.0.0-20200119233628-307026ed798b => ../../queue-lib
