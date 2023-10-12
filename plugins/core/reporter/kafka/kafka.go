// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package kafka

import (
	"context"
	"strings"
	"time"

	agentv3 "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
	logv3 "skywalking.apache.org/repo/goapi/collect/logging/v3"

	"github.com/apache/skywalking-go/plugins/core/operator"
	"github.com/apache/skywalking-go/plugins/core/reporter"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// NewkafkaReporter create a new reporter to send data to kafka oap server. Only one backend address is allowed.
func NewkafkaReporter(logger operator.LogOperator, brokers string, opts ...KReporterOption) (reporter.Reporter, error) {
	r := &kafkaReporter{
		logger:           logger,
		connectionStatus: reporter.ConnectionStatusConnected,
	}
	for _, o := range opts {
		o(r)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	kopts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.DefaultProduceTopic(r.topic),
	}

	kopts = append(kopts, kgo.AllowAutoTopicCreation())
	conn, err := kgo.NewClient(kopts...)
	if err != nil {
		return r, err
	}
	if err = conn.Ping(r.ctx); err != nil {
		return r, err
	}
	r.conn = conn

	return r, nil
}

type kafkaReporter struct {
	entity *reporter.Entity
	logger operator.LogOperator

	topic  string
	conn   *kgo.Client
	ctx    context.Context
	cancel context.CancelFunc

	// bootFlag is set if Boot be executed
	bootFlag         bool
	connectionStatus reporter.ConnectionStatus
}

func (r *kafkaReporter) Boot(entity *reporter.Entity, cdsWatchers []reporter.AgentConfigChangeWatcher) {
	r.entity = entity
	r.bootFlag = true
}

func (r *kafkaReporter) ConnectionStatus() reporter.ConnectionStatus {
	return r.connectionStatus
}

func (r *kafkaReporter) SendTracing(spans []reporter.ReportedSpan) {
	spanSize := len(spans)
	if spanSize < 1 {
		return
	}
	rootSpan := spans[spanSize-1]
	rootCtx := rootSpan.Context()
	segmentObject := &agentv3.SegmentObject{
		TraceId:         rootCtx.GetTraceID(),
		TraceSegmentId:  rootCtx.GetSegmentID(),
		Spans:           make([]*agentv3.SpanObject, spanSize),
		Service:         r.entity.ServiceName,
		ServiceInstance: r.entity.ServiceInstanceName,
	}
	for i, s := range spans {
		spanCtx := s.Context()
		segmentObject.Spans[i] = &agentv3.SpanObject{
			SpanId:        spanCtx.GetSpanID(),
			ParentSpanId:  spanCtx.GetParentSpanID(),
			StartTime:     s.StartTime(),
			EndTime:       s.EndTime(),
			OperationName: s.OperationName(),
			Peer:          s.Peer(),
			SpanType:      s.SpanType(),
			SpanLayer:     s.SpanLayer(),
			ComponentId:   s.ComponentID(),
			IsError:       s.IsError(),
			Tags:          s.Tags(),
			Logs:          s.Logs(),
		}
		srr := make([]*agentv3.SegmentReference, 0)
		if i == (spanSize-1) && spanCtx.GetParentSpanID() > -1 {
			srr = append(srr, &agentv3.SegmentReference{
				RefType:               agentv3.RefType_CrossThread,
				TraceId:               spanCtx.GetTraceID(),
				ParentTraceSegmentId:  spanCtx.GetParentSegmentID(),
				ParentSpanId:          spanCtx.GetParentSpanID(),
				ParentService:         r.entity.ServiceName,
				ParentServiceInstance: r.entity.ServiceInstanceName,
			})
		}
		if len(s.Refs()) > 0 {
			for _, tc := range s.Refs() {
				srr = append(srr, &agentv3.SegmentReference{
					RefType:                  agentv3.RefType_CrossProcess,
					TraceId:                  spanCtx.GetTraceID(),
					ParentTraceSegmentId:     tc.GetParentSegmentID(),
					ParentSpanId:             tc.GetParentSpanID(),
					ParentService:            tc.GetParentService(),
					ParentServiceInstance:    tc.GetParentServiceInstance(),
					ParentEndpoint:           tc.GetParentEndpoint(),
					NetworkAddressUsedAtPeer: tc.GetAddressUsedAtClient(),
				})
			}
		}
		segmentObject.Spans[i].Refs = srr
	}
	defer func() {
		// recover the panic caused by close tracingSendCh
		if err := recover(); err != nil {
			r.logger.Errorf("reporter segment err %v", err)
		}
	}()
	r.produce(segmentObject)
}

func (r *kafkaReporter) SendMetrics(metrics []reporter.ReportedMeter) {
	if len(metrics) == 0 {
		return
	}
	meters := make([]*agentv3.MeterData, len(metrics))
	for i, m := range metrics {
		meter := &agentv3.MeterData{}
		switch data := m.(type) {
		case reporter.ReportedMeterSingleValue:
			meter.Metric = &agentv3.MeterData_SingleValue{
				SingleValue: &agentv3.MeterSingleValue{
					Name:   data.Name(),
					Labels: r.convertLabels(data.Labels()),
					Value:  data.Value(),
				},
			}
		case reporter.ReportedMeterHistogram:
			buckets := make([]*agentv3.MeterBucketValue, len(data.BucketValues()))
			for i, b := range data.BucketValues() {
				buckets[i] = &agentv3.MeterBucketValue{
					Bucket:             b.Bucket(),
					Count:              b.Count(),
					IsNegativeInfinity: b.IsNegativeInfinity(),
				}
			}
			meter.Metric = &agentv3.MeterData_Histogram{
				Histogram: &agentv3.MeterHistogram{
					Name:   data.Name(),
					Labels: r.convertLabels(data.Labels()),
					Values: buckets,
				},
			}
		}
		meters[i] = meter
	}

	meters[0].Service = r.entity.ServiceName
	meters[0].ServiceInstance = r.entity.ServiceInstanceName
	meters[0].Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	defer func() {
		// recover the panic caused by close tracingSendCh
		if err := recover(); err != nil {
			r.logger.Errorf("reporter metrics err %v", err)
		}
	}()

	r.produce(meters[0])
}

func (r *kafkaReporter) SendLog(log *logv3.LogData) {
	defer func() {
		if err := recover(); err != nil {
			r.logger.Errorf("reporter log err %v", err)
		}
	}()
	r.produce(log)
}

func (r *kafkaReporter) convertLabels(labels map[string]string) []*agentv3.Label {
	if len(labels) == 0 {
		return nil
	}
	ls := make([]*agentv3.Label, 0)
	for k, v := range labels {
		ls = append(ls, &agentv3.Label{
			Name:  k,
			Value: v,
		})
	}
	return ls
}

func (r *kafkaReporter) Close() {
	r.conn.Close()
}

func (r *kafkaReporter) produce(m interface{}) {
	var (
		raw []byte
		err error
	)
	switch msg := m.(type) {
	case *logv3.LogData:
		raw, err = proto.Marshal(msg)
	case *agentv3.SegmentObject:
		raw, err = proto.Marshal(msg)
	case *agentv3.MeterData:
		raw, err = proto.Marshal(msg)
	}
	if err != nil {
		r.logger.Errorf("marshal error %v", err)
		return
	}
	record := &kgo.Record{Topic: r.topic, Value: raw}
	if err := r.conn.Ping(r.ctx); err != nil {
		r.logger.Warnf("kafka disconnect caused by: %v", err)
		return
	}
	if err := r.conn.ProduceSync(r.ctx, record).FirstErr(); err != nil {
		r.logger.Warnf("record had a produce error while synchronously producing: %v\n", err)
	}
}
