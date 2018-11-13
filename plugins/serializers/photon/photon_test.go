package photon

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/stretchr/testify/require"
)

func MustMetric(v telegraf.Metric, err error) telegraf.Metric {
	if err != nil {
		panic(err)
	}
	return v
}

type PhotonReadResult struct {
	SenderId  string
	timestamp time.Time
	metrics   map[string][]photonMetricSample
}

var MetricTime time.Time = time.Unix(0, 0).UTC()

var tests = []struct {
	name      string
	input     telegraf.Metric
	errReason string
}{
	{
		name: "minimal",
		input: MustMetric(
			metric.New(
				"cpu",
				map[string]string{},
				map[string]interface{}{
					"value": 42.0,
				},
				MetricTime,
			),
		),
	},
	{
		name: "arbitrary name field",
		input: MustMetric(
			metric.New(
				"cpu",
				map[string]string{},
				map[string]interface{}{
					"x": 42.0,
				},
				MetricTime,
			),
		),
	},
	{
		name: "arbitrary name fieldS",

		input: MustMetric(
			metric.New(
				"cpu",
				map[string]string{},
				map[string]interface{}{
					"x": 42.0,
					"y": 42.0,
				},
				MetricTime,
			),
		),
		errReason: NoFields,
	},
	{
		name: "float NaN",
		input: MustMetric(
			metric.New(
				"cpu",
				map[string]string{},
				map[string]interface{}{
					"x": math.NaN(),
				},
				time.Unix(0, 0),
			),
		),
		errReason: NoFields,
	},
	{
		name: "float NaN only",
		input: MustMetric(
			metric.New(
				"cpu",
				map[string]string{},
				map[string]interface{}{
					"value": math.NaN(),
				},
				time.Unix(0, 0),
			),
		),
		errReason: NoFields,
	},
	{
		name: "float Inf",
		input: MustMetric(
			metric.New(
				"cpu",
				map[string]string{},
				map[string]interface{}{
					"value": math.Inf(1),
				},
				time.Unix(0, 0),
			),
		),
		errReason: NoFields,
	},
	{
		name: "integer field",
		input: MustMetric(
			metric.New(
				"cpu",
				map[string]string{},
				map[string]interface{}{
					"value": int32(42),
				},
				time.Unix(0, 0).UTC(),
			),
		),
	},
	// {
	// 	name: "integer field 64-bit",
	// 	input: MustMetric(
	// 		metric.New(
	// 			"cpu",
	// 			map[string]string{},
	// 			map[string]interface{}{
	// 				"value": int64(123456789012345),
	// 			},
	// 			MetricTime,
	// 		),
	// 	),
	// },
	{
		name: "no fields",
		input: MustMetric(
			metric.New(
				"cpu",
				map[string]string{},
				map[string]interface{}{},
				time.Unix(0, 0),
			),
		),
		errReason: NoFields,
	},
}

func TestSerializer(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serializer := NewSerializer("TestSerializerId")
			output, err := serializer.Serialize(tt.input)
			if tt.errReason != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errReason)
				return
			}

			result := ProcessBinary(t, output)

			m := tt.input

			resultM := result.metrics[m.Name()]

			require.Equal(t, m.Time(), resultM[0].time)
			require.EqualValues(t, m.FieldList()[0].Value, resultM[0].value)

		})
	}
}

func BenchmarkSerializer(b *testing.B) {
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			serializer := NewSerializer("TestSerializerId")
			for n := 0; n < b.N; n++ {
				output, err := serializer.Serialize(tt.input)
				_ = err
				_ = output
			}
		})
	}
}

func TestSerialize_SerializeBatch(t *testing.T) {
	m := MustMetric(
		metric.New(
			"cpu",
			map[string]string{},
			map[string]interface{}{
				"value": 42.0,
			},
			MetricTime,
		),
	)

	metrics := []telegraf.Metric{m, m}

	serializer := NewSerializer("TestSerializerId")
	output, err := serializer.SerializeBatch(metrics)
	require.NoError(t, err)

	result := ProcessBinary(t, output)

	m = metrics[0]
	resultM := result.metrics["cpu"]

	require.Equal(t, m.Time(), resultM[0].time)
	require.EqualValues(t, m.FieldList()[0].Value, resultM[0].value)

	require.Equal(t, m.Time(), resultM[1].time)
	require.EqualValues(t, m.FieldList()[0].Value, resultM[1].value)
}

func ProcessBinary(t *testing.T, data []byte) PhotonReadResult {

	var result PhotonReadResult

	reader := bytes.NewReader(data)

	// parsing utility functions
	dotnetTimeToUnix := func(dotnetTime uint64) int64 {
		// http://stackoverflow.com/questions/15919598/serialize-datetime-as-binary
		// http://ben.lobaugh.net/blog/749/converting-datetime-ticks-to-a-unix-timestamp-and-back-in-php
		// http://www.dotnetframework.org/default.aspx/DotNET/DotNET/8@0/untmp/whidbey/REDBITS/ndp/clr/src/BCL/System/DateTime@cs/1/DateTime@cs
		//private const UInt64 TicksMask             = 0x3FFFFFFFFFFFFFFF;
		//private const UInt64 FlagsMask             = 0xC000000000000000;
		//private const UInt64 LocalMask             = 0x8000000000000000;
		//private const Int64 TicksCeiling           = 0x4000000000000000;
		//private const UInt64 KindUnspecified       = 0x0000000000000000;
		//private const UInt64 KindUtc               = 0x4000000000000000;
		//private const UInt64 KindLocal             = 0x8000000000000000;
		//private const UInt64 KindLocalAmbiguousDst = 0xC000000000000000;
		//private const Int32 KindShift = 62;

		ticks := dotnetTime & 0x3FFFFFFFFFFFFFFF
		return int64((ticks - 621355968000000000) / 10000000)
	}

	readString7BitEncodingLen := func() string {
		//http://stackoverflow.com/questions/1550560/encoding-an-integer-in-7-bit-format-of-c-sharp-binaryreader-readstring
		length := 0
		i := 0
		for ; i < 4; i++ {
			var b byte
			b, err := reader.ReadByte()
			if err != nil {
				panic(err)
			}
			length = length + int(b&0x7F)
			if b > 127 {
				length = length << 7
			} else {
				break
			}
		}
		var buf []byte
		if i == 4 {
			panic("readString7BitEncodingLen: too many bytes for len")
		} else {
			buf = make([]byte, length)
			lRead, err := io.ReadFull(reader, buf)
			if lRead != length || err != nil {
				panic(fmt.Sprintf("readString7BitEncodingLen: read underrun: read %v < want %v, con-len %v, err %v",
					lRead, length, string(buf), err))
			}
		}
		return string(buf)
	}

	read := func(data interface{}) {
		err := binary.Read(reader, binary.LittleEndian, data)
		if err != nil {
			panic(err)
		}
	}

	defer func() {
		// parse errors reported via panic (they are generated inside utility functions)
		err := recover()
		if err != nil {
			log.Printf("E! [photon_bin.test] err: %v", err)
		}
		log.Println("I! [photon_bin.test] reading completed")
	}()

	var magic uint16 // 0xffee
	read(&magic)
	if magic != 0xffee {
		log.Println("E! [photon_bin.test] Bad magic")
	} else {
		var dotnetServerTime uint64
		read(&dotnetServerTime)
		result.timestamp = time.Unix(dotnetTimeToUnix(dotnetServerTime), 0)
		var count int32
		read(&count)
		result.SenderId = readString7BitEncodingLen()
		result.metrics = make(map[string][]photonMetricSample)

		//var err error
		for i := int32(0); i < count; i++ {

			CounterName := readString7BitEncodingLen()
			var valueCount int16
			read(&valueCount)

			require.NotEqual(t, int16(0), valueCount)

			samples := make([]photonMetricSample, 0, 1)

			for i := 0; i < int(valueCount); i++ {

				var dotnetTimestamp uint64
				read(&dotnetTimestamp)
				var value float32
				read(&value)
				samples = append(samples, photonMetricSample{time.Unix(dotnetTimeToUnix(dotnetTimestamp), 0).UTC(), value})
			}

			result.metrics[CounterName] = samples
		}
	}
	return result
}
