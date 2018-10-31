package photon

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"time"

	"github.com/influxdata/telegraf"
)

var (
	// NeedMoreSpace ...
	NeedMoreSpace = "need more space"
	// InvalidName ..
	InvalidName = "invalid name"
	// NoFields ...
	NoFields = "no serializable fields"
)

// MetricError is an error causing an entire metric to be unserializable.
type MetricError struct {
	series string
	reason string
}

func (e MetricError) Error() string {
	if e.series != "" {
		return fmt.Sprintf("%q: %s", e.series, e.reason)
	}
	return e.reason
}

// FieldError is an error causing a field to be unserializable.
type FieldError struct {
	reason string
}

func (e FieldError) Error() string {
	return e.reason
}

// Serializer is a serializer for line protocol.
type Serializer struct {
	SenderID string
	buf      bytes.Buffer
}

// NewSerializer create new photon binary serializer
func NewSerializer() *Serializer {
	log.Printf("I! [serializers.photon_bin] NewSerializer is called")
	serializer := &Serializer{}
	serializer.SenderID = "TestSernderId"
	return serializer
}

// Serialize writes the telegraf.Metric to a byte slice.  May produce multiple
// lines of output if longer than maximum line length.  Lines are terminated
// with a newline (LF) char.
func (s *Serializer) Serialize(m telegraf.Metric) ([]byte, error) {
	s.buf.Reset()

	log.Printf("I! [serializers.photon_bin] Serialize is called")

	writeBatchHeader(&s.buf, 1, s.SenderID)

	err := writeMetric(&s.buf, m)
	if err != nil {
		return nil, err
	}

	out := make([]byte, s.buf.Len())
	copy(out, s.buf.Bytes())
	return out, nil
}

// SerializeBatch writes the slice of metrics and returns a byte slice of the
// results.  The returned byte slice may contain multiple lines of data.
func (s *Serializer) SerializeBatch(metrics []telegraf.Metric) ([]byte, error) {
	s.buf.Reset()

	log.Printf("I! [serializers.photon_bin] SerializeBatch is called")

	var (
		writtenMetricsCount int32
		metricBuffer        bytes.Buffer
	)

	for _, m := range metrics {

		metricBuffer.Reset()
		err := writeMetric(&metricBuffer, m)
		if err != nil {

			log.Printf("W! [serializers.photon_bin] SerializeBatch got error from writeMetric: %v", err)
			continue
		}
		metricBuffer.WriteTo(&s.buf)

		writtenMetricsCount++
	}

	var result bytes.Buffer
	writeBatchHeader(&result, writtenMetricsCount, s.SenderID)

	s.buf.WriteTo(&result)
	return result.Bytes(), nil
}

func writeMetric(w *bytes.Buffer, m telegraf.Metric) error {
	var (
		err error
	)

	io.WriteString(w, m.Name())
	writeInt16(w, 1)

	switch len(m.FieldList()) {
	case 0:
		log.Printf(
			"W! [serializers.photon_bin] could not serialize metric %v; It has no fields. discarding it", m.Name())
		return nil
	case 1:
		flds := m.FieldList()
		err = appendFieldValue(w, m.Name(), flds[0].Key, flds[0].Value)
	default:
		log.Printf("D! [serializers.photon_bin] metric %v; has MANY! fields", m.Name())
		for k := range m.Fields() {
			log.Printf("D! [serializers.photon_bin] metric %v; has field: %v", m.Name(), k)
		}
		err = appendFieldValue(w, m.Name(), "value_mean", m.Fields()["value_mean"])
	}

	return err
}

func writeBatchHeader(w *bytes.Buffer, len int32, senderId string) error {
	w.WriteByte(0xee)
	w.WriteByte(0xff)

	writeInt32(w, int32(len))
	_, err := w.WriteString(senderId)

	return err
}

func writeString(w io.Writer, str string) error {
	_, err := io.WriteString(w, str)
	return err
}

func writeTime(w io.ByteWriter, t time.Time) {

	d := t.Sub(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))
	ticks := d.Nanoseconds() / int64(100)

	writeInt64Value(w, ticks)
}

func writeInt32(w io.ByteWriter, value int32) error {

	err := w.WriteByte(byte(value))
	err = w.WriteByte(byte(value >> 8))
	err = w.WriteByte(byte(value >> 16))
	err = w.WriteByte(byte(value >> 24))

	return err
}

func writeInt16(w io.ByteWriter, value int16) error {

	err := w.WriteByte(byte(value))
	err = w.WriteByte(byte(value >> 8))
	return err
}

func (s *Serializer) newMetricError(reason string) *MetricError {
	return &MetricError{reason: reason}
}

func appendFieldValue(w io.ByteWriter, metricName, fieldName string, value interface{}) error {

	if value == nil {
		return &FieldError{fmt.Sprintf("metric %v does not have field %v", metricName, fieldName)}
	}

	switch v := value.(type) {
	case float64:
		if math.IsNaN(v) {
			return &FieldError{"is NaN"}
		}

		if math.IsInf(v, 0) {
			return &FieldError{"is Inf"}
		}

		appendFloatField(w, v)
		return nil
	default:
		log.Printf("D! [serializers.photon_bin] invalid value type: %T", v)
		return &FieldError{fmt.Sprintf("invalid value type: %T", v)}
	}
}

func writeInt64Value(w io.ByteWriter, value int64) {
	w.WriteByte(byte(value))
	w.WriteByte(byte(value >> 8))
	w.WriteByte(byte(value >> 16))
	w.WriteByte(byte(value >> 24))
	w.WriteByte(byte(value >> 32))
	w.WriteByte(byte(value >> 40))
	w.WriteByte(byte(value >> 48))
	w.WriteByte(byte(value >> 56))
}

func appendFloatField(w io.ByteWriter, value float64) {
	int64Value := int64(value)
	writeInt64Value(w, int64Value)
}
